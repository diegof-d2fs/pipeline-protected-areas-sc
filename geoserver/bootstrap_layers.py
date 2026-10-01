"""Publica e sincroniza no GeoServer as camadas da camada de leitura `reporting`.

Idempotente. Roda na subida do serviço (manualmente) e, depois de cada carga do pipeline, pela
`DAG_GEOSERVER_SYNC`. Cria ou atualiza o workspace, o store PostGIS (login `geoserver_svc`,
schema `reporting`), as camadas vetoriais com seus estilos e uma camada raster por ano
publicado; remove camadas raster de anos que deixaram de estar publicados; recalcula extensões;
descarta o cache de leitores (um COG recarregado é relido) e o GeoWebCache das camadas.

Os anos raster vêm de `reporting.mapbiomas_raster_asset` (apenas ativos `PUBLISHED`), lida pelo
próprio WFS do GeoServer; cada COG Gold tem o SHA-256 conferido antes de ser publicado. O estilo
do raster é derivado, no momento da publicação, do QML oficial do MapBiomas em `styles/`, que é a
única fonte da paleta.

Uso (a partir da raiz do repositório):
    python geoserver/bootstrap_layers.py --env-file airflow/.env
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import quoteattr

ROOT = Path(__file__).resolve().parent
WORKSPACE = "protected_areas_sc"
DATASTORE = "reporting"
RASTER_STYLE = "mapbiomas_uso_cobertura"
RASTER_QML = ROOT / "styles" / "ESTILO_QGIS_COL11_PT.qml"
GOLD_MOUNT = "/data/gold"
NATIVE_SRS = "EPSG:4674"


@dataclass(frozen=True)
class VectorLayer:
    name: str
    title: str
    abstract: str
    symbolizer: str
    active_filter: str | None = None


def _polygon(fill: str, stroke: str, fill_opacity: float, dash: str | None = None) -> str:
    dash_param = f'<CssParameter name="stroke-dasharray">{dash}</CssParameter>' if dash else ""
    return (
        "<PolygonSymbolizer>"
        f'<Fill><CssParameter name="fill">{fill}</CssParameter>'
        f'<CssParameter name="fill-opacity">{fill_opacity}</CssParameter></Fill>'
        f'<Stroke><CssParameter name="stroke">{stroke}</CssParameter>'
        f'<CssParameter name="stroke-width">1.5</CssParameter>{dash_param}</Stroke>'
        "</PolygonSymbolizer>"
    )


_UC_POINT = (
    "<PointSymbolizer><Graphic><Mark><WellKnownName>circle</WellKnownName>"
    '<Fill><CssParameter name="fill">#1b7837</CssParameter></Fill>'
    '<Stroke><CssParameter name="stroke">#ffffff</CssParameter></Stroke>'
    "</Mark><Size>9</Size></Graphic></PointSymbolizer>"
)

VECTOR_LAYERS = (
    VectorLayer(
        "uc",
        "Unidades de Conservação",
        "Unidades de Conservação de SC. O estilo mostra só as UCs com situação ATIVA; "
        "o WFS entrega todas, com situação e vigência.",
        _polygon("#1b7837", "#1b7837", 0.25) + _UC_POINT,
        "<ogc:PropertyIsEqualTo><ogc:PropertyName>situacao</ogc:PropertyName>"
        "<ogc:Literal>ATIVA</ogc:Literal></ogc:PropertyIsEqualTo>",
    ),
    VectorLayer(
        "za_oficial",
        "Zonas de Amortecimento oficiais",
        "Zonas de Amortecimento oficiais. O estilo mostra só as versões vigentes; "
        "o WFS entrega o histórico com fl_ativa e vigência.",
        _polygon("#e08214", "#b35806", 0.2),
        "<ogc:PropertyIsEqualTo><ogc:PropertyName>fl_ativa</ogc:PropertyName>"
        "<ogc:Literal>true</ogc:Literal></ogc:PropertyIsEqualTo>",
    ),
    VectorLayer(
        "buffer_abrangencia",
        "Buffers de Abrangência",
        "Buffer de Abrangência de 3 km das UCs sem Zona de Amortecimento oficial. O estilo mostra "
        "só as versões vigentes; o WFS entrega o histórico com fl_ativa e vigência.",
        _polygon("#4393c3", "#2166ac", 0.1, "6 4"),
        "<ogc:PropertyIsEqualTo><ogc:PropertyName>fl_ativa</ogc:PropertyName>"
        "<ogc:Literal>true</ogc:Literal></ogc:PropertyIsEqualTo>",
    ),
    VectorLayer(
        "prodes_clip",
        "PRODES — desmatamento recortado",
        "Polígonos PRODES recortados por UC, Zona de Amortecimento oficial ou Buffer de Abrangência.",
        _polygon("#d73027", "#a50026", 0.6),
    ),
    VectorLayer(
        "mapbiomas_alerta_clip",
        "MapBiomas Alerta — alertas recortados",
        "Alertas de desmatamento do MapBiomas Alerta recortados por UC e entorno.",
        _polygon("#c51b7d", "#8e0152", 0.6),
    ),
    VectorLayer(
        "firms_clip",
        "FIRMS — focos de calor",
        "Detecções de fogo ativo NASA FIRMS dentro das UCs e de seus entornos.",
        "<PointSymbolizer><Graphic><Mark><WellKnownName>circle</WellKnownName>"
        '<Fill><CssParameter name="fill">#ff4500</CssParameter></Fill>'
        '<Stroke><CssParameter name="stroke">#7f0000</CssParameter></Stroke>'
        "</Mark><Size>7</Size></Graphic></PointSymbolizer>",
    ),
    VectorLayer(
        "mapbiomas_raster_asset",
        "MapBiomas Uso e Cobertura — extensão dos rasters publicados",
        "Extensão, ano e checksum de cada raster MapBiomas Uso e Cobertura publicado.",
        _polygon("#000000", "#525252", 0.0, "2 4"),
    ),
)


class GeoServer:
    def __init__(self, base_url: str, user: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {token}"}

    def request(
        self,
        method: str,
        path: str,
        body: str | bytes | dict | None = None,
        content_type: str = "application/json",
        allow: tuple[int, ...] = (),
    ) -> tuple[int, bytes]:
        data = json.dumps(body).encode() if isinstance(body, dict) else body
        if isinstance(data, str):
            data = data.encode("utf-8")
        headers = dict(self.headers, Accept="application/json")
        if data is not None:
            headers["Content-Type"] = content_type
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            if error.code in allow:
                return error.code, error.read()
            detail = error.read().decode("utf-8", "replace")[:500]
            raise RuntimeError(f"{method} {path} -> HTTP {error.code}: {detail}") from error

    def exists(self, path: str) -> bool:
        status, _ = self.request("GET", path, allow=(404,))
        return status == 200

    def upsert(self, collection: str, name: str, body: dict) -> str:
        if self.exists(f"{collection}/{name}.json"):
            self.request("PUT", f"{collection}/{name}.json", body)
            return "atualizado"
        self.request("POST", f"{collection}.json", body)
        return "criado"


def load_env_file(path: Path) -> None:
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Variável obrigatória ausente: {name}")
    return value


def vector_sld(layer: VectorLayer) -> str:
    rule_filter = f"<ogc:Filter>{layer.active_filter}</ogc:Filter>" if layer.active_filter else ""
    return _sld_document(
        layer.name,
        f"<FeatureTypeStyle><Rule><Title>{layer.title}</Title>{rule_filter}{layer.symbolizer}</Rule></FeatureTypeStyle>",
    )


def raster_sld_from_qml(qml_path: Path) -> str:
    entries = ET.parse(qml_path).getroot().findall(".//colorPalette/paletteEntry")
    if not entries:
        raise SystemExit(f"QML sem paleta: {qml_path}")
    color_map = ['<ColorMapEntry color="#000000" quantity="0" opacity="0" label="Sem dado"/>']
    for entry in sorted(entries, key=lambda item: int(item.get("value"))):
        opacity = int(entry.get("alpha", "255")) / 255
        color_map.append(
            f'<ColorMapEntry color="{entry.get("color")}" quantity="{int(entry.get("value"))}" '
            f'opacity="{opacity:g}" label={quoteattr(entry.get("label", ""))}/>'
        )
    body = (
        "<FeatureTypeStyle><Rule><RasterSymbolizer><Opacity>1.0</Opacity>"
        f'<ColorMap type="values">{"".join(color_map)}</ColorMap>'
        "</RasterSymbolizer></Rule></FeatureTypeStyle>"
    )
    return _sld_document(RASTER_STYLE, body)


def _sld_document(name: str, feature_type_style: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" '
        'xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        f"<NamedLayer><Name>{name}</Name><UserStyle><Name>{name}</Name>"
        f"{feature_type_style}</UserStyle></NamedLayer></StyledLayerDescriptor>"
    )


def publish_style(geoserver: GeoServer, name: str, sld: str) -> None:
    styles = f"/rest/workspaces/{WORKSPACE}/styles"
    sld_type = "application/vnd.ogc.sld+xml"
    if geoserver.exists(f"{styles}/{name}.json"):
        geoserver.request("PUT", f"{styles}/{name}", sld, content_type=sld_type)
    else:
        geoserver.request("POST", f"{styles}?name={name}", sld, content_type=sld_type)


def set_default_style(geoserver: GeoServer, layer: str, style: str) -> None:
    geoserver.request(
        "PUT",
        f"/rest/layers/{WORKSPACE}:{layer}.json",
        {"layer": {"defaultStyle": {"name": f"{WORKSPACE}:{style}"}}},
    )


def publish_vector_layers(geoserver: GeoServer, db: dict[str, str]) -> None:
    workspace = f"/rest/workspaces/{WORKSPACE}"
    store_body = {
        "dataStore": {
            "name": DATASTORE,
            "description": "Views somente leitura do schema reporting (login geoserver_svc).",
            "type": "PostGIS",
            "enabled": True,
            "connectionParameters": {
                "entry": [
                    {"@key": "dbtype", "$": "postgis"},
                    {"@key": "host", "$": db["host"]},
                    {"@key": "port", "$": db["port"]},
                    {"@key": "database", "$": db["database"]},
                    {"@key": "schema", "$": "reporting"},
                    {"@key": "user", "$": "geoserver_svc"},
                    {"@key": "passwd", "$": db["password"]},
                    {"@key": "Expose primary keys", "$": "true"},
                    # Views não têm chave no catálogo; sem isto a paginação do WFS falha.
                    {"@key": "Primary key metadata table", "$": "reporting.gt_pk_metadata"},
                    {"@key": "Loose bbox", "$": "true"},
                    {"@key": "Estimated extends", "$": "false"},
                    {"@key": "validate connections", "$": "true"},
                    {"@key": "min connections", "$": "1"},
                    {"@key": "max connections", "$": "10"},
                    {"@key": "fetch size", "$": "1000"},
                ]
            },
        }
    }
    print(f"store {DATASTORE}: {geoserver.upsert(f'{workspace}/datastores', DATASTORE, store_body)}")

    feature_types = f"{workspace}/datastores/{DATASTORE}/featuretypes"
    for layer in VECTOR_LAYERS:
        body = {
            "featureType": {
                "name": layer.name,
                "nativeName": layer.name,
                "title": layer.title,
                "abstract": layer.abstract,
                "srs": NATIVE_SRS,
                "projectionPolicy": "FORCE_DECLARED",
                "enabled": True,
            }
        }
        if geoserver.exists(f"{feature_types}/{layer.name}.json"):
            geoserver.request(
                "PUT", f"{feature_types}/{layer.name}.json?recalculate=nativebbox,latlonbbox", body
            )
            action = "atualizada"
        else:
            geoserver.request("POST", f"{feature_types}.json", body)
            action = "criada"
        publish_style(geoserver, layer.name, vector_sld(layer))
        set_default_style(geoserver, layer.name, layer.name)
        print(f"camada {layer.name}: {action}")


def published_raster_assets(geoserver: GeoServer) -> list[dict]:
    query = urllib.parse.urlencode(
        {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": f"{WORKSPACE}:mapbiomas_raster_asset",
            "outputFormat": "application/json",
            "propertyName": "reference_year,coverage_scope,storage_key,checksum_sha256",
        }
    )
    _, payload = geoserver.request("GET", f"/{WORKSPACE}/ows?{query}")
    return [feature["properties"] for feature in json.loads(payload)["features"]]


def verify_checksum(gold_root: Path, storage_key: str, expected: str) -> None:
    path = gold_root / storage_key
    if not path.is_file():
        raise SystemExit(f"COG publicado não encontrado no Gold: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected.strip():
        raise SystemExit(f"Checksum divergente para {storage_key}; publicação interrompida.")


def publish_raster_layers(geoserver: GeoServer, gold_root: Path) -> list[str]:
    publish_style(geoserver, RASTER_STYLE, raster_sld_from_qml(RASTER_QML))
    workspace = f"/rest/workspaces/{WORKSPACE}"
    assets = published_raster_assets(geoserver)
    if not assets:
        print("nenhum raster MapBiomas publicado em reporting.mapbiomas_raster_asset")
    published: list[str] = []
    for asset in sorted(assets, key=lambda item: item["reference_year"]):
        storage_key = asset["storage_key"]
        if storage_key.startswith("/") or ".." in Path(storage_key).parts:
            raise SystemExit(f"storage_key inválida: {storage_key}")
        verify_checksum(gold_root, storage_key, asset["checksum_sha256"])
        name = f"mapbiomas_uso_cobertura_{asset['reference_year']}"
        url = f"file:{GOLD_MOUNT}/{storage_key}"  # forma canônica que o GeoServer devolve
        store_path = f"{workspace}/coveragestores/{name}"
        coverage_path = f"{store_path}/coverages/{name}.json"
        if coverage_is_complete(geoserver, store_path, coverage_path, url):
            store_action = "mantido"
        else:
            # Store e cobertura são recriados juntos: o POST da cobertura dispara a
            # autoconfiguração que preenche bandas, grade, formato nativo e SRS exigidos pelo WCS.
            geoserver.request("DELETE", f"{store_path}?recurse=true&purge=none", allow=(404,))
            geoserver.request(
                "POST",
                f"{workspace}/coveragestores.json",
                {
                    "coverageStore": {
                        "name": name,
                        "type": "GeoTIFF",
                        "enabled": True,
                        "workspace": {"name": WORKSPACE},
                        "url": url,
                    }
                },
            )
            geoserver.request(
                "POST",
                f"{store_path}/coverages.json",
                {"coverage": {"name": name, "nativeCoverageName": Path(storage_key).stem}},
            )
            store_action = "configurado"
        geoserver.request(
            "PUT",
            coverage_path,
            {
                "coverage": {
                    "title": f"MapBiomas Uso e Cobertura {asset['reference_year']} ({asset['coverage_scope']})",
                    "abstract": (
                        "MapBiomas Coleção 11, recorte de Santa Catarina. Valor do pixel = código "
                        "de classe; legenda oficial MapBiomas."
                    ),
                }
            },
        )
        set_default_style(geoserver, name, RASTER_STYLE)
        published.append(name)
        print(f"raster {name}: store {store_action}, checksum conferido")
    remove_unpublished_rasters(geoserver, set(published))
    return published


def coverage_is_complete(geoserver: GeoServer, store_path: str, coverage_path: str, url: str) -> bool:
    status, payload = geoserver.request("GET", f"{store_path}.json", allow=(404,))
    if status == 404 or json.loads(payload)["coverageStore"].get("url") != url:
        return False
    status, payload = geoserver.request("GET", coverage_path, allow=(404,))
    if status == 404:
        return False
    coverage = json.loads(payload)["coverage"]
    return all(
        coverage.get(key) for key in ("nativeFormat", "dimensions", "requestSRS", "responseSRS")
    )


def remove_unpublished_rasters(geoserver: GeoServer, published: set[str]) -> None:
    _, payload = geoserver.request("GET", f"/rest/workspaces/{WORKSPACE}/coveragestores.json")
    stores = (json.loads(payload).get("coverageStores") or {}).get("coverageStore") or []
    for store in stores:
        name = store["name"]
        if name.startswith("mapbiomas_uso_cobertura_") and name not in published:
            geoserver.request(
                "DELETE", f"/rest/workspaces/{WORKSPACE}/coveragestores/{name}?recurse=true&purge=none"
            )
            print(f"raster {name}: removido (ano não está mais publicado)")


def refresh_caches(geoserver: GeoServer, layers: list[str]) -> None:
    # Leitores de arquivo e conexões ficam em memória: sem isto um COG recarregado
    # continuaria sendo lido na versão anterior, e o GeoWebCache serviria tiles antigos.
    geoserver.request("POST", "/rest/reset")
    for layer in layers:
        geoserver.request(
            "POST",
            "/gwc/rest/masstruncate",
            f"<truncateLayer><layerName>{WORKSPACE}:{layer}</layerName></truncateLayer>",
            content_type="text/xml",
            allow=(404,),
        )
    print(f"caches descartados: {len(layers)} camadas")


def harden_services(geoserver: GeoServer) -> None:
    # Somente leitura também na borda OGC: WFS sem Transaction/LockFeature.
    geoserver.request("PUT", "/rest/services/wfs/settings.json", {"wfs": {"serviceLevel": "BASIC"}})


def publish_all(gold_root: Path) -> list[str]:
    """Sincroniza o GeoServer com `reporting` e o Gold; devolve as camadas publicadas."""
    port = os.environ.get("GEOSERVER_PORT", "8600")
    geoserver = GeoServer(
        os.environ.get("GEOSERVER_URL", f"http://localhost:{port}/geoserver"),
        os.environ.get("GEOSERVER_ADMIN_USER", "admin"),
        required_env("GEOSERVER_ADMIN_PASSWORD"),
    )
    db = {
        "host": os.environ.get("GEOSERVER_DB_HOST", "protected-areas-sc-db-main"),
        "port": os.environ.get("GEOSERVER_DB_PORT", "5432"),
        "database": os.environ.get("PROJECT_DB_NAME", "protected-areas-sc-db-main"),
        "password": required_env("REPORTING_GEOSERVER_DB_PASSWORD"),
    }

    if not geoserver.exists(f"/rest/workspaces/{WORKSPACE}.json"):
        geoserver.request("POST", "/rest/workspaces.json", {"workspace": {"name": WORKSPACE}})
        print(f"workspace {WORKSPACE}: criado")
    harden_services(geoserver)
    publish_vector_layers(geoserver, db)
    rasters = publish_raster_layers(geoserver, gold_root)
    layers = [layer.name for layer in VECTOR_LAYERS] + rasters
    refresh_caches(geoserver, layers)
    return layers


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--gold-root", type=Path, default=ROOT.parent / "airflow" / "data" / "gold")
    args = parser.parse_args()
    if args.env_file:
        load_env_file(args.env_file)
    publish_all(args.gold_root)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        sys.exit(str(error))
