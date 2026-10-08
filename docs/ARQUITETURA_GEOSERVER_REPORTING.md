# Acesso externo aos dados — camada `reporting` + GeoServer

Consumidores externos (QGIS, ArcGIS, Power BI de um laboratório de 10 a 15 pessoas, uso
ocasional) nunca leem as tabelas de produção que servem a API e o pipeline. O isolamento está no
Postgres; o GeoServer é a camada de serviço OGC (WMS/WFS/WCS) sobre ele.

## 1. Isolamento no Postgres

Definido em `fast-api-protected-areas-sc/migrations/005_reporting_readonly_layer.sql`, aplicado
pelo runner com checksum da API (`api-migrate`).

| View `reporting` | Origem | Observação |
|---|---|---|
| `uc` | `public.uc` | sem `criado_por` |
| `za_oficial` | `public.za_oficial` | sem `ator`, `motivo`, `correlation_id`, `import_id`, `dag_run_id` |
| `buffer_abrangencia` | `public.buffer_abrangencia` | idem |
| `prodes_clip` | `public.prodes_clip` | — |
| `mapbiomas_alerta_clip` | `public.mapbiomas_alerta_clip` | — |
| `firms_clip` | `public.firms_clip` | sem `run_id`, `source_checksum` e datas de camada |
| `mapbiomas_legend_class` | `public.mapbiomas_legend_class` | legenda hierárquica (41 códigos) |
| `mapbiomas_clip` | `public.mapbiomas_clip` + legenda | nome e cor da classe resolvidos; sem `run_id` |
| `mapbiomas_raster_asset` | `public.mapbiomas_raster_asset` | só `PUBLISHED`; sem `run_id` |

`reporting.gt_pk_metadata` declara a chave de cada view no formato que o GeoServer lê
(parâmetro `Primary key metadata table` do store). View não expõe chave primária ao catálogo, e
sem essa tabela a paginação do WFS (`startIndex`, usada pelo QGIS) falha com
"Cannot do natural order without a primary key".

As views vetoriais trazem `longitude`/`latitude` (ponto interno da geometria, SIRGAS 2000) para
ferramentas sem suporte espacial, como o Power BI. Tabelas de autenticação, sessão, eventos
cadastrais, histórico geométrico e controle interno ficam fora.

Papéis:

- `reporting_readonly` (`NOLOGIN`): `USAGE` em `reporting` e `SELECT` nas views (inclusive nas
  futuras, por `ALTER DEFAULT PRIVILEGES`);
- `geoserver_svc` e `powerbi_svc` (`LOGIN`, membros do grupo): `search_path = reporting, public`,
  transação somente leitura por padrão, `statement_timeout = 120s`, limite de 20 e 10 conexões;
- `lab_svc` (`LOGIN`, membro do grupo, migração `007_reporting_lab_login.sql`): login fixo do
  grupo do laboratório para Power BI e clientes SQL, com os mesmos parâmetros, mas
  `statement_timeout = 220s` e limite de 12 conexões. Participantes de oficina usam o login do
  modo eventos.

`public` mantém `USAGE` apenas para resolver as funções PostGIS; nenhuma tabela de `public` é
concedida. As views pertencem ao dono das tabelas, por isso o consumidor só precisa de `SELECT`
nelas. A proteção vale por privilégio, não só pela transação somente leitura: mesmo desligando-a,
`CREATE`, `UPDATE` e `DELETE` são negados.

Senhas: a migration cria os logins sem senha (login impossível). `api-migrate` aplica
`PA_SC_REPORTING_GEOSERVER_DB_PASSWORD`, `PA_SC_REPORTING_POWERBI_DB_PASSWORD` e
`PA_SC_REPORTING_LAB_DB_PASSWORD` do `.env` da API
a cada execução; variável vazia mantém o login bloqueado. O GeoServer recebe a mesma senha por
`REPORTING_GEOSERVER_DB_PASSWORD` no `.env` do pipeline.

## 2. GeoServer

- Serviço `protected-areas-sc-geoserver` em `airflow/docker-compose.yaml`, imagem
  `docker.osgeo.org/geoserver:2.28.5` (Java 21), porta `${GEOSERVER_PORT:-8600}`, administrador por
  `GEOSERVER_ADMIN_PASSWORD`. Configuração em volume nomeado, recriável a qualquer momento pelo
  publicador.
- Workspace `protected_areas_sc`; store PostGIS `reporting` com `geoserver_svc`.
- Camadas vetoriais: `uc`, `za_oficial`, `buffer_abrangencia`, `prodes_clip`,
  `mapbiomas_alerta_clip`, `firms_clip`, `mapbiomas_raster_asset` (extensão dos rasters
  publicados). Nas três cadastrais o estilo desenha só o registro vigente; o WFS entrega todo o
  histórico com vigência.
- Raster: uma camada `mapbiomas_uso_cobertura_<ano>` por ano publicado, lida do COG Gold
  (`airflow/data/gold/mapbiomas_lulc`, montado somente leitura). Os anos vêm de
  `reporting.mapbiomas_raster_asset` e cada arquivo tem o SHA-256 conferido antes de publicar.
- Tabelas sem geometria, só no WFS (WMS e WMTS desligados na camada): `mapbiomas_clip` (área por
  classe, ano e AOI) e `mapbiomas_legend_class`. Atendem quem consome tabelas pela web, como o
  Excel (`outputFormat=csv`), sem login de banco. A extensão é declarada (Santa Catarina), porque
  o GeoServer não a calcula sem geometria.
- WFS em nível `BASIC`: sem `Transaction`/`LockFeature`.
- Cada ano raster é criado como store GeoTIFF e cobertura via REST JSON; o `POST` da cobertura
  dispara a autoconfiguração do GeoServer, que preenche bandas, formato nativo, grade e SRS de
  requisição/resposta exigidos pelo WCS. A cobertura só é recriada quando falta algum desses
  metadados ou quando o arquivo publicado muda de caminho.
- Exposição: localmente na porta 8600; em produção, os serviços OGC ficam abertos na internet, sem
  senha e sem restrição de rede, por HTTPS no domínio do projeto.

Por que o raster vem do COG e não das tabelas `mapbiomas_raster_<ano>`: a imagem oficial não traz
suporte a PostGIS Raster (só módulo comunitário); views sobre raster entram em `raster_columns`
com `srid = 0`; e o pipeline recria a tabela do ano a cada carga, o que uma view dependente
bloquearia. O COG Gold já é o produto publicado e verificado por checksum, e servi-lo por arquivo
somente leitura dispensa acesso do GeoServer ao banco para raster.

Legenda: a paleta vem de `geoserver/styles/ESTILO_QGIS_COL11_PT.qml`, o QML oficial da Coleção 11
(33 classes; distribuído em `COVERAGE_QGIS_COL11_PT_EN.zip`). Os estilos `N_CHANGES`/`N_CLASSES`
são de produtos auxiliares e não servem para a cobertura anual. Como o GeoServer não importa QML,
o publicador converte a paleta em `ColorMap` SLD no momento da publicação; o QML continua sendo a
única fonte, e o mesmo arquivo serve a quem abrir o raster direto no QGIS.

## 3. Atualização após novas cargas

Toda tarefa que grava dados lidos por `reporting` declara o Dataset `REPORTING_PUBLISHED`
(`scripts_python/reporting_dataset.py`):

| DAG | Tarefa |
|---|---|
| `DAG_UCS`, `DAG_UC_ZA` | `load_postgres` |
| `DAG_ZA_BUFFER` | `load_postgres_za`, `load_postgres_buffer` |
| `DAG_PRODES`, `DAG_MAPBIOMAS_ALERTA` | `load_postgres` |
| `DAG_MAPBIOMAS` | `load_area_statistics_postgres` |
| `DAG_FIRMS`, `DAG_FIRMS_BACKFILL` | `summarize` |

`DAG_GEOSERVER_SYNC` é agendada por esse Dataset (eventos simultâneos viram uma execução). Primeiro
envia ao S3 o que mudou na Medallion local (`push_medallion`, sem efeito fora da AWS); na AWS, o nó de
serviço baixa o COG Gold do S3 por SSM Run Command. Em seguida roda `publish_all` de
`geoserver/bootstrap_layers.py`, idempotente:

1. garante workspace, store, estilos e camadas vetoriais, recalculando as extensões;
2. publica todo ano raster `PUBLISHED` (ano novo aparece sozinho) e remove camadas de anos que
   deixaram de estar publicados;
3. `POST /rest/reset` descarta leitores e conexões em memória (um COG recarregado é relido);
4. trunca o GeoWebCache de todas as camadas.

Vetor novo já aparece no WFS sem sincronização (as views leem as tabelas vivas); a sincronização
garante extensão correta e tiles sem cache velho. Se o GeoServer estiver fora do ar, as cargas
não falham: só `DAG_GEOSERVER_SYNC` é retentada.

Uma view nova em `reporting` exige migration na API e uma entrada em `VECTOR_LAYERS` no
publicador.

## 4. Operação

```bash
# API: cria/atualiza schema, papéis e senhas
docker compose --profile tools run --rm api-migrate

# pipeline: sobe o GeoServer e publica tudo (também roda sozinho após cada carga)
cd airflow && docker compose up -d protected-areas-sc-geoserver
python geoserver/bootstrap_layers.py --env-file airflow/.env
```

Acesso do laboratório:

- QGIS/ArcGIS: `http://<host>:8600/geoserver/protected_areas_sc/ows` (WMS, WFS, WCS);
- Power BI: conector PostgreSQL em `<host>:5432`, banco do projeto, usuário `powerbi_svc`,
  schema `reporting`.

## 5. Validação com clientes

- QGIS: validado com GDAL/OGR, a mesma biblioteca que o QGIS usa para WMS/WFS/WCS. O WFS lista as
  7 camadas com título, SRS EPSG:4674 e extensão, e filtros de atributo batem com o banco;
  com paginação, o PRODES lido em páginas de 1.000 traz os 3.276 registros, sem repetir nem pular. O WMS
  lista as 9 camadas. O WCS 1.0.0 (padrão do QGIS) e o 2.0.1 devolvem o raster e recortes com
  códigos de classe válidos. Com EPSG:4326, a requisição automática da GDAL em WCS 2.0.1 inverte
  os eixos; o servidor responde corretamente quando os eixos seguem a norma (`Lat`, `Long`).
- Power BI: conector PostgreSQL simulado com `powerbi_svc` pela rede. O Navegador enxerga as
  views de `reporting` e os catálogos de sistema do PostGIS (`geometry_columns`,
  `spatial_ref_sys`, `tiger`, `topology`), concedidos a todos pelas extensões e sem dado do
  projeto; o laboratório deve usar apenas o schema `reporting`.

## 6. Testes

- API (`tests/unit/test_reporting_layer.py`): conjunto de views, nenhuma coluna de auditoria,
  nenhum `SELECT *`, provisionamento rejeita papel desconhecido.
- Pipeline (`airflow/tests/test_geoserver_publisher.py`): SLD do raster com todas as classes do
  QML, SLDs vetoriais válidos, DagBag sem erro e conjunto exato de DAGs que emitem o Dataset.

## 7. Melhorias futuras

- Uma camada raster com dimensão `TIME` quando a série tiver muitos anos carregados.
- Pirâmide/overviews se a navegação em escala pequena ficar lenta com mais usuários.
