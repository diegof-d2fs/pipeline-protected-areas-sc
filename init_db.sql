-- =========================================================
-- EXTENSÕES
-- =========================================================
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;

-- =========================================================
-- CONFIGURAÇÃO GDAL DO POSTGIS RASTER
-- =========================================================
-- Habilita o driver GTiff no nível do banco para a carga in-db do COG
-- MapBiomas (ST_FromGDALRaster) e para exportação controlada de recortes
-- (ST_AsGDALRaster). O acesso out-db permanece desabilitado: os tiles são
-- materializados dentro do banco em mapbiomas_raster_<ano>, sem depender de
-- caminhos de arquivo visíveis ao servidor. Aplica-se a novas conexões.
ALTER DATABASE :"DBNAME" SET postgis.gdal_enabled_drivers = 'GTiff';
ALTER DATABASE :"DBNAME" SET postgis.enable_outdb_rasters = 'False';

-- =========================================================
-- TABELA PRINCIPAL: UC
-- =========================================================
CREATE TABLE IF NOT EXISTS uc (
    id_uc               BIGSERIAL PRIMARY KEY,
    uc_id               VARCHAR(50),
    cd_cnuc             VARCHAR(50),
    wdpa_pid            VARCHAR(50),
    nm_uc               VARCHAR(255) NOT NULL,
    dt_criacao          DATE,
    ds_ato_legal        TEXT,
    ds_grupo            VARCHAR(100),
    ds_categoria        VARCHAR(150),
    ds_esfera           VARCHAR(50),
    nm_orgao_gestor     VARCHAR(255),
    sg_uf               CHAR(2) NOT NULL DEFAULT 'SC',
    area_total_ha       NUMERIC(18,2),
    area_ato_ha         NUMERIC(18,2),
    update_geom         TEXT,
    geom                geometry(Geometry, 4674) NOT NULL,
    dt_bronze           DATE,
    dt_silver           TIMESTAMP,
    dt_gold             TIMESTAMP,
    dt_carga            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_dag          VARCHAR(100),
    versao_registro     BIGINT NOT NULL DEFAULT 1,
    atualizado_em       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uc_cd_cnuc
    ON uc (cd_cnuc);

CREATE INDEX IF NOT EXISTS idx_uc_geom
    ON uc
    USING GIST (geom);

CREATE UNIQUE INDEX IF NOT EXISTS uq_uc_uc_id_present
    ON uc (uc_id)
    WHERE uc_id IS NOT NULL;

-- =========================================================
-- ZA OFICIAL
-- =========================================================
CREATE TABLE IF NOT EXISTS za_oficial (
    id_za_oficial       BIGSERIAL PRIMARY KEY,
    id_uc               BIGINT NOT NULL,
    ds_fonte            VARCHAR(255),
    update_geom         TEXT,
    dt_inicio_vigencia  DATE,
    dt_fim_vigencia     DATE,
    fl_ativa            BOOLEAN NOT NULL DEFAULT TRUE,
    geom                geometry(MultiPolygon, 4674) NOT NULL,
    dt_bronze           DATE,
    dt_silver           TIMESTAMP,
    dt_gold             TIMESTAMP,
    dt_carga            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_dag          VARCHAR(100),
    numero_versao       BIGINT NOT NULL DEFAULT 1,
    motivo              TEXT,
    ator                 VARCHAR(255),
    correlation_id      VARCHAR(128),
    import_id           UUID,
    dag_run_id          VARCHAR(255),

    CONSTRAINT fk_za_oficial_uc
        FOREIGN KEY (id_uc)
        REFERENCES uc (id_uc)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT ck_za_oficial_periodo
        CHECK (dt_fim_vigencia IS NULL OR dt_fim_vigencia >= dt_inicio_vigencia)
);

CREATE INDEX IF NOT EXISTS idx_za_oficial_uc
    ON za_oficial (id_uc);

CREATE INDEX IF NOT EXISTS idx_za_oficial_geom
    ON za_oficial
    USING GIST (geom);

-- =========================================================
-- FONTE E REVISÃO DE VÍNCULOS DE ZA
-- =========================================================
CREATE TABLE IF NOT EXISTS za_oficial_fonte (
    id_za_fonte          BIGSERIAL PRIMARY KEY,
    chave_origem         VARCHAR(255) NOT NULL UNIQUE,
    id_za_origem         VARCHAR(100),
    codigo_uc_origem     VARCHAR(100),
    nm_za_origem         VARCHAR(255),
    ds_fonte             VARCHAR(255),
    geom                 geometry(MultiPolygon, 4674) NOT NULL,
    dt_bronze            DATE NOT NULL,
    dt_carga             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_dag           VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_za_oficial_fonte_geom
    ON za_oficial_fonte USING GIST (geom);

CREATE TABLE IF NOT EXISTS za_oficial_uc (
    id_za_oficial_uc     BIGSERIAL PRIMARY KEY,
    id_za_fonte          BIGINT NOT NULL REFERENCES za_oficial_fonte (id_za_fonte) ON DELETE RESTRICT,
    id_uc                BIGINT NOT NULL REFERENCES uc (id_uc) ON DELETE RESTRICT,
    status_revisao       VARCHAR(24) NOT NULL DEFAULT 'PENDENTE_REVISAO',
    criterio_vinculo     VARCHAR(80) NOT NULL,
    area_intersecao_m2   NUMERIC(20,2) NOT NULL,
    distancia_minima_m   NUMERIC(20,2) NOT NULL,
    observacao_revisao   TEXT,
    revisado_por         VARCHAR(255),
    dt_revisao           TIMESTAMP,
    dt_carga             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_za_oficial_uc_status
        CHECK (status_revisao IN ('PENDENTE_REVISAO', 'CONFIRMADA', 'REJEITADA')),
    CONSTRAINT uq_za_oficial_uc_origem
        UNIQUE (id_za_fonte, id_uc)
);

CREATE INDEX IF NOT EXISTS idx_za_oficial_uc_status
    ON za_oficial_uc (status_revisao, id_uc);

-- Garante apenas uma ZA oficial ativa por UC
CREATE UNIQUE INDEX IF NOT EXISTS uq_za_oficial_ativa_por_uc
    ON za_oficial (id_uc)
    WHERE fl_ativa = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_za_oficial_versao_por_uc
    ON za_oficial (id_uc, numero_versao);

-- =========================================================
-- BUFFER DE ABRANGÊNCIA
-- =========================================================
CREATE TABLE IF NOT EXISTS buffer_abrangencia (
    id_buffer_abrangencia BIGSERIAL PRIMARY KEY,
    id_uc               BIGINT NOT NULL,
    ds_fonte            VARCHAR(255),
    update_geom         TEXT,
    dt_geracao          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dist_buffer_m       NUMERIC(12,2) NOT NULL DEFAULT 3000,
    fl_ativa            BOOLEAN NOT NULL DEFAULT TRUE,
    geom                geometry(MultiPolygon, 4674) NOT NULL,
    dt_bronze           DATE,
    dt_silver           TIMESTAMP,
    dt_gold             TIMESTAMP,
    dt_carga            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_dag          VARCHAR(100),
    numero_versao       BIGINT NOT NULL DEFAULT 1,
    dt_inicio_vigencia  DATE NOT NULL DEFAULT CURRENT_DATE,
    dt_fim_vigencia     DATE,
    motivo              TEXT,
    ator                 VARCHAR(255),
    correlation_id      VARCHAR(128),
    import_id           UUID,
    dag_run_id          VARCHAR(255),

    CONSTRAINT fk_buffer_abrangencia_uc
        FOREIGN KEY (id_uc)
        REFERENCES uc (id_uc)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT ck_buffer_abrangencia_periodo
        CHECK (dt_fim_vigencia IS NULL OR dt_fim_vigencia >= dt_inicio_vigencia)
);

CREATE INDEX IF NOT EXISTS idx_buffer_abrangencia_uc
    ON buffer_abrangencia (id_uc);

CREATE INDEX IF NOT EXISTS idx_buffer_abrangencia_geom
    ON buffer_abrangencia
    USING GIST (geom);

-- Garante apenas um Buffer de Abrangência ativo por UC
CREATE UNIQUE INDEX IF NOT EXISTS uq_buffer_abrangencia_ativo_por_uc
    ON buffer_abrangencia (id_uc)
    WHERE fl_ativa = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_buffer_abrangencia_versao_por_uc
    ON buffer_abrangencia (id_uc, numero_versao);

-- ZA oficial e Buffer de Abrangência são alternativas de entorno: nunca podem ficar ativos ao mesmo tempo.
-- A linha-pai de UC serializa inserções concorrentes entre as duas tabelas.
CREATE OR REPLACE FUNCTION enforce_uc_active_zone_exclusivity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.fl_ativa IS DISTINCT FROM TRUE THEN
        RETURN NEW;
    END IF;

    PERFORM 1 FROM uc WHERE id_uc = NEW.id_uc FOR UPDATE;

    IF TG_TABLE_NAME = 'za_oficial' AND EXISTS (
        SELECT 1 FROM buffer_abrangencia WHERE id_uc = NEW.id_uc AND fl_ativa = TRUE
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = format(
                'UC %s já possui Buffer de Abrangência ativo; encerre-a antes de ativar a ZA oficial.',
                NEW.id_uc
            ),
            CONSTRAINT = 'ck_uc_active_zone_exclusivity';
    END IF;

    IF TG_TABLE_NAME = 'buffer_abrangencia' AND EXISTS (
        SELECT 1 FROM za_oficial WHERE id_uc = NEW.id_uc AND fl_ativa = TRUE
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = format(
                'UC %s já possui ZA oficial ativa; não é permitido ativar um Buffer de Abrangência.',
                NEW.id_uc
            ),
            CONSTRAINT = 'ck_uc_active_zone_exclusivity';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_za_oficial_active_zone_exclusivity ON za_oficial;
CREATE TRIGGER trg_za_oficial_active_zone_exclusivity
BEFORE INSERT OR UPDATE OF id_uc, fl_ativa ON za_oficial
FOR EACH ROW WHEN (NEW.fl_ativa = TRUE)
EXECUTE FUNCTION enforce_uc_active_zone_exclusivity();

DROP TRIGGER IF EXISTS trg_buffer_abrangencia_active_zone_exclusivity ON buffer_abrangencia;
CREATE TRIGGER trg_buffer_abrangencia_active_zone_exclusivity
BEFORE INSERT OR UPDATE OF id_uc, fl_ativa ON buffer_abrangencia
FOR EACH ROW WHEN (NEW.fl_ativa = TRUE)
EXECUTE FUNCTION enforce_uc_active_zone_exclusivity();

-- =========================================================
-- PRODES CLIP
-- =========================================================
CREATE TABLE IF NOT EXISTS prodes_clip (
    id_prodes_clip          BIGSERIAL PRIMARY KEY,
    id_uc                   BIGINT NOT NULL,
    id_za_oficial           BIGINT,
    id_buffer_abrangencia                  BIGINT,
    tipo_cruzamento         VARCHAR(20),
    id_prodes_original      VARCHAR(120),
    nr_ano                  INTEGER NOT NULL,
    ds_class_name           VARCHAR(100),
    area_km2                NUMERIC(18,6),
    area_intersecao_km2     NUMERIC(18,6),
    geom                    geometry(MultiPolygon, 4674) NOT NULL,
    dt_bronze               DATE,
    dt_silver               TIMESTAMP,
    dt_gold                 TIMESTAMP,
    dt_carga                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_dag              VARCHAR(100),

    CONSTRAINT fk_prodes_uc
        FOREIGN KEY (id_uc)
        REFERENCES uc (id_uc)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_prodes_za_oficial
        FOREIGN KEY (id_za_oficial)
        REFERENCES za_oficial (id_za_oficial)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_prodes_buffer_abrangencia
        FOREIGN KEY (id_buffer_abrangencia)
        REFERENCES buffer_abrangencia (id_buffer_abrangencia)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT chk_prodes_um_entorno
        CHECK (
            NOT (id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NOT NULL)
        ),

    CONSTRAINT chk_prodes_tipo_cruzamento
        CHECK (
            tipo_cruzamento IS NULL
            OR (
                tipo_cruzamento IN ('UC', 'ZA', 'BUFFER_ABRANGENCIA')
                AND (
                    (tipo_cruzamento = 'UC' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NULL)
                    OR (tipo_cruzamento = 'ZA' AND id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NULL)
                    OR (tipo_cruzamento = 'BUFFER_ABRANGENCIA' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NOT NULL)
                )
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_prodes_uc
    ON prodes_clip (id_uc);

CREATE INDEX IF NOT EXISTS idx_prodes_za_oficial
    ON prodes_clip (id_za_oficial);

CREATE INDEX IF NOT EXISTS idx_prodes_buffer_abrangencia
    ON prodes_clip (id_buffer_abrangencia);

CREATE INDEX IF NOT EXISTS idx_prodes_geom
    ON prodes_clip
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_prodes_original
    ON prodes_clip (id_prodes_original);

-- =========================================================
-- MAPBIOMAS RASTER ASSETS, LEGEND AND AREA STATISTICS
-- =========================================================
CREATE TABLE IF NOT EXISTS mapbiomas_raster_asset (
    id_raster_asset         BIGSERIAL PRIMARY KEY,
    collection_code         VARCHAR(50) NOT NULL,
    collection_version      VARCHAR(50) NOT NULL,
    reference_year          INTEGER NOT NULL,
    coverage_scope          VARCHAR(10) NOT NULL,
    storage_key             VARCHAR(1024) NOT NULL,
    checksum_sha256         CHAR(64) NOT NULL,
    byte_size               BIGINT NOT NULL,
    media_type              VARCHAR(100) NOT NULL,
    suggested_filename      VARCHAR(255) NOT NULL,
    crs_epsg                INTEGER NOT NULL,
    affine_transform        JSONB NOT NULL,
    width_px                INTEGER NOT NULL,
    height_px               INTEGER NOT NULL,
    band_count              SMALLINT NOT NULL,
    data_type               VARCHAR(32) NOT NULL,
    nodata_value            INTEGER,
    compression             VARCHAR(50),
    overview_levels         JSONB NOT NULL DEFAULT '[]'::jsonb,
    footprint               geometry(MultiPolygon, 4674) NOT NULL,
    publication_status      VARCHAR(20) NOT NULL DEFAULT 'PUBLISHED',
    run_id                  VARCHAR(250) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at            TIMESTAMPTZ,
    CONSTRAINT uq_mapbiomas_raster_asset_checksum UNIQUE (checksum_sha256),
    CONSTRAINT chk_mapbiomas_raster_asset_scope
        CHECK (coverage_scope IN ('BRAZIL', 'SC')),
    CONSTRAINT chk_mapbiomas_raster_asset_dimensions
        CHECK (byte_size >= 0 AND width_px > 0 AND height_px > 0 AND band_count > 0),
    CONSTRAINT chk_mapbiomas_raster_asset_status
        CHECK (publication_status IN ('STAGED', 'PUBLISHED', 'RETIRED'))
);

CREATE INDEX IF NOT EXISTS idx_mapbiomas_raster_asset_lookup
    ON mapbiomas_raster_asset (collection_code, collection_version, reference_year, coverage_scope);

CREATE INDEX IF NOT EXISTS idx_mapbiomas_raster_asset_footprint
    ON mapbiomas_raster_asset USING GIST (footprint);

CREATE TABLE IF NOT EXISTS mapbiomas_legend_class (
    id_legend_class         BIGSERIAL PRIMARY KEY,
    collection_code         VARCHAR(50) NOT NULL,
    collection_version      VARCHAR(50) NOT NULL,
    class_code              INTEGER NOT NULL,
    parent_class_code       INTEGER,
    hierarchy_level         SMALLINT NOT NULL,
    name_pt_br              VARCHAR(150) NOT NULL,
    name_en                 VARCHAR(150),
    color_hex               CHAR(7),
    is_terminal             BOOLEAN NOT NULL,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    source_checksum         CHAR(64) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_mapbiomas_legend_class UNIQUE (collection_code, collection_version, class_code),
    CONSTRAINT chk_mapbiomas_legend_class_level CHECK (hierarchy_level > 0),
    CONSTRAINT chk_mapbiomas_legend_class_color CHECK (color_hex IS NULL OR color_hex ~ '^#[0-9A-Fa-f]{6}$')
);

CREATE INDEX IF NOT EXISTS idx_mapbiomas_legend_class_version
    ON mapbiomas_legend_class (collection_code, collection_version, is_terminal);

CREATE TABLE IF NOT EXISTS mapbiomas_clip (
    id_mb_clip              BIGSERIAL PRIMARY KEY,
    id_raster_asset         BIGINT NOT NULL,
    id_legend_class         BIGINT NOT NULL,
    id_uc                   BIGINT NOT NULL,
    id_za_oficial           BIGINT,
    id_buffer_abrangencia                  BIGINT,
    -- Dataset coordinates denormalised from mapbiomas_raster_asset so that a
    -- multi-year backfill (one asset per year) can be filtered without a join.
    collection_code         VARCHAR(50) NOT NULL,
    collection_version      VARCHAR(50) NOT NULL,
    reference_year          INTEGER NOT NULL,
    aoi_type                VARCHAR(20) NOT NULL,
    aoi_geometry_version    VARCHAR(100) NOT NULL,
    aoi_geometry_sha256     CHAR(64) NOT NULL,
    aoi_area_ha_geodesic    NUMERIC(20,6) NOT NULL,
    pixel_count             BIGINT NOT NULL,
    -- class_area_ha: geodesic area of this legend class inside the AOI.
    -- classified_area_ha: total classified (non-NoData) area of the AOI.
    class_area_ha           NUMERIC(20,6) NOT NULL,
    classified_area_ha      NUMERIC(20,6) NOT NULL,
    coverage_ratio          NUMERIC(12,9) NOT NULL,
    area_method             VARCHAR(100) NOT NULL,
    area_method_version     VARCHAR(50) NOT NULL,
    boundary_policy         VARCHAR(50) NOT NULL,
    source_checksum         CHAR(64) NOT NULL,
    run_id                  VARCHAR(250) NOT NULL,
    dt_bronze               TIMESTAMPTZ,
    dt_silver               TIMESTAMPTZ,
    dt_gold                 TIMESTAMPTZ,
    dt_carga                TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_mapbiomas_uc
        FOREIGN KEY (id_uc)
        REFERENCES uc (id_uc)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_mapbiomas_raster_asset
        FOREIGN KEY (id_raster_asset)
        REFERENCES mapbiomas_raster_asset (id_raster_asset)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_mapbiomas_legend_class
        FOREIGN KEY (id_legend_class)
        REFERENCES mapbiomas_legend_class (id_legend_class)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_mapbiomas_za_oficial
        FOREIGN KEY (id_za_oficial)
        REFERENCES za_oficial (id_za_oficial)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_mapbiomas_buffer_abrangencia
        FOREIGN KEY (id_buffer_abrangencia)
        REFERENCES buffer_abrangencia (id_buffer_abrangencia)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT chk_mapbiomas_um_entorno
        CHECK (
            NOT (id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NOT NULL)
        ),

    CONSTRAINT chk_mapbiomas_aoi_type
        CHECK (
            (aoi_type = 'UC' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NULL)
            OR (aoi_type = 'ZA' AND id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NULL)
            OR (aoi_type = 'BUFFER_ABRANGENCIA' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NOT NULL)
        ),
    -- coverage_ratio may slightly exceed 1: the pixel-center method
    -- (boundary_policy = 'pixel_center') can select a classified pixel area a
    -- fraction above the vector geodesic AOI area near the AOI edge. A 5% ceiling
    -- accepts that boundary effect while still rejecting gross errors.
    CONSTRAINT chk_mapbiomas_area_statistics
        CHECK (
            aoi_area_ha_geodesic >= 0
            AND pixel_count >= 0
            AND class_area_ha >= 0
            AND classified_area_ha >= 0
            AND coverage_ratio >= 0
            AND coverage_ratio <= 1.05
        ),
    CONSTRAINT chk_mapbiomas_clip_reference_year
        CHECK (reference_year BETWEEN 1985 AND 2100)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mapbiomas_clip_natural
    ON mapbiomas_clip (
        id_raster_asset,
        id_legend_class,
        aoi_type,
        id_uc,
        COALESCE(id_za_oficial, 0),
        COALESCE(id_buffer_abrangencia, 0),
        aoi_geometry_sha256,
        area_method_version
    );

CREATE INDEX IF NOT EXISTS idx_mapbiomas_clip_aoi
    ON mapbiomas_clip (id_uc, aoi_type, id_za_oficial, id_buffer_abrangencia);

CREATE INDEX IF NOT EXISTS idx_mapbiomas_clip_dataset
    ON mapbiomas_clip (collection_code, collection_version, reference_year);

CREATE INDEX IF NOT EXISTS idx_mapbiomas_clip_asset_class
    ON mapbiomas_clip (id_raster_asset, id_legend_class);

-- Raster in-db MapBiomas: uma tabela física por ano, mapbiomas_raster_<ano>
-- (ex.: mapbiomas_raster_2025), criada e mantida por load_raster_postgres.
-- Não há tabela unificada de todos os anos: a série temporal por classe/AOI
-- está em mapbiomas_clip, não em uma união de rasters. Cada mapbiomas_raster_<ano>
-- é catalogada em mapbiomas_raster_asset, tem AddRasterConstraints aplicado
-- (srid/scale/pixel_type reais em raster_columns) e é a camada usada no QGIS.
-- Uma view filtrada por ano não serve: entra em raster_columns com srid 0 e sem
-- escala, e o provider postgresraster do QGIS não consegue montá-la.
-- Estrutura de cada tabela por ano:
--   id_raster_tile BIGSERIAL PK
--   id_raster_asset BIGINT NOT NULL REFERENCES mapbiomas_raster_asset ON DELETE CASCADE
--   collection_code / collection_version VARCHAR(50) NOT NULL
--   reference_year INTEGER NOT NULL
--   tile_row / tile_col INTEGER NOT NULL   (partição física, nunca reamostragem)
--   rast raster NOT NULL                   (janela verbatim do COG, grade nativa 4326)
--   created_at TIMESTAMPTZ NOT NULL
--   UNIQUE (id_raster_asset, tile_row, tile_col); índice GiST em ST_ConvexHull(rast)

COMMENT ON TABLE mapbiomas_raster_asset IS 'Catalogo de artefatos raster MapBiomas publicados (uma linha por colecao/versao/ano).';
COMMENT ON TABLE mapbiomas_legend_class IS 'Legenda versionada do MapBiomas.';
COMMENT ON TABLE mapbiomas_clip IS 'Estatisticas de area MapBiomas por AOI exclusiva e classe, por ano (reference_year).';

-- =========================================================
-- MAPBIOMAS ALERTA CLIP
-- =========================================================
CREATE TABLE IF NOT EXISTS mapbiomas_alerta_clip (
    id_alerta_clip          BIGSERIAL PRIMARY KEY,
    id_uc                   BIGINT NOT NULL,
    id_za_oficial           BIGINT,
    id_buffer_abrangencia                  BIGINT,
    tipo_cruzamento         VARCHAR(20),
    id_alerta_original      BIGINT NOT NULL,
    dt_deteccao             DATE,
    dt_imagem_anterior      DATE,
    dt_imagem_posterior     DATE,
    area_ha                 NUMERIC(18,6),
    ds_bioma                VARCHAR(100),
    ds_fonte_deteccao       VARCHAR(255),
    geom                    geometry(Polygon, 4674) NOT NULL,
    dt_bronze               DATE,
    dt_silver               TIMESTAMP,
    dt_gold                 TIMESTAMP,
    dt_carga                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_dag              VARCHAR(100),

    CONSTRAINT fk_mb_alerta_uc
        FOREIGN KEY (id_uc)
        REFERENCES uc (id_uc)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_mb_alerta_za_oficial
        FOREIGN KEY (id_za_oficial)
        REFERENCES za_oficial (id_za_oficial)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_mb_alerta_buffer_abrangencia
        FOREIGN KEY (id_buffer_abrangencia)
        REFERENCES buffer_abrangencia (id_buffer_abrangencia)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT chk_mb_alerta_um_entorno
        CHECK (
            NOT (id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NOT NULL)
        ),

    CONSTRAINT chk_mb_alerta_tipo_cruzamento
        CHECK (
            tipo_cruzamento IS NULL
            OR (
                tipo_cruzamento IN ('UC', 'ZA', 'BUFFER_ABRANGENCIA')
                AND (
                    (tipo_cruzamento = 'UC' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NULL)
                    OR (tipo_cruzamento = 'ZA' AND id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NULL)
                    OR (tipo_cruzamento = 'BUFFER_ABRANGENCIA' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NOT NULL)
                )
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_mb_alerta_uc
    ON mapbiomas_alerta_clip (id_uc);

CREATE INDEX IF NOT EXISTS idx_mb_alerta_za_oficial
    ON mapbiomas_alerta_clip (id_za_oficial);

CREATE INDEX IF NOT EXISTS idx_mb_alerta_buffer_abrangencia
    ON mapbiomas_alerta_clip (id_buffer_abrangencia);

CREATE INDEX IF NOT EXISTS idx_mb_alerta_geom
    ON mapbiomas_alerta_clip
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_mb_alerta_original
    ON mapbiomas_alerta_clip (id_alerta_original);

-- =========================================================
-- FIRMS CLIP
-- =========================================================
CREATE TABLE IF NOT EXISTS firms_clip (
    id_firms_clip           BIGSERIAL PRIMARY KEY,
    detection_id            CHAR(64) NOT NULL,
    source_product          VARCHAR(100) NOT NULL,
    source_version          VARCHAR(100),
    source_processing       VARCHAR(20) NOT NULL,
    id_uc                   BIGINT NOT NULL,
    id_za_oficial           BIGINT,
    id_buffer_abrangencia                  BIGINT,
    tipo_cruzamento         VARCHAR(20) NOT NULL,
    acquired_at_utc         TIMESTAMPTZ NOT NULL,
    satellite               VARCHAR(50),
    instrument              VARCHAR(50),
    confidence_raw          VARCHAR(20) NOT NULL,
    confidence_scheme       VARCHAR(20) NOT NULL,
    confidence_score        NUMERIC(8,3),
    confidence_class        VARCHAR(20) NOT NULL,
    publication_rule_version VARCHAR(50) NOT NULL,
    frp_mw                  NUMERIC(18,6),
    brightness_kelvin       NUMERIC(10,4),
    brightness_secondary_kelvin NUMERIC(10,4),
    scan_km                 NUMERIC(10,4),
    track_km                NUMERIC(10,4),
    daynight                CHAR(1),
    detection_type          VARCHAR(50),
    geom                    geometry(Point, 4674) NOT NULL,
    source_checksum         CHAR(64) NOT NULL,
    run_id                  VARCHAR(250) NOT NULL,
    dt_bronze               TIMESTAMPTZ,
    dt_silver               TIMESTAMPTZ,
    dt_gold                 TIMESTAMPTZ,
    dt_carga                TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_firms_uc
        FOREIGN KEY (id_uc)
        REFERENCES uc (id_uc)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_firms_za_oficial
        FOREIGN KEY (id_za_oficial)
        REFERENCES za_oficial (id_za_oficial)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_firms_buffer_abrangencia
        FOREIGN KEY (id_buffer_abrangencia)
        REFERENCES buffer_abrangencia (id_buffer_abrangencia)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT chk_firms_um_entorno
        CHECK (
            NOT (id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NOT NULL)
        ),

    CONSTRAINT chk_firms_tipo_cruzamento
        CHECK (
            (tipo_cruzamento = 'UC' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NULL)
            OR (tipo_cruzamento = 'ZA' AND id_za_oficial IS NOT NULL AND id_buffer_abrangencia IS NULL)
            OR (tipo_cruzamento = 'BUFFER_ABRANGENCIA' AND id_za_oficial IS NULL AND id_buffer_abrangencia IS NOT NULL)
        ),
    CONSTRAINT chk_firms_processing CHECK (source_processing IN ('NRT', 'SP', 'ARCHIVE')),
    CONSTRAINT chk_firms_confidence_scheme CHECK (confidence_scheme IN ('VIIRS_CATEGORY', 'MODIS_PERCENT')),
    CONSTRAINT chk_firms_confidence_score CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 100)),
    CONSTRAINT chk_firms_daynight CHECK (daynight IS NULL OR daynight IN ('D', 'N'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_firms_clip_detection_aoi
    ON firms_clip (
        detection_id,
        id_uc,
        tipo_cruzamento,
        COALESCE(id_za_oficial, 0),
        COALESCE(id_buffer_abrangencia, 0)
    );

CREATE INDEX IF NOT EXISTS idx_firms_uc
    ON firms_clip (id_uc);

CREATE INDEX IF NOT EXISTS idx_firms_za_oficial
    ON firms_clip (id_za_oficial);

CREATE INDEX IF NOT EXISTS idx_firms_buffer_abrangencia
    ON firms_clip (id_buffer_abrangencia);

CREATE INDEX IF NOT EXISTS idx_firms_geom
    ON firms_clip
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_firms_clip_acquired_at
    ON firms_clip (acquired_at_utc, source_product);

CREATE TABLE IF NOT EXISTS firms_backfill_window (
    id_firms_backfill_window BIGSERIAL PRIMARY KEY,
    source_product          VARCHAR(100) NOT NULL,
    start_date              DATE NOT NULL,
    end_date                DATE NOT NULL,
    processing_state        VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempt_count           INTEGER NOT NULL DEFAULT 0,
    manifest_key            VARCHAR(1024),
    source_checksum         CHAR(64),
    run_id                  VARCHAR(250),
    last_error              TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at            TIMESTAMPTZ,
    CONSTRAINT uq_firms_backfill_window UNIQUE (source_product, start_date, end_date),
    CONSTRAINT chk_firms_backfill_window_dates CHECK (start_date <= end_date AND end_date - start_date <= 4),
    CONSTRAINT chk_firms_backfill_window_state
        CHECK (processing_state IN ('PENDING', 'RUNNING', 'PUBLISHED', 'FAILED')),
    CONSTRAINT chk_firms_backfill_window_attempts CHECK (attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_firms_backfill_window_pending
    ON firms_backfill_window (processing_state, start_date, source_product);

-- =========================================================
-- COMENTÁRIOS ÚTEIS
-- =========================================================
COMMENT ON TABLE uc IS 'Unidades de Conservação';
COMMENT ON TABLE za_oficial IS 'Zonas de Amortecimento oficiais, com histórico e indicação de vigência';
COMMENT ON TABLE buffer_abrangencia IS 'Buffers de Abrangência gerados pelo pipeline';
COMMENT ON TABLE prodes_clip IS 'Recortes de desmatamento anual do PRODES relacionados a UCs e zonas de entorno';
COMMENT ON TABLE mapbiomas_clip IS 'Recortes de uso e cobertura do solo do MapBiomas relacionados a UCs e zonas de entorno';
COMMENT ON TABLE mapbiomas_alerta_clip IS 'Alertas do MapBiomas Alerta recortados para UCs e zonas de entorno';
COMMENT ON TABLE firms_clip IS 'Focos de calor do FIRMS recortados para UCs e zonas de entorno';

-- BEGIN CADASTRAL_AND_RASTER_BASELINE
-- Definicoes canonicas aditivas; aplicaveis tambem ao banco de desenvolvimento.
ALTER TABLE public.uc
    ADD COLUMN IF NOT EXISTS situacao VARCHAR(10) NOT NULL DEFAULT 'ATIVA',
    ADD COLUMN IF NOT EXISTS dt_inicio_vigencia DATE NOT NULL DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS dt_fim_vigencia DATE,
    ADD COLUMN IF NOT EXISTS criado_por VARCHAR(255);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_uc_situacao' AND conrelid = 'public.uc'::regclass) THEN
        ALTER TABLE public.uc ADD CONSTRAINT ck_uc_situacao CHECK (situacao IN ('ATIVA', 'EXTINTA'));
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_uc_cd_cnuc
    ON public.uc (cd_cnuc) WHERE cd_cnuc IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.uc_geometry_version (
    id_geometry_version BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    id_uc BIGINT NOT NULL REFERENCES public.uc(id_uc) ON UPDATE CASCADE ON DELETE RESTRICT,
    numero_versao BIGINT NOT NULL,
    geom geometry(Geometry, 4674) NOT NULL,
    fl_ativa BOOLEAN NOT NULL DEFAULT TRUE,
    dt_inicio_vigencia DATE NOT NULL DEFAULT CURRENT_DATE,
    dt_fim_vigencia DATE,
    motivo TEXT NOT NULL,
    fonte VARCHAR(255) NOT NULL,
    ator VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_uc_geometry_version UNIQUE (id_uc, numero_versao),
    CONSTRAINT ck_uc_geometry_version_periodo CHECK (
        dt_fim_vigencia IS NULL OR dt_fim_vigencia >= dt_inicio_vigencia
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_uc_geometry_version_ativa
    ON public.uc_geometry_version (id_uc) WHERE fl_ativa = TRUE;
CREATE INDEX IF NOT EXISTS idx_uc_geometry_version_geom
    ON public.uc_geometry_version USING GIST (geom);

-- Recupera somente snapshots sem historico; nao inventa versoes/eventos passados.
INSERT INTO public.uc_geometry_version (
    id_uc, numero_versao, geom, fl_ativa, dt_inicio_vigencia,
    motivo, fonte, ator, correlation_id
)
SELECT u.id_uc, u.versao_registro, u.geom, TRUE,
       COALESCE(u.dt_criacao, u.dt_carga::date, CURRENT_DATE),
       'Snapshot inicial do cadastro preexistente; historico anterior indisponivel',
       'baseline', 'baseline', 'baseline-cadastral'
FROM public.uc u
WHERE NOT EXISTS (SELECT 1 FROM public.uc_geometry_version v WHERE v.id_uc = u.id_uc);

CREATE TABLE IF NOT EXISTS public.cadastral_event (
    event_id UUID PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    actor VARCHAR(255) NOT NULL,
    reason TEXT NOT NULL,
    source VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    previous_state JSONB,
    new_state JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cadastral_event_entity
    ON public.cadastral_event (entity_type, entity_id, occurred_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cadastral_event_idempotency
    ON public.cadastral_event (idempotency_key);

-- Tabelas anuais sao instanciadas somente na carga de um ano disponivel.
-- O loader usa esta definicao, evitando um segundo DDL divergente em Python.
CREATE OR REPLACE FUNCTION public.ensure_mapbiomas_raster_year(p_year INTEGER)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    raster_table TEXT;
BEGIN
    IF p_year IS NULL OR p_year < 1985 OR p_year > 2100 THEN
        RAISE EXCEPTION 'Invalid MapBiomas year: %', p_year;
    END IF;
    raster_table := 'mapbiomas_raster_' || p_year;
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS public.%I (
            id_raster_tile BIGSERIAL PRIMARY KEY,
            id_raster_asset BIGINT NOT NULL REFERENCES public.mapbiomas_raster_asset(id_raster_asset) ON DELETE CASCADE,
            collection_code VARCHAR(50) NOT NULL,
            collection_version VARCHAR(50) NOT NULL,
            reference_year INTEGER NOT NULL,
            tile_row INTEGER NOT NULL,
            tile_col INTEGER NOT NULL,
            rast raster NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT %I UNIQUE (id_raster_asset, tile_row, tile_col),
            CONSTRAINT %I CHECK (tile_row >= 0 AND tile_col >= 0)
        )', raster_table, 'uq_' || raster_table || '_pos', 'chk_' || raster_table || '_pos'
    );
END $$;

COMMENT ON TABLE public.uc_geometry_version IS 'Historico de geometria e vigencia das UCs; uma versao ativa por UC.';
COMMENT ON TABLE public.cadastral_event IS 'Eventos cadastrais confirmados pelo pipeline e chave de idempotencia.';
-- END CADASTRAL_AND_RASTER_BASELINE
