-- ============================================================
-- Mutation Testing Report - Database Initialization
-- ============================================================
-- Schema   : mutation_test_report
-- Database : protected-areas-sc-db-mutation
-- Execute via:
--   docker compose exec protected-areas-sc-db-mutation \
--     psql -U mutation -d protected-areas-sc-db-mutation -f /docker-entrypoint-initdb.d/10_init_mutation_db.sql
-- Ou é executado automaticamente pelo docker-entrypoint-initdb.d/ na primeira inicialização.
-- ============================================================
-- Campos CALCULADOS (não persistidos como coluna separada):
--   mutation_score  = killed / total * 100
--   tested_score    = killed / (total - no_coverage) * 100
--   coverage        = (total - no_coverage) / total * 100
--   Bloco "overall" = view vw_mutation_overall sobre mutation_project
-- ============================================================

-- -------------------------------------------------------------
-- Schema
-- -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS mutation_test_report;

SET search_path TO mutation_test_report;

-- -------------------------------------------------------------
-- ENUM: tipo de finding de mutação
-- -------------------------------------------------------------
DO $$
BEGIN
    CREATE TYPE mutation_test_report.mutation_finding_type
        AS ENUM ('survivor', 'no_coverage', 'timeout');
EXCEPTION WHEN duplicate_object THEN NULL;
END
$$;

-- -------------------------------------------------------------
-- 1. PROJETO
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mutation_test_report.mutation_project (
    id               SERIAL        PRIMARY KEY,
    name             VARCHAR(255)  NOT NULL,
    status           VARCHAR(50)   NOT NULL,            -- 'success' | 'failed'
    total_mutations  INT           NULL,                -- NULL quando status = 'failed'
    killed           INT           NULL,
    survived         INT           NULL,
    no_coverage      INT           NULL,
    timed_out        INT           NULL,
    error_message    TEXT          NULL,                -- preenchido quando status = 'failed'
    created_at       TIMESTAMP     NOT NULL DEFAULT NOW(),

    -- Scores calculados e persistidos automaticamente pelo Postgres
    mutation_score   NUMERIC(5, 2) GENERATED ALWAYS AS (
                         CASE WHEN total_mutations > 0
                              THEN ROUND((killed::NUMERIC / total_mutations) * 100, 2)
                         END
                     ) STORED,

    tested_score     NUMERIC(5, 2) GENERATED ALWAYS AS (
                         CASE WHEN (total_mutations - COALESCE(no_coverage, 0)) > 0
                              THEN ROUND(
                                       killed::NUMERIC / (total_mutations - COALESCE(no_coverage, 0)) * 100,
                                   2)
                         END
                     ) STORED,

    coverage         NUMERIC(5, 2) GENERATED ALWAYS AS (
                         CASE WHEN total_mutations > 0
                              THEN ROUND(
                                       (total_mutations - COALESCE(no_coverage, 0))::NUMERIC / total_mutations * 100,
                                   2)
                         END
                     ) STORED
);

-- Índice para buscar histórico de um projeto por data
CREATE INDEX IF NOT EXISTS idx_mutation_project_name
    ON mutation_test_report.mutation_project (name, created_at DESC);

-- -------------------------------------------------------------
-- 2. HOTSPOTS  (arquivos com mais mutações / problemas)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mutation_test_report.mutation_hotspot (
    id               SERIAL        PRIMARY KEY,
    project_id       INT           NOT NULL
                         REFERENCES mutation_test_report.mutation_project(id)
                         ON DELETE CASCADE,
    file             VARCHAR(500)  NOT NULL,
    total_mutations  INT           NOT NULL DEFAULT 0,
    problems         INT           NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_hotspot_project
    ON mutation_test_report.mutation_hotspot (project_id);

-- -------------------------------------------------------------
-- 3. FINDINGS  (survivors, no-coverage e timeouts)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mutation_test_report.mutation_finding (
    id          SERIAL                                   PRIMARY KEY,
    project_id  INT                                      NOT NULL
                    REFERENCES mutation_test_report.mutation_project(id)
                    ON DELETE CASCADE,
    type        mutation_test_report.mutation_finding_type NOT NULL,
    file        VARCHAR(500)                             NOT NULL,
    line        INT                                      NOT NULL,
    method      VARCHAR(255)                             NOT NULL,
    mutator     VARCHAR(255)                             NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_finding_project
    ON mutation_test_report.mutation_finding (project_id);

CREATE INDEX IF NOT EXISTS idx_finding_project_type
    ON mutation_test_report.mutation_finding (project_id, type);

-- -------------------------------------------------------------
-- 4. TESTES QUE MAIS MATAM MUTANTES
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mutation_test_report.mutation_killing_test (
    id          SERIAL        PRIMARY KEY,
    project_id  INT           NOT NULL
                    REFERENCES mutation_test_report.mutation_project(id)
                    ON DELETE CASCADE,
    test_name   VARCHAR(500)  NOT NULL,
    kills       INT           NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_killing_test_project
    ON mutation_test_report.mutation_killing_test (project_id);

-- =============================================================
-- VIEW: consolidação geral (bloco "overall")
-- =============================================================
CREATE OR REPLACE VIEW mutation_test_report.vw_mutation_overall AS
SELECT
    COUNT(*)                                                                   AS total_projects,
    SUM(total_mutations)                                                       AS total_mutations,
    SUM(killed)                                                                AS killed,
    SUM(survived)                                                              AS survived,
    SUM(no_coverage)                                                           AS no_coverage,
    SUM(timed_out)                                                             AS timed_out,

    ROUND(
        SUM(killed)::NUMERIC / NULLIF(SUM(total_mutations), 0) * 100,
    2)                                                                         AS mutation_score,

    ROUND(
        SUM(killed)::NUMERIC / NULLIF(SUM(total_mutations) - SUM(no_coverage), 0) * 100,
    2)                                                                         AS tested_score,

    ROUND(
        (SUM(total_mutations) - SUM(no_coverage))::NUMERIC / NULLIF(SUM(total_mutations), 0) * 100,
    2)                                                                         AS coverage
FROM mutation_test_report.mutation_project
WHERE status = 'success';

-- =============================================================
-- Confirmar estrutura criada
-- =============================================================
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || quote_ident(tablename))) AS tamanho
FROM pg_tables
WHERE schemaname = 'mutation_test_report'
ORDER BY tablename;
