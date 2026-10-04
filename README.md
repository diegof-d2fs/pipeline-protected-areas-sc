# pipeline-protected-areas-sc

Pipeline geoespacial orquestrado para monitoramento de Unidades de Conservacao (UCs) e seus entornos em Santa Catarina, com arquitetura baseada em Apache Airflow, PostgreSQL/PostGIS e camadas Medallion locais (bronze, silver, gold).

Este repositorio implementa o backend tecnico do TCC, com foco em ingestao, padronizacao, validacao, transformacao e persistencia de dados geoespaciais de diferentes fontes ambientais. A arquitetura foi desenhada para uso local no MVP atual e preparada para evolucao futura para AWS/S3.

---

## Sumario

**Contexto e dominio**
- [Introducao e contexto](#introducao-e-contexto)
- [Importancia de monitorar UCs](#importancia-de-monitorar-ucs)
- [ZA e Buffer de Abrangência](#za-e-buffer)
- [Bases de dados utilizadas](#bases-de-dados-utilizadas)

**Arquitetura e modelo de dados**
- [Arquitetura backend](#arquitetura-backend)
- [Modelo de dados principal](#modelo-de-dados-principal)
- [Containers e bancos criados no build](#containers-e-bancos-criados-no-build)
- [Estrutura de pastas](#estrutura-de-pastas)

**DAGs e orquestracao**
- [DAGs do backend Airflow](#dags-do-backend-airflow)
- [Reprocessamento e resiliencia](#reprocessamento-e-resiliencia)
- [Cobertura de requisitos do PRD](#cobertura-de-requisitos-do-prd-escopo-backend)

**Qualidade de dados**
- [Observabilidade de qualidade (rejeicoes)](#observabilidade-de-qualidade-rejeicoes)
  - [Estrutura de particionamento](#estrutura-de-particionamento)
  - [Arquivos gerados por execucao](#arquivos-gerados-por-execucao)
  - [Estrutura do summary.json](#estrutura-do-summaryjson)
  - [Motivos padronizados de rejeicao](#motivos-padronizados-de-rejeicao)
  - [Politica de versionamento](#politica-de-versionamento)

**Metricas operacionais**
- [Metricas de execucao das DAGs](#metricas-de-execucao-das-dags)
  - [Acessar o banco de metadados](#acessar-o-banco-de-metadados)
  - [Consulta: duracao e falhas por DAG](#consulta-duracao-e-falhas-por-dag)
  - [Consulta: detalhe de falhas por task](#consulta-detalhe-de-falhas-por-task)
  - [Consulta: historico completo run a run](#consulta-historico-completo-run-a-run)
  - [Resultado do MVP (execucoes reais)](#resultado-do-mvp-execucoes-reais)

**Operacao**
- [Build e execucao (Windows e Linux)](#build-e-execucao-windows-e-linux)
  - [Opcao rapida (one-command)](#opcao-rapida-one-command)
  - [Quando usar docker compose vs setup](#quando-usar-docker-compose-vs-setup)
- [Comandos operacionais](#comandos-operacionais)
- [Setup automatizado](#setup-automatizado)

**Evolucao**
- [Evolucao planejada](#evolucao-planejada)

---

## Introducao e contexto

No monitoramento ambiental, a maior dor operacional normalmente nao e a falta de dados, e sim a falta de padronizacao e automacao para transformar dados heterogeneos em informacao reutilizavel. No caso das UCs, isso impacta diretamente:

- rastreabilidade de mudancas territoriais
- consistencia dos recortes espaciais por periodo
- capacidade de comparar fontes diferentes com confiabilidade
- rapidez para gerar insumos tecnicos para analise, pesquisa e apoio a decisao

Este projeto reduz retrabalho manual com um pipeline reprocessavel e idempotente, com regras formais de geometria, dependencia entre DAGs e dupla persistencia (PostGIS + Medallion).

## Importancia de monitorar UCs

As UCs sao espacos protegidos para conservacao ambiental e exigem acompanhamento continuo de pressao no territorio (desmatamento, queimadas, alteracao de uso e cobertura, alertas validados etc.).

Sem um pipeline estruturado:

- recortes espaciais sao refeitos manualmente
- resultados perdem reprodutibilidade
- aumenta risco de inconsistencias de CRS/geometria
- cresce o tempo para produzir analises tecnicas confiaveis

Com a automacao deste backend, as bases ficam organizadas para consumo analitico recorrente, mantendo historico operacional e rastreabilidade por execucao.

## ZA e Buffer de Abrangência

No dominio do projeto, as zonas de entorno sao tratadas em dois tipos:

- ZA (Zona de Amortecimento oficial): zona formalmente instituida para a UC, quando existe delimitacao oficial.
- Buffer de Abrangência: poligono gerado pelo pipeline quando nao ha ZA oficial, usando buffer de 3000m em torno da UC.

Base legal e normativa utilizada no projeto:

- Lei Federal 9.985/2000 (SNUC), que estrutura o sistema de UCs.
- Resolucao CONAMA 428/2010, adotada como referencia tecnica para o criterio operacional de entorno quando nao houver ZA estabelecida.

Importante: o Buffer de Abrangência e uma estrategia operacional do pipeline para analise tecnica; nao substitui juridicamente uma ZA oficial.

### Precedencia e descoberta tardia de ZA oficial

Uma ZA oficial ativa tem precedencia sobre o Buffer de Abrangência da mesma UC. Quando a ZA e ativada, o Buffer de Abrangência
vigente e encerrada (`fl_ativa=false` e fim de vigencia), mas permanece no historico. Indices
parciais e triggers no PostGIS impedem ZA e Buffer de Abrangência simultaneamente ativos para a mesma UC.

O pipeline aceita duas trajetorias:

- execucao dirigida pela FastAPI com `operation=replace_buffer_abrangencia`, `uc_identifier` explicito e
  justificativa; o encerramento do Buffer de Abrangência, a insercao/versionamento da ZA e a auditoria ocorrem na
  mesma transacao;
- execucao agendada sobre a Bronze legada: uma ZA com `ds_fonte` e autoritativa e pode ser
  descoberta quando a UC correspondente passar a existir. A associacao espacial e permitida
  somente nesse fluxo legado autoritativo.

Uma importacao dirigida pela API nunca usa associacao espacial implicita. Falha na transicao faz
rollback, e o replay do mesmo `import_id` e idempotente. Depois do commit, apenas as bases
tematicas implementadas atualmente, `DAG_PRODES`, `DAG_MAPBIOMAS_ALERTA` e
`DAG_MAPBIOMAS`, sao reprocessadas. MapBiomas processa todos os anos disponiveis.

## Bases de dados utilizadas

O backend foi desenhado para integrar e padronizar principalmente as seguintes fontes tematicas:

- UCs: base de referencia territorial das unidades de conservacao.
- ZA oficial: delimitacoes oficiais de zonas de amortecimento quando disponiveis.
- PRODES: recortes de desmatamento anual relacionados a UCs e entornos.
- MapBiomas Uso e Cobertura: classes tematicas por ano para analise territorial.
- MapBiomas Alerta: alertas validados de supressao de vegetacao no recorte de interesse.
- FIRMS: focos de calor para monitoramento de ocorrencias de alta frequencia.

## Arquitetura backend

Componentes principais:

- orquestracao: Apache Airflow 2.8
- persistencia espacial: PostgreSQL 17 + PostGIS 3.5
- armazenamento em camadas: Medallion local em disco
- scripts operacionais multiplataforma: PowerShell e Bash

Fluxo padrao de cada DAG:

1. extract
2. validate
3. transform
4. load_postgres
5. load_silver
6. load_gold

Dependencias de orquestracao:

1. DAG_UCS
2. DAG_ZA_BUFFER
3. DAGs tematicas dependentes das duas anteriores

### Cardinalidade da entrada dirigida pela API

O fluxo dirigido aceita uma ou varias UCs em GeoJSON ou em ZIP contendo um Shapefile. A carga
usa identidade por feature, verifica duplicidades, grava o lote em uma transacao e registra um
evento por UC. A validacao ponta a ponta dos lotes ZIP e GeoJSON reais de tres UCs foi concluida
em 13/09/2026, incluindo consulta dos resultados no PostGIS.

Observacao operacional:

- DAG_MAPBIOMAS_ALERTA possui fluxo dedicado e pode executar sem sensor externo; quando executada antes de DAG_UCS/DAG_ZA_BUFFER, os registros sem correspondencia espacial sao auditados em data quality.

## Build e execucao (Windows e Linux)

Este projeto pode ser executado em Windows (PowerShell) e Linux/WSL (Bash).

Pre-requisitos:

- Docker 20+ e Docker Compose v2
- PowerShell 5.1+ (Windows) ou Bash (Linux/WSL)

### Opcao rapida (one-command)

Sim, e possivel subir o projeto inteiro com um comando na raiz do repositorio:

Arquivo de compose oficial (unico): airflow/docker-compose.yaml.

```bash
docker compose -f airflow/docker-compose.yaml --env-file airflow/.env up -d --build
```

Observacao importante: o compose usa `airflow/.env`. Se o arquivo ainda nao existir no seu ambiente, crie antes com:

- Linux/WSL: `cp airflow/.env.example airflow/.env`
- PowerShell: `Copy-Item airflow/.env.example airflow/.env`

Quando usar a opcao one-command, voce sobe a stack completa, mas nao executa automaticamente os passos auxiliares dos scripts de setup (como `seed_bronze_local` e `update_project_files`).

### Quando usar docker compose vs setup

- Use `docker compose ... up -d --build` quando voce so precisa subir a infraestrutura (containers, rede e volumes).
- Use `./airflow/setup.ps1 setup` (Windows) ou `./airflow/setup.sh setup` (Linux/WSL) quando quiser o fluxo completo de operacao do projeto.

O `setup` inclui passos auxiliares alem do compose, como validacao de ambiente e comandos operacionais adicionais (por exemplo: `validate_env`, `seed_bronze_local`, `init_db` e `update_project_files`).

Passos:

1. Copiar variaveis de ambiente
   - Linux/WSL: cp airflow/.env.example airflow/.env
   - PowerShell: Copy-Item airflow/.env.example airflow/.env
2. Subir stack completa
   - Linux/WSL: ./airflow/setup.sh setup
   - PowerShell: ./airflow/setup.ps1 setup
3. Acessar Airflow
   - http://localhost:8080

Credenciais default do Airflow:

- usuario: admin
- senha: admin

## Containers e bancos criados no build

Todos os recursos seguem o padrao de nomenclatura com prefixo protected-areas-sc.

Containers principais:

- protected-areas-sc-airflow-webserver: interface web e API do Airflow
- protected-areas-sc-airflow-scheduler: agendamento e execucao das DAGs
- protected-areas-sc-airflow-triggerer: processamento de tasks deferrable
- protected-areas-sc-airflow-init: bootstrap inicial do Airflow (migracao e usuario admin)
- protected-areas-sc-airflow-db: banco de metadados internos do Airflow
- protected-areas-sc-db-main: banco principal do dominio geoespacial (PostGIS)
- protected-areas-sc-db-mutation: banco auxiliar para cenarios de mutation/testing

Bancos e finalidade:

- Airflow metadata DB: controle interno de DAGs, runs, tasks e estado de orquestracao
- Main PostGIS DB: tabelas de dominio (uc, za_oficial, buffer_abrangencia, prodes_clip, mapbiomas_clip, mapbiomas_alerta_clip, firms_clip)
- Mutation DB: persistencia de artefatos de testes/experimentos auxiliares

## Estrutura de pastas

Estrutura principal do scaffold:

- airflow/dags: DAGs de dominio e componentes Python reutilizaveis
- airflow/dags/scripts_python: classes e servicos do pipeline
- airflow/dags/sql: SQL de apoio a operadores
- airflow/data/bronze: dados brutos de entrada
- airflow/data/silver: dados tratados/intermediarios
- airflow/data/gold: dados finais deduplicados
- airflow/data/raw: area auxiliar de dados de origem
- airflow/data/tmp: artefatos temporarios de processamento
- airflow/data/quality: artefatos de qualidade/rejeicoes por run e stage
- airflow/logs: logs do runtime do Airflow
- airflow/plugins: plugins customizados do Airflow
- airflow/scripts: scripts auxiliares operacionais
- airflow/.env.example: referencia de configuracao por ambiente
- airflow/docker-compose.yaml: composicao da stack Airflow/PostGIS
- airflow/setup.ps1 e airflow/setup.sh: operacao multiplataforma da stack

## Comandos operacionais

Scripts suportados:

- setup: valida ambiente, prepara estrutura local e sobe stack
- start: inicia servicos
- stop: encerra servicos
- restart: reinicia servicos
- status: lista estado dos containers
- logs [service]: exibe logs gerais ou por servico
- init_db: aplica schema principal no banco PostGIS
- validate_env: valida variaveis obrigatorias
- seed_bronze_local: cria estrutura inicial da camada bronze
- reprocess <dag_id> <periodo>: dispara reprocessamento com conf por periodo
- update_project_files: atualiza README e gitignore conforme ambiente

## DAGs do backend Airflow

Implementadas como templates operacionais:

- DAG_UCS
- DAG_ZA_BUFFER
- DAG_PRODES
- DAG_MAPBIOMAS
- DAG_MAPBIOMAS_ALERTA
- DAG_FIRMS

As DAGs tematicas usam services dedicados. No fluxo cadastral, PRODES, MapBiomas Alerta e
MapBiomas Uso/Cobertura e disparado e aguardado no fluxo cadastral. FIRMS opera de forma independente em uma DAG semanal NRT e outra DAG manual de backfill historico.

## Cobertura de requisitos do PRD (escopo backend)

Requisitos funcionais cobertos por este backend scaffold:

- RF01, RF02, RF03, RF04, RF05, RF06, RF07, RF08, RF09, RF10, RF11, RF12

Requisitos nao funcionais cobertos no backend:

- RNF01, RNF02, RNF03, RNF04, RNF05, RNF06, RNF07, RNF08, RNF09, RNF10, RNF11, RNF12

Observacao: itens de interface web e UX estao fora do escopo desta fase (roadmap futuro).

## Modelo de dados principal

Schema base definido em init_db.sql com foco em integridade espacial e referencial:

- uc
- za_oficial
- buffer_abrangencia
- prodes_clip
- mapbiomas_clip
- mapbiomas_alerta_clip
- firms_clip
- uc_geometry_version
- cadastral_event
- mapbiomas_raster_asset e mapbiomas_raster_<ano>

Destaques de modelagem:

- uc.geom como geometry(Geometry, 4674), com politica operacional para Point e MultiPolygon
- indices GiST para geometrias
- restricoes de exclusividade de entorno nas tabelas tematicas (za_oficial xor buffer)
- unicidade de zona ativa por UC para ZA oficial e Buffer de Abrangência

## Reprocessamento e resiliencia

Parametros operacionais padrao:

- minimo de 3 retries por task
- backoff exponencial
- reprocessamento por DAG e periodo
- idempotencia por execucao/periodo para evitar duplicacao

## Observabilidade de qualidade (rejeicoes)

Cada pipeline de dominio gera, de forma autonoma ao final do stage `load_postgres`, um conjunto de artefatos de auditoria em disco. Esses artefatos nao dependem de DAG auxiliar nem de banco externo — sao gravados diretamente pelo servico Python de cada pipeline na camada `airflow/data/quality/`.

### Estrutura de particionamento

Os artefatos seguem o padrao Hive-style para facilitar consulta por run ou por periodo:

```
airflow/data/quality/rejections/
  <dominio>/
    run_id=<run_id>/
      branch=<branch>/
        stage=<stage>/
          summary.json
          rejected_rows.csv
          rejected_rows.geojson
```

Exemplos reais gerados no MVP:

```
rejections/ucs/run_id=scheduled__2026-04-20T00_00_00_00_00/branch=ucs/stage=load_postgres/
rejections/za_buffer/run_id=scheduled__2026-04-20T00_00_00_00_00/branch=za/stage=load_postgres_za/
rejections/za_buffer/run_id=scheduled__2026-04-20T00_00_00_00_00/branch=buffer/stage=load_postgres_buffer/
rejections/prodes/run_id=manual__2026-04-19T18_47_21.../branch=prodes/stage=load_postgres/
rejections/mapbiomas_alerta/run_id=scheduled__2026-04-19T12_00_00_00_00/branch=mapbiomas_alerta/stage=load_postgres/
```

### Arquivos gerados por execucao

Tres artefatos sao criados em cada pasta de stage:

- `summary.json`: totalizadores da carga com breakdown de rejeicoes por reason_code
- `rejected_rows.csv`: registros rejeitados linha a linha com reason_code e descricao
- `rejected_rows.geojson`: subconjunto dos rejeitados que possuem geometria valida, para inspecao espacial direta em QGIS ou similar

### Estrutura do summary.json

```json
{
  "domain": "prodes",
  "branch": "prodes",
  "stage": "load_postgres",
  "dag_id": "DAG_PRODES",
  "run_id": "manual__2026-04-19T18:47:21.998075+00:00",
  "logical_date": "2026-04-19T18:47:21.998075+00:00",
  "generated_at_utc": "2026-04-19T18:49:56.601070+00:00",
  "source_records": 146796,
  "inserted_records": 657,
  "inserted_new_records": 657,
  "inserted_updated_records": 0,
  "skipped_records": 146139,
  "result_reason": "loaded_postgres",
  "result_reason_description": "Carga no Postgres concluida com sucesso.",
  "rejection_counts": {
    "NO_UC_INTERSECTION": 146139
  },
  "rejection_reason_details": [
    {
      "reason_code": "NO_UC_INTERSECTION",
      "reason_description": "Registro nao intersecta nenhuma UC.",
      "count": 146139
    }
  ]
}
```

O campo `result_reason` representa o desfecho geral da task (sucesso ou motivo de nao-carga). O campo `rejection_counts` detalha quantos registros foram descartados por cada regra de negocio, sem que a task falhe.

### Motivos padronizados de rejeicao

| reason_code | Dominio | Descricao |
|---|---|---|
| `NO_UC_ID` | UCS | Registro sem uc_id valido na origem |
| `NO_UC_FK_MATCH` | ZA/Buffer de Abrangência | Sem correspondencia de UC por chave e intersecao espacial |
| `SKIPPED_BY_UPDATE_RULE` | UCS, ZA/Buffer de Abrangência | Registro existente com update_geom diferente de TRUE |
| `UC_HAS_OFFICIAL_ZA` | ZA/Buffer de Abrangência | Registro de Buffer de Abrangência bloqueado pois UC possui ZA oficial ativa |
| `INVALID_OR_EMPTY_GEOMETRY` | Todos | Geometria nula ou vazia apos transformacao |
| `NO_UC_INTERSECTION` | PRODES, MapBiomas, FIRMS | Registro sem intersecao com nenhuma UC |
| `NO_ZONE_INTERSECTION` | PRODES, MapBiomas | Sem intersecao com zona de entorno esperada (ZA ou Buffer de Abrangência) |
| `NO_ACTIVE_ZONE_FOR_UC` | PRODES, MapBiomas | UC sem zona ativa no ramo aplicado |
| `DUPLICATE_ALERT_ZONE_KEY` | MapBiomas Alerta | Duplicado no lote por chave alerta/uc/entorno |

### Politica de versionamento

O conteudo de `airflow/data/quality/` e gerado por execucao e nao e commitado no repositorio (ver `.gitignore`). Para manter historico tecnico de uma execucao especifica, copie os artefatos desejados para `docs/` ou publique como anexo externo.

## Metricas de execucao das DAGs

O Apache Airflow armazena o historico completo de execucao das DAGs no banco de metadados interno (`protected-areas-sc-airflow-db`), nas tabelas `dag_run` e `task_instance`. Nao e necessaria nenhuma DAG auxiliar para consultar esses dados.

### Acessar o banco de metadados

```bash
docker exec -it protected-areas-sc-airflow-db psql -U airflow -d airflow
```

Credenciais: usuario `airflow`, senha `airflow`, banco `airflow` (valores default do `.env.example`).

### Consulta: duracao e falhas por DAG

```sql
-- Resumo por DAG: runs com sucesso, duracao media representativa e falhas
SELECT
    dag_id,
    COUNT(*) FILTER (WHERE state = 'success')                                               AS runs_sucesso,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (end_date - start_date)))
        FILTER (
            WHERE state = 'success'
              AND start_date IS NOT NULL
              AND EXTRACT(EPOCH FROM (end_date - start_date)) BETWEEN 0 AND 7200
        ), 1
    )                                                                                        AS duracao_media_s,
    COUNT(*) FILTER (WHERE state = 'failed')                                                AS falhas
FROM dag_run
WHERE dag_id IN ('DAG_UCS', 'DAG_ZA_BUFFER', 'DAG_PRODES', 'DAG_MAPBIOMAS_ALERTA')
GROUP BY dag_id
ORDER BY dag_id;
```

Executar sem entrar no container (modo nao-interativo):

**Ubuntu/bash:**
```bash
docker exec protected-areas-sc-airflow-db psql -U airflow -d airflow -c \
"SELECT dag_id,
        COUNT(*) FILTER (WHERE state = 'success') AS runs_sucesso,
        ROUND(AVG(EXTRACT(EPOCH FROM (end_date - start_date)))
              FILTER (WHERE state = 'success' AND start_date IS NOT NULL
                        AND EXTRACT(EPOCH FROM (end_date - start_date)) BETWEEN 0 AND 7200), 1) AS duracao_media_s,
        COUNT(*) FILTER (WHERE state = 'failed') AS falhas
 FROM dag_run
 WHERE dag_id IN ('DAG_UCS','DAG_ZA_BUFFER','DAG_PRODES','DAG_MAPBIOMAS_ALERTA')
 GROUP BY dag_id ORDER BY dag_id;"
```

**Windows PowerShell** (query em uma unica linha — `\` nao funciona como continuador):
```powershell
docker exec protected-areas-sc-airflow-db psql -U airflow -d airflow -c "SELECT dag_id, COUNT(*) FILTER (WHERE state = 'success') AS runs_sucesso, ROUND(AVG(EXTRACT(EPOCH FROM (end_date - start_date))) FILTER (WHERE state = 'success' AND start_date IS NOT NULL AND EXTRACT(EPOCH FROM (end_date - start_date)) BETWEEN 0 AND 7200), 1) AS duracao_media_s, COUNT(*) FILTER (WHERE state = 'failed') AS falhas FROM dag_run WHERE dag_id IN ('DAG_UCS','DAG_ZA_BUFFER','DAG_PRODES','DAG_MAPBIOMAS_ALERTA') GROUP BY dag_id ORDER BY dag_id;"
```

### Consulta: detalhe de falhas por task

```sql
-- Quais tasks falharam e quantas vezes
SELECT dag_id, task_id, state, COUNT(*) AS ocorrencias
FROM task_instance
WHERE dag_id IN ('DAG_UCS', 'DAG_ZA_BUFFER', 'DAG_PRODES', 'DAG_MAPBIOMAS_ALERTA')
  AND state = 'failed'
GROUP BY dag_id, task_id, state
ORDER BY dag_id, task_id;
```

### Consulta: historico completo run a run

```sql
-- Todas as execucoes com duracao calculada
SELECT
    dag_id,
    run_id,
    state,
    start_date,
    end_date,
    ROUND(EXTRACT(EPOCH FROM (end_date - start_date))::numeric, 1) AS duracao_s
FROM dag_run
WHERE dag_id IN ('DAG_UCS', 'DAG_ZA_BUFFER', 'DAG_PRODES', 'DAG_MAPBIOMAS_ALERTA')
  AND end_date IS NOT NULL
ORDER BY dag_id, start_date;
```

### Resultado do MVP (execucoes reais)

| DAG | Runs com sucesso | Duracao media (s) | Falhas |
|---|---|---|---|
| DAG_UCS | 10 | 13,0 | 0 |
| DAG_ZA_BUFFER | 9 | 42,6 | 6* |
| DAG_PRODES | 4 | 630,4 | 0 |
| DAG_MAPBIOMAS_ALERTA | 9 | 155,6 | 1** |

*Falhas da DAG_ZA_BUFFER ocorreram durante ciclos de ajuste de regras espaciais no desenvolvimento, nao em operacao estavel.

**Falha da DAG_MAPBIOMAS_ALERTA com start_date NULL — run rejeitada pelo scheduler antes de iniciar, sem impacto em dados.

## Evolucao planejada

Preparado para evoluir com baixo acoplamento para:

- persistencia Medallion em AWS/S3
- governanca e auditoria ampliadas
- camada de interface web de gestao operacional no TCC III

## Setup automatizado

Os scripts [airflow/setup.ps1](airflow/setup.ps1) e [airflow/setup.sh](airflow/setup.sh) aplicam validacoes de ambiente e operacao da stack.

Comandos principais:

- setup
- start
- stop
- restart
- status
- logs
- init_db
- validate_env
- seed_bronze_local
- reprocess <dag_id> <periodo>
- update_project_files


## CI/CD na AWS

O workflow [.github/workflows/ci.yml](.github/workflows/ci.yml) executa a suíte completa
com PostGIS em pushes no main e pull requests. Depois dos testes, um push no main publica
a imagem do Airflow no ECR e registra seu commit em /pa-sc/prod/deploy/airflow_image_tag.

O deploy exige AWS_DEPLOY_ROLE_ARN, fornecida pelo módulo ci da infraestrutura.
A AWS entrega credenciais temporárias por GitHub OIDC e exige este repositório na
branch main. A imagem é ativada no próximo início do nó de processamento. Antes de
uma ativação manual, garantir zero runs ativos e espelhamento da Medallion no S3.
O backfill histórico e seus requisitos de cobertura estão em
[docs/FIRMS_PIPELINE.md](docs/FIRMS_PIPELINE.md).
