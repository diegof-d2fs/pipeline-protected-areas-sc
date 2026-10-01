#!/usr/bin/env bash
set -euo pipefail

COMMAND="${1:-setup}"
DAG_ID="${2:-}"
PERIODO="${3:-}"
SERVICE="${4:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yaml"
ENV_FILE="${SCRIPT_DIR}/.env"
ENV_EXAMPLE_FILE="${SCRIPT_DIR}/.env.example"

compose() {
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" "$@"
}

ensure_env_file() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    cp "${ENV_EXAMPLE_FILE}" "${ENV_FILE}"
    echo "[setup] .env criado a partir de .env.example"
  fi
}

update_project_files() {
  local readme_path="${REPO_ROOT}/README.md"
  local gitignore_path="${REPO_ROOT}/.gitignore"

  if [[ ! -f "${readme_path}" ]]; then
    echo "# pipeline-protected-areas-sc" > "${readme_path}"
  fi

  if ! grep -q "## Setup automatizado" "${readme_path}"; then
    cat >> "${readme_path}" <<'EOF'

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
EOF
    echo "[update_project_files] README.md atualizado"
  fi

  if [[ ! -f "${gitignore_path}" ]]; then
    touch "${gitignore_path}"
  fi

  if ! grep -q "# Airflow runtime artifacts" "${gitignore_path}"; then
    cat >> "${gitignore_path}" <<'EOF'

# Airflow runtime artifacts
airflow/logs/
airflow/data/tmp/
EOF
    echo "[update_project_files] .gitignore atualizado"
  fi
}

validate_env() {
  ensure_env_file

  local required=(
    AIRFLOW_IMAGE_NAME
    AIRFLOW_UID
    PROJECT_DB_HOST
    PROJECT_DB_PORT
    PROJECT_DB_NAME
    PROJECT_DB_USER
    PROJECT_DB_PASSWORD
  )

  local missing=()
  for key in "${required[@]}"; do
    if ! grep -qE "^${key}=" "${ENV_FILE}"; then
      missing+=("${key}")
    fi
  done

  if (( ${#missing[@]} > 0 )); then
    echo "Variaveis ausentes no .env: ${missing[*]}" >&2
    exit 1
  fi

  echo "[validate_env] OK"
}

seed_bronze_local() {
  local bronze_dir="${SCRIPT_DIR}/data/bronze"
  mkdir -p "${bronze_dir}"

  local seed_file="${bronze_dir}/README_BRONZE.md"
  if [[ ! -f "${seed_file}" ]]; then
    cat > "${seed_file}" <<'EOF'
# Bronze seed local

Coloque os dados brutos aqui (ex.: shapefiles, geojson, csv).
Estrutura canonica sugerida (dominio/timestamp):

- ucs/AAAA-MM-DD-HH-mm-ss/
- za/AAAA-MM-DD-HH-mm-ss/
- prodes/AAAA-MM-DD-HH-mm-ss/
- mapbiomas/AAAA-MM-DD-HH-mm-ss/
- mapbiomas_alerta/AAAA-MM-DD-HH-mm-ss/
- firms/AAAA-MM-DD-HH-mm-ss/

Exemplo:
- data/bronze/ucs/2026-04-19-14-40-35/
EOF
  fi

  echo "[seed_bronze_local] Estrutura BRONZE pronta em ${bronze_dir}"
}

ensure_env_file

case "${COMMAND}" in
  setup)
    validate_env
    update_project_files
    seed_bronze_local
    compose up -d protected-areas-sc-airflow-init
    compose up -d
    echo "[setup] Stack inicializada"
    ;;
  start)
    compose up -d
    ;;
  stop)
    compose down
    ;;
  restart)
    compose down
    compose up -d
    ;;
  status)
    compose ps
    ;;
  logs)
    if [[ -z "${SERVICE}" ]]; then
      compose logs --tail 200
    else
      compose logs --tail 200 -f "${SERVICE}"
    fi
    ;;
  init_db)
    compose exec protected-areas-sc-db-main psql -U "${PROJECT_DB_USER:-project}" -d "${PROJECT_DB_NAME:-protected-areas-sc-db-main}" -f /docker-entrypoint-initdb.d/10_init_db.sql
    ;;
  validate_env)
    validate_env
    ;;
  seed_bronze_local)
    seed_bronze_local
    ;;
  reprocess)
    if [[ -z "${DAG_ID}" || -z "${PERIODO}" ]]; then
      echo "Uso: ./setup.sh reprocess <dag_id> <periodo>" >&2
      exit 1
    fi
    compose exec protected-areas-sc-airflow-webserver airflow dags trigger "${DAG_ID}" --conf "{\"periodo\":\"${PERIODO}\"}"
    ;;
  update_project_files)
    update_project_files
    ;;
  *)
    echo "Comando invalido: ${COMMAND}" >&2
    exit 1
    ;;
esac
