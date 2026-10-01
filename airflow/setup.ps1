param(
    [Parameter(Position=0)]
    [ValidateSet("setup", "start", "stop", "restart", "status", "logs", "init_db", "validate_env", "seed_bronze_local", "reprocess", "update_project_files")]
    [string]$Command = "setup",

    [Parameter(Position=1)]
    [string]$DagId,

    [Parameter(Position=2)]
    [string]$Periodo,

    [string]$Service = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$ComposeFile = Join-Path $ScriptDir "docker-compose.yaml"
$EnvFile = Join-Path $ScriptDir ".env"
$EnvExampleFile = Join-Path $ScriptDir ".env.example"

function Invoke-Compose {
    param([Parameter(Mandatory=$true)][string[]]$Args)
    docker compose -f $ComposeFile --env-file $EnvFile @Args
}

function Ensure-EnvFile {
    if (-not (Test-Path $EnvFile)) {
        Copy-Item -Path $EnvExampleFile -Destination $EnvFile
        Write-Host "[setup] .env criado a partir de .env.example"
    }
}

function Update-ProjectFiles {
    $readmePath = Join-Path $RepoRoot "README.md"
    $gitignorePath = Join-Path $RepoRoot ".gitignore"

    $readmeBlock = @"

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
"@

    if (-not (Test-Path $readmePath)) {
        Set-Content -Path $readmePath -Value "# pipeline-protected-areas-sc`n" -Encoding UTF8
    }

    $readmeContent = Get-Content -Path $readmePath -Raw
    if ($readmeContent -notmatch "## Setup automatizado") {
        Add-Content -Path $readmePath -Value $readmeBlock
        Write-Host "[update_project_files] README.md atualizado"
    }

    if (-not (Test-Path $gitignorePath)) {
        Set-Content -Path $gitignorePath -Value "" -Encoding UTF8
    }

    $gitignoreContent = Get-Content -Path $gitignorePath -Raw
    if ($gitignoreContent -notmatch "# Airflow runtime artifacts") {
        Add-Content -Path $gitignorePath -Value "`n# Airflow runtime artifacts`nairflow/logs/`nairflow/data/tmp/`n"
        Write-Host "[update_project_files] .gitignore atualizado"
    }
}

function Validate-Env {
    Ensure-EnvFile
    $required = @(
        "AIRFLOW_IMAGE_NAME",
        "AIRFLOW_UID",
        "PROJECT_DB_HOST",
        "PROJECT_DB_PORT",
        "PROJECT_DB_NAME",
        "PROJECT_DB_USER",
        "PROJECT_DB_PASSWORD"
    )

    $content = Get-Content -Path $EnvFile
    $keys = @{}
    foreach ($line in $content) {
        if ($line -match "^\s*#" -or [string]::IsNullOrWhiteSpace($line)) { continue }
        $parts = $line.Split("=", 2)
        if ($parts.Count -eq 2) { $keys[$parts[0].Trim()] = $parts[1].Trim() }
    }

    $missing = @()
    foreach ($name in $required) {
        if (-not $keys.ContainsKey($name)) { $missing += $name }
    }

    if ($missing.Count -gt 0) {
        throw "Variaveis ausentes no .env: $($missing -join ', ')"
    }

    Write-Host "[validate_env] OK"
}

function Seed-BronzeLocal {
    $bronzeDir = Join-Path $ScriptDir "data/bronze"
    if (-not (Test-Path $bronzeDir)) {
        New-Item -Path $bronzeDir -ItemType Directory | Out-Null
    }

    $seedFile = Join-Path $bronzeDir "README_BRONZE.md"
    if (-not (Test-Path $seedFile)) {
        @"
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
"@ | Set-Content -Path $seedFile -Encoding UTF8
    }

    Write-Host "[seed_bronze_local] Estrutura BRONZE pronta em $bronzeDir"
}

function Get-EnvValue {
    param(
        [Parameter(Mandatory=$true)][string]$Key,
        [Parameter(Mandatory=$true)][string]$DefaultValue
    )

    $line = Get-Content -Path $EnvFile | Where-Object { $_ -match "^$Key=" } | Select-Object -First 1
    if (-not $line) {
        return $DefaultValue
    }

    $parts = $line.Split("=", 2)
    if ($parts.Count -ne 2 -or [string]::IsNullOrWhiteSpace($parts[1])) {
        return $DefaultValue
    }

    return $parts[1].Trim()
}

Ensure-EnvFile

switch ($Command) {
    "setup" {
        Validate-Env
        Update-ProjectFiles
        Seed-BronzeLocal
        Invoke-Compose -Args @("up", "-d", "protected-areas-sc-airflow-init")
        Invoke-Compose -Args @("up", "-d")
        Write-Host "[setup] Stack inicializada"
    }
    "start" {
        Invoke-Compose -Args @("up", "-d")
    }
    "stop" {
        Invoke-Compose -Args @("down")
    }
    "restart" {
        Invoke-Compose -Args @("down")
        Invoke-Compose -Args @("up", "-d")
    }
    "status" {
        Invoke-Compose -Args @("ps")
    }
    "logs" {
        if ([string]::IsNullOrWhiteSpace($Service)) {
            Invoke-Compose -Args @("logs", "--tail", "200")
        } else {
            Invoke-Compose -Args @("logs", "--tail", "200", "-f", $Service)
        }
    }
    "init_db" {
        $dbUser = Get-EnvValue -Key "PROJECT_DB_USER" -DefaultValue "project"
        $dbName = Get-EnvValue -Key "PROJECT_DB_NAME" -DefaultValue "protected-areas-sc-db-main"
        Invoke-Compose -Args @("exec", "protected-areas-sc-db-main", "psql", "-U", $dbUser, "-d", $dbName, "-f", "/docker-entrypoint-initdb.d/10_init_db.sql")
    }
    "validate_env" {
        Validate-Env
    }
    "seed_bronze_local" {
        Seed-BronzeLocal
    }
    "reprocess" {
        if ([string]::IsNullOrWhiteSpace($DagId) -or [string]::IsNullOrWhiteSpace($Periodo)) {
            throw "Uso: ./setup.ps1 reprocess <dag_id> <periodo>"
        }
        $confJson = ('{"periodo":"{0}"}' -f $Periodo)
        Invoke-Compose -Args @(
            "exec", "protected-areas-sc-airflow-webserver", "airflow", "dags", "trigger", $DagId,
            "--conf", $confJson
        )
    }
    "update_project_files" {
        Update-ProjectFiles
    }
}
