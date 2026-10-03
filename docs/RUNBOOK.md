# Runbook de operação

Guia para agir sobre um e-mail de falha do pipeline. Cada e-mail traz a DAG, a tarefa, a
execução, o erro, o contexto da carga e os links do log no CloudWatch e dos relatórios de
qualidade no S3.

## 1. Investigar

1. Abra o link **Log da tarefa (CloudWatch)** do e-mail. O log fica disponível mesmo com o nó de
   processamento desligado.
2. Abra os **relatórios de qualidade** (`quality/` no bucket do lake) da execução: contagens de
   registros aceitos e rejeitados e o motivo de cada rejeição.
3. Se a falha veio de uma importação da API (`import_id` no e-mail), o mesmo erro aparece para o
   usuário em `GET /api/v1/imports/{import_id}`.

## 2. Acessar o nó de processamento

O nó liga sozinho nas janelas agendadas e quando a API publica uma importação; fora disso, está
desligado. Para ligá-lo e acessar a interface do Airflow sem expor portas:

```bash
aws ec2 start-instances --instance-ids <id-do-no-de-processamento>
aws ssm start-session --target <id-do-no-de-processamento> \
  --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
# Airflow em http://localhost:8080 (usuário admin; senha em /pa-sc/prod/airflow/admin_password)
```

## 3. Reprocessar

Todas as cargas são idempotentes: repetir uma execução não duplica dados.

| Situação | Ação |
|---|---|
| Falha transitória (rede, API externa fora do ar) | na interface do Airflow, *Clear* na tarefa que falhou |
| Importação da API falhou por dado inválido | corrigir o arquivo e enviar uma nova importação pela API |
| FIRMS: janela de backfill falhou | disparar `DAG_FIRMS_BACKFILL` de novo; só as janelas pendentes ou com falha são reprocessadas |
| FIRMS semanal falhou | disparar `DAG_FIRMS` com `{"end_date": "AAAA-MM-DD"}` do último dia da semana perdida |
| MapBiomas Alerta falhou | disparar `DAG_MAPBIOMAS_ALERTA`; com `{"reprocess_published_snapshot": true}` reconstrói a partir da última fotografia, sem chamar a API externa |
| Área nova sem focos FIRMS históricos | disparar `DAG_FIRMS_RECROSS` |
| GeoServer desatualizado | disparar `DAG_GEOSERVER_SYNC` |

## 4. Depois de corrigir

O nó de processamento desliga sozinho após 20 minutos sem execuções. Confirme no console do EC2
que ele parou; um alerta é enviado se ficar ligado por mais de 6 horas.
