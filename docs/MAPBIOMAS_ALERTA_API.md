# Aquisição incremental do MapBiomas Alerta

DAG_MAPBIOMAS_ALERTA consulta a API GraphQL v2 por data de publicação.
A primeira aquisição começa em 2019-01-01. As seguintes retomam a última
max_published_at, inclusive, e mesclam os alertas por alertCode com o snapshot
completo anterior. Os territórios configurados abrangem SC, PR e RS; o produto
espacial é filtrado para SC e relacionado a UC, ZA e Buffer de Abrangência.

Uma operação cadastral com manifest_key ou um replay com
reprocess_published_snapshot=true reutiliza a Bronze, sem chamar a API.
Uma consulta periódica sem essas opções busca novas publicações e republicações.

## Reconsultar um intervalo recente

```json
{
  "acquisition_start_date": "2026-09-24",
  "acquisition_end_date": "2026-10-03"
}
```

O início pode recuar para cobrir a sobreposição pedida. Se a última publicação
armazenada for mais antiga que o início solicitado, a consulta começa nesse
checkpoint para preservar a continuidade. O fim é limitado ao máximo de
publicação informado pela API. Um intervalo parcial requer um snapshot
histórico completo já existente; ele não substitui a primeira carga integral.

A DAG preserva o histórico, incorpora registros novos ou alterados e compara o
checksum do conteúdo agregado. Se o conteúdo não mudar, não cria outra cópia
na Bronze nem repete o cruzamento espacial.

Toda aquisição concluída registra em
quality/mapbiomas_alerta/acquisition/<run_id>/summary.json o intervalo real,
a data máxima disponível, o total anterior, os registros retornados, os novos
ou alterados, o total mesclado e o checksum. Esse relatório também é produzido
quando a fonte não muda. Não contém senha nem token de autenticação.