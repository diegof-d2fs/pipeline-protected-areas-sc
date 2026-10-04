# Engenharia e operação — FIRMS

## Evidências oficiais

- **CONFIRMADO:** a [FIRMS Area API](https://firms.modaps.eosdis.nasa.gov/api/area/) entrega CSV de hotspots por fonte, bbox, janela de 1–5 dias e data opcional; exige `MAP_KEY` e limita o uso a 5.000 transações por 10 minutos.
- **CONFIRMADO:** a chamada da Area API é síncrona. Não há contrato de submit/job/polling nessa API.
- **CONFIRMADO:** estão disponíveis `VIIRS_NOAA20_NRT` e `VIIRS_NOAA21_NRT`; a página da API também anuncia o encerramento do produto Suomi‑NPP em 1º/11/2026.
- **CONFIRMADO:** VIIRS representa confiança como `l`, `n`, `h`; a [descrição oficial dos hotspots VIIRS](https://firms.modaps.eosdis.nasa.gov/content/descriptions/FIRMS_VIIRS_Firehotspots.html) explica que `h` está associado a pixels saturados e `n` a anomalia forte sem contaminação potencial por brilho solar.
- **CONFIRMADO:** MODIS representa confiança entre 0 e 100, mas a NASA alerta que o corte ideal depende da aplicação; ele não é probabilidade estatística universal.
- **CONFIRMADO:** a Area API aceita bbox ou `world`, não aceita polígono arbitrário. Para SC será usada a bbox IBGE `-53.8371493,-29.3550659,-48.3278753,-25.9557228`, seguida de filtro exato pela geometria estadual na Silver.
- **CONFIRMADO:** dados VIIRS globais normalmente ficam disponíveis em até três horas. A API aceita apenas janelas em dias, portanto execuções intradiárias necessariamente repetem parte dos dados e dependem de idempotência.

## Evidência das amostras locais

Foram inventariados os oito downloads oficiais em `F:\Univali\IA_2`, incluindo os ZIPs originais e os Shapefiles extraídos. Apesar de destinados ao estudo de SC, os datasets possuem bbox do Brasil e milhões de registros; não são recortes estaduais prontos.

| Código | Produto identificado | Cobertura encontrada | Registros principais |
|---|---|---|---:|
| `J1V-C2` | VIIRS NOAA-20 Collection 2 | 2018–2019 e 2020–12/04/2025 | 10.026.167 |
| `J2V-C2` | VIIRS NOAA-21 Collection 2 NRT | 17/01/2024–12/04/2025 | 1.986.298 |
| `SV-C2` | VIIRS Suomi-NPP Collection 2 | 20/01/2012–11/04/2025 | 18.416.666 |
| `M-C61` | MODIS Aqua/Terra Collection 6.1 | 2004–12/04/2025 | 8.528.212, além do derivado local `conf_75_br` |

No intervalo comum de 01/02/2025 a 12/04/2025, filtrado pelo polígono real de SC:

| Produto | Detecções SC | Confiança |
|---|---:|---|
| NOAA-20 VIIRS | 775 | `l=54`, `n=702`, `h=19` |
| NOAA-21 VIIRS | 756 | `l=40`, `n=664`, `h=52` |
| Suomi-NPP VIIRS | 771 | `l=44`, `n=674`, `h=53` |
| MODIS | 195 | 90 com confiança `>=70`; 44 na classe oficial alta `>=80` |

As amostras confirmam que VIIRS oferece muito mais detecções em SC que MODIS no mesmo período. Também demonstram que filtrar VIIRS apenas por `h` descartaria a maioria das anomalias nominais tecnicamente válidas. O campo `type` aparece em arquivos Standard Processing, mas não nos NRT observados; o parser deve tratá-lo como opcional e versionado.

## Produtos e política de publicação aprovados

Usar semanalmente duas fontes NRT de 375 m, `VIIRS_NOAA20_NRT` e `VIIRS_NOAA21_NRT`, consultadas separadamente e consolidadas com o produto original preservado. Não iniciar uma dependência nova de SNPP perto da descontinuação. MODIS fica fora da operação diária, mas entra no backfill como série histórica identificada.

VIIRS usa `l`, `n` e `h` na Silver após o recorte exato de SC; Gold e PostGIS recebem somente `h`. MODIS usa a escala original 0–100: valores de 35 a 70, inclusive, ficam na Silver; valores maiores que 70 seguem para Gold e PostGIS; valores menores que 35 ficam somente na Bronze. Não converter categorias VIIRS em percentuais MODIS.

## Pré-requisitos e aprendizados da Frente 0

FIRMS consome as AOIs vigentes e aplica a partição exclusiva `UC`, `ZA − UC` e `Buffer de Abrangência − UC`. Como a detecção é pontual, cada ponto produz no máximo uma categoria por UC, com precedência da própria UC sobre sua zona; o mesmo ponto pode relacionar-se a UCs diferentes. O service cria diretórios temporários idempotentemente, preserva o payload Bronze nativo e valida no PostGIS que a geometria publicada pertence à AOI correspondente.

## Histórico 2015–presente e operação incremental

O produto deve conter histórico desde 01/01/2015 e continuar incrementalmente. São fluxos distintos:

1. **Backfill único:** preferir Standard Processing (`*_SP`) requisitado pela API em blocos de até cinco dias e bbox de SC, pois incorpora reprocessamentos/correções vigentes e produz payloads muito menores. Consultar primeiro `data_availability` para cada produto.
2. **Fallback histórico:** quando a API não disponibilizar produto/período, ler os ZIPs locais em streaming, filtrar exatamente por SC e publicar a proveniência do download de 12/04/2025. Não copiar os Shapefiles expandidos se o ZIP original já estiver preservado.
3. **Incremental:** adquirir NRT para NOAA-20 e NOAA-21, mantendo sobreposição curta e idempotência; substituir/reconciliar NRT com Standard Processing quando este se tornar disponível.
4. **Séries científicas:** nunca somar ingenuamente sensores como se a capacidade de observação fosse constante. Preservar produto/satélite e publicar métricas por sensor, além de indicar quantos sensores estavam ativos em cada período.

Suomi-NPP é relevante para a continuidade histórica a partir de 2015, mas não deve ser dependência operacional futura: há anomalia oficial após 09/03/2026 e encerramento de entrega anunciado para 01/11/2026. MODIS é valioso para continuidade histórica e comparação de longo prazo; não substitui a camada operacional VIIRS de 375 m.

### Orquestração do backfill

O histórico não será obtido por script manual fora do produto. Uma `DAG_FIRMS_BACKFILL`, sem schedule automático e acionada explicitamente, cria janelas atômicas de no máximo cinco dias por produto. O estado de cada janela (`pending`, `running`, `published`, `failed`, tentativas, checksum e run_id) fica em tabela de controle no PostGIS; a DAG processa somente um lote limitado de janelas pendentes por execução. Assim o backfill pode ser pausado, retomado, reexecutado sem duplicação e auditado sem gerar milhares de tasks em um único DAG run.

Produtos iniciais do histórico: `VIIRS_SNPP_SP` a partir de 01/01/2015, `VIIRS_NOAA20_SP` a partir de sua disponibilidade e `MODIS_SP` como série complementar. A disponibilidade efetiva por produto/data será consultada e registrada antes de cada lote; `VIIRS_NOAA21_NRT` permanece fonte operacional diária e só entra no histórico quando a API confirmar cobertura recuperável para o período solicitado.

A `DAG_FIRMS` semanal é independente: consulta NOAA‑20 e NOAA‑21 NRT para a janela incremental, com sobreposição e reconciliação futura NRT→SP. Ambas reutilizam o mesmo client, contratos Bronze/Silver/Gold, validações e regras de publicação.

### Teste ponta a ponta da API

Em 30/08/2026 foi executado um smoke test isolado da DAG, sem persistir chave, URL autenticada ou payload. A autenticação, a bbox e os produtos foram aceitos pela API:

| Produto/data | HTTP | Registros na bbox | Registros após polígono exato de SC |
|---|---:|---:|---:|
| `VIIRS_NOAA20_NRT`, último dia | 200 | 0 | 0 |
| `VIIRS_NOAA21_NRT`, último dia | 200 | 0 | 0 |
| `VIIRS_NOAA20_SP`, 01/08/2020 | 200 | 154 | 55 |
| `VIIRS_SNPP_SP`, 01/08/2015 | 200 | 141 | 56 |
| `MODIS_SP`, 01/08/2015 | 200 | 13 | 5 |

Nos NRT, a resposta continha o cabeçalho CSV válido e nenhuma detecção; isso comprova resposta vazia legítima, não falha de autenticação. Os testes SP comprovam que o backfill histórico filtrado por bbox é tecnicamente viável. A chave usada no teste foi exposta no canal de conversa e deve ser revogada; a implementação deve receber uma nova chave exclusivamente pelo mecanismo de secrets.

Também foi persistido um artefato QGIS reproduzível para `VIIRS_NOAA20_SP`, janela 01–05/08/2020: 1.563 registros no retângulo e 647 após o filtro do polígono de SC. Os arquivos de teste estão em `airflow/data/bronze/firms_smoke/product=VIIRS_NOAA20_SP/window=2020-08-01_2020-08-05/response.csv` e `airflow/data/gold/firms_smoke/product=VIIRS_NOAA20_SP/window=2020-08-01_2020-08-05/sc_detections.geojson`. O GeoJSON usa pontos WGS84 e abre diretamente no QGIS; os dois diretórios possuem `manifest.json` com checksum e contagens.

Esse request da Area API não aparece em `download/list.php`: a [documentação da API](https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html) descreve chamadas síncronas de aplicação usando MAP_KEY, enquanto a lista do perfil registra pedidos assíncronos do formulário de download. Para criar uma linha nessa lista é necessário abrir o portal autenticado e enviar o formulário “Create New Request”; não há autorização para fazê-lo apenas com MAP_KEY.

O fluxo automatizado foi consolidado com `FIRMS_MAP_KEY` provisionada fora do Git:

1. a Area API é consultada por produto e janela usando a bbox de SC;
2. CSV e manifesto saneado são preservados imutavelmente na Bronze;
3. o polígono estadual exato, schema, coordenadas, tempo e confiança são validados na Silver;
4. apenas detecções publicáveis relacionadas a UC/ZA/Buffer de Abrangência seguem para Gold/PostGIS;
5. retry, resposta vazia, 4xx, 429/5xx, retomada e idempotência possuem testes;
6. quota e `data_availability` permanecem verificações operacionais antes do backfill integral.

## Topologia implementada

```text
start
  -> resolve_windows / plan_windows
  -> process_window.expand(produto + janela)
       -> Bronze -> Silver -> relações Gold -> PostGIS -> DQ
  -> summarize
  -> finish
```

A aquisição é HTTP síncrona com timeout e retries. Logo, os itens “submit/polling” do plano são **não aplicáveis** à Area API. Se uma fonte FIRMS diferente for adotada no futuro, uma nova decisão deverá justificar sensor/operador assíncrono.

## Aquisição, autenticação e resiliência

1. Bbox deve ser configuração explícita e incluir uma margem apenas se aprovada; o filtro final usa SC/AOIs reais.
2. `FIRMS_MAP_KEY` deve vir de Airflow Connection/secret/env e ser mascarada em logs e URLs.
3. Um request por fonte e janela de no máximo cinco dias.
4. Timeouts de conexão/leitura; retry exponencial com jitter para 429, 500, 502, 503 e 504; respeitar `Retry-After` quando presente.
5. Falhas 4xx de contrato/autenticação não devem ser repetidas indiscriminadamente.
6. Validar `Content-Type`, cabeçalho CSV e tamanho máximo antes de aceitar.
7. Registrar contagem de transações disponível quando a API fornecer, sem expor chave.

## Contratos Medallion

### Bronze

Uma resposta por produto/data de consulta, imutável:

```text
bronze/firms/product=VIIRS_NOAA20_NRT/start_date=YYYY-MM-DD/end_date=YYYY-MM-DD/run_id=<run>/
  response.csv
  manifest.json
```

Manifesto: schema version, produto, bbox, janela, requested/received UTC, status HTTP, checksum, bytes, record count e URL saneada. O CSV não é filtrado nem reescrito.

### Silver

Parquet ou GPKG normalizado, com `detection_id`, `source_product`, `source_version`, `satellite`, `instrument`, `acquired_at_utc`, `latitude`, `longitude`, `confidence_raw`, `confidence_scheme`, `confidence_score`, `confidence_class`, `publication_rule_version`, `frp_mw`, `brightness_*`, `scan_km`, `track_km`, `daynight`, `detection_type`, `geom` EPSG:4674, `source_checksum` e `run_id`.

VIIRS preserva `confidence_raw` em `l/n/h`, com `confidence_score` nulo. MODIS preserva `confidence_raw` e `confidence_score` numéricos. A Silver contém todos os VIIRS de SC e apenas MODIS com confiança maior ou igual a 35.

Chave determinística proposta: SHA‑256 de produto + versão + satélite + data/hora UTC + latitude/longitude normalizadas. O dado original permanece disponível para auditar colisões.

### Gold e PostGIS

Uma linha por detecção × AOI relacionada, preservando `id_uc`, exatamente uma zona ativa quando aplicável (`id_za_oficial` ou `id_buffer_abrangencia`), `tipo_cruzamento`, área de referência e atributos FIRMS. A Gold recebe VIIRS `h` e MODIS `>70`.

O baseline reconstrói `firms_clip` com: `detection_id`, `source_product`, `source_version`, `source_processing`, `acquired_at_utc`, satélite/instrumento, confiança bruta/esquema/escore/classe/regra, medidas ópticas e FRP, `daynight`, tipo inferido, geometria, checksum, `run_id` e timestamps. Uma chave única funcional por detecção e relação AOI impede replay duplicado.

O baseline também cria `firms_backfill_window` para controlar produto, início/fim, estado, tentativas, manifesto/checksum, erro saneado e runs responsáveis. Essa tabela não contém CSV nem segredo.

## Relação espacial

- Detecção dentro da UC: associação `UC`.
- Detecção fora da UC e dentro da zona ativa: associação `ZA` ou `BUFFER_ABRANGENCIA`.
- Se o ponto estiver simultaneamente na UC e na zona, a categoria analítica preferencial é UC, evitando dupla contagem no agregado; a relação espacial bruta pode ser preservada separadamente se houver necessidade.
- Uma detecção pode relacionar-se a UCs diferentes; isso não é duplicidade.
- A consulta deve usar apenas ZA ou Buffer de Abrangência ativos e respeitar a exclusividade do banco.

## Frequência e janelas

A DAG executa semanalmente (segunda, 06:00 UTC). Cada run cobre os sete dias completos do seu `data_interval` (até o dia anterior a `data_interval_end`) mais uma sobreposição configurável, dividida em janelas de até cinco dias — limite da Area API — por produto; detecções repetidas pela sobreposição são deduplicadas. `FIRMS_INCREMENTAL_PERIOD_DAYS` ajusta o período se a agenda mudar. Quando só uma fonte operacional concluir, o resultado é publicado com qualidade `DEGRADED` e alerta; se ambas falharem, a DAG falha. Backfill informa datas explícitas e fragmenta períodos em blocos de até cinco dias.

## Data quality e observabilidade

- respostas/linhas por produto, aceitas por confiança, rejeitadas por motivo, duplicadas e relacionadas por tipo;
- data máxima/mínima, atraso entre aquisição e ingestão, checksum e versão;
- schema inesperado, coordenada inválida, timestamp inválido, confiança desconhecida e FRP inválido;
- ausência de detecções é sucesso vazio, não falha;
- registrar `DEGRADED` quando uma fonte falhar e falhar a DAG quando ambas falharem.

## Testes

- unitários: parser, horários UTC, confiança `l/n/h`, chave natural, spatial precedence e janela;
- cliente: 200 vazio/com dados, CSV inválido, 401/403, 429 + `Retry-After`, 5xx e timeout;
- integração: fixtures pequenas para NOAA‑20/21, replay e PostGIS com UC/ZA/Buffer de Abrangência;
- DAG: import, grafo, schedule, retries e ausência de rede no parse;
- DQ: contagens e rejeições determinísticas.

## Arquivos implementados

- `airflow/dags/dag_firms_recross.py`: recruzamento do histórico publicado com áreas cadastrais novas ou alteradas, disparado na cadeia cadastral, sem chamada à API;
- `airflow/dags/dag_firms.py`: orquestração semanal fina e mapeada por janela e produto;
- `airflow/dags/dag_firms_backfill.py`: paginação histórica retomável;
- `airflow/dags/scripts_python/firms_pipeline.py`: service de domínio compartilhado;
- `airflow/dags/scripts_python/firms_client.py`: única borda HTTP testável;
- `airflow/dags/scripts_python/config.py`, `.env.example` e Compose: opções sem segredo;
- `init_db.sql`: `firms_clip`, controle de backfill, índices e constraints;
- `airflow/tests/test_firms_pipeline.py`: contratos de client, normalização, espaço, resumo e DAGs.

## Evidência de implementação — 13/09/2026

- `DAG_FIRMS_BACKFILL`, `VIIRS_NOAA20_SP`, 01–05/08/2020: 1.563 linhas no bbox, 647 na Silver e uma relação UC de alta confiança no Gold/PostGIS.
- A falha deliberadamente observada entre Silver e Gold foi retomada com o checksum Bronze original; a tentativa 2 publicou sem segunda aquisição.
- Um novo replay da mesma janela terminou sem criar task mapeada e sem duplicar `firms_clip`.
- `DAG_FIRMS` executou NOAA‑20 e NOAA‑21 NRT; os dois CSVs vazios válidos foram publicados como sucesso.
- Suíte completa: 39 testes aprovados, 6 pulados, zero erro de importação e `pip check` íntegro.
- A leitura de UC/ZA/Buffer de Abrangência usa transação `REPEATABLE READ` somente leitura e bloqueia a publicação se qualquer UC ativa não possuir exatamente uma zona ativa.

O backfill de 2015 ao presente ainda precisa ser executado operacionalmente em páginas. A reconciliação automática NRT→SP e a reassociação cadastral somente a partir da Silver permanecem incrementos posteriores.

## Evidência de implementação — 13/09/2026 (backfill 2020 completo)

O ano-calendário 2020 foi processado em 3 páginas de `DAG_FIRMS_BACKFILL` (300 janelas de até
5 dias, produtos `VIIRS_NOAA20_SP`, `VIIRS_SNPP_SP`, `MODIS_SP`), todas concluídas com sucesso e
sem retrabalho de Bronze nas janelas retomadas. Dois defeitos foram encontrados e corrigidos
durante a execução real (não no smoke test curto): uma janela MODIS que ficava totalmente vazia
após o filtro de confiança quebrava `_normalize`, e `id_za_oficial`/`id_buffer_abrangencia` chegavam como
`NaN` do GeoPandas em vez de `None`, quebrando o cast para `int` na carga do Gold. Ambos têm
teste de regressão em `test_firms_pipeline.py` (suíte com 10 testes, todos verdes no container).

Resultado publicado em `firms_clip` para 2020: 81 relações, sem duplicidade
(`detection_id`+`id_uc`+`tipo_cruzamento`+`id_za_oficial`+`id_buffer_abrangencia`) e sem geometria fora da AOI
correspondente — 32 `UC` (MODIS 17, NOAA‑20 5, SNPP 10), 1 `ZA` (MODIS) e 48 `BUFFER_ABRANGENCIA`
(MODIS 25, NOAA‑20 9, SNPP 14).

Essas 81 relações foram reconciliadas ponto a ponto contra o cruzamento manual antigo
(`cruzamentos_uc_sc_FIRMS_*.shp`, extraído em abril/2025) para as mesmas 15 UCs ativas,
recortando ambos os lados pelo mesmo `limites_SC.geojson` e pela mesma regra de publicação
(VIIRS somente `h`, MODIS somente confiança `>70`):

- `VIIRS_NOAA20_SP` e `VIIRS_SNPP_SP`: contagem `UC` bate exatamente (5 e 10) com o manual.
- `MODIS_SP`: o banco tem 17 contra 12 do manual; os 5 registros a mais (Baleia Franca ×1,
  São Joaquim ×4, confiança 71–74) simplesmente não existem no extrato manual estático — efeito
  esperado de reprocessamento do Standard Processing da NASA ao longo do tempo, não um erro de
  contagem.
- A diferença observada em `PARQUE NACIONAL DE APARADOS DA SERRA` (17604) e `PARQUE NACIONAL DA
  SERRA GERAL` (20832) é geometria, não bug: essas UCs se estendem para o RS, o cruzamento
  manual conta pontos fisicamente no RS e o pipeline os exclui corretamente pelo recorte exato
  do polígono estadual de SC (comportamento documentado acima, não um relaxamento do filtro).

Não restou diferença não explicada entre o backfill 2020 e a base manual de referência.

## Integração API e S3

FIRMS continua fonte interna do Airflow; a FastAPI não recebe seus arquivos. Uma mudança cadastral não deve disparar automaticamente nova aquisição. Reprocessamento deve reutilizar Silver por janela e recalcular apenas relações. As chaves relativas e o contrato de storage devem permitir trocar o filesystem por S3 sem mudar regras de domínio.

### Replay do FIRMS a partir da Bronze

O limite IBGE de Santa Catarina é resolvido preferencialmente na partição
`bronze/boundaries/source=ibge/year=2025/area=sc/`. O leitor exige o manifesto do domínio
`ibge_sc_boundary`, a entrada `limites_SC.geojson`, o SHA-256 e o tamanho declarados.
A pasta `raw/ibge` só é consultada quando ainda não existe esse pacote na Bronze.
Um pacote Bronze incompleto ou alterado interrompe a publicação; a fonte raw não mascara
falhas de integridade. Isso permite executar o backfill em uma máquina nova sincronizada
com o S3, sem depender da landing local do primeiro carregamento.
### Execução em lotes e auditoria histórica

Cada página seleciona até 100 janelas e as distribui em até quatro tarefas do Airflow,
com até 25 janelas por tarefa. Cada janela mantém sua transação, estado retomável,
partições Medallion e validação de checksum. Uma falha não impede as demais janelas
do lote de serem processadas; o resumo da página falha se qualquer janela falhar.
Runs antigos já expandidos continuam compatíveis com a janela individual.

A geometria do limite estadual é reutilizada dentro da tarefa, mas os bytes e o
manifesto são verificados a cada janela. O predicado covers usa geometria preparada
sem alterar a regra de inclusão de pontos na borda de SC.

Os relatórios de qualidade incluem produto, início e fim da janela no caminho.
Assim, várias janelas do mesmo produto e run não sobrescrevem suas estatísticas.
A aquisição histórica deve respeitar a disponibilidade real de cada produto:
SP e NRT são fontes identificadas separadamente. A cobertura NRT completa o período
recente ainda ausente em SP; uma resposta vazia fora da disponibilidade SP não
comprova ausência de focos naquele período.
Disponibilidade oficial: https://firms.modaps.eosdis.nasa.gov/api/data_availability/
A descoberta com safe mode desativado usa airflow/dags/.airflowignore para excluir
scripts_python da varredura. Os módulos continuam importáveis e as mesmas 12 DAGs são
descobertas; o scheduler deixa de analisar 18 bibliotecas como possíveis DAGs.
Referência: https://airflow.apache.org/docs/apache-airflow/2.10.3/core-concepts/dags.html#airflowignore

O snapshot cadastral é consultado de novo a cada janela, em transação somente leitura
com REPEATABLE READ. A conversão dos polígonos, o recorte das zonas e a preparação espacial
são reutilizados apenas quando IDs e SHA-256 das geometrias permanecem iguais. Mudanças
geométricas ou de áreas ativas invalidam o cache, inclusive sem alteração do número de versão.

### Retomada por ano e cota compartilhada

Com `yearly_batches: true`, cada página seleciona apenas o primeiro ano que ainda
possui janelas PENDING ou FAILED. O ano é o da data inicial da janela; uma janela
que atravessa dezembro/janeiro permanece inteira. A grade original de cinco dias,
os manifestos e as janelas PUBLISHED são preservados. O coordenador de replay usa
esse modo para fechar o ano corrente antes de avançar ao próximo.

Em respostas HTTP 400 ou 429 sem Retry-After, o cliente consulta o endpoint oficial
de cota. Somente quando current_transactions >= transaction_limit ele aguarda
610 segundos e tenta novamente, dentro do limite configurado de tentativas.
Erros de contrato continuam interrompendo a janela. A cota é compartilhada pela
MAP_KEY entre as tarefas; a espera é registrada sem divulgar a chave ou a URL
autenticada. Os intervalos publicados continuam disponíveis durante a espera.
Referência: https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html
### Planejamento limitado à disponibilidade do sensor

A DAG_FIRMS_BACKFILL consulta a disponibilidade oficial antes de semear ou selecionar
janelas. Produtos ausentes ou datas inválidas interrompem o planejamento. Novas
execuções criam apenas janelas que intersectam min_date/max_date do produto, mantendo
a grade original de cinco dias para não duplicar partições em uma retomada.

Janelas PENDING/FAILED já semeadas fora desse intervalo passam a
SKIPPED_UNAVAILABLE, com o intervalo oficial em last_error. Não consomem a Area API,
não geram CSV vazio e não executam cruzamento espacial. Janelas PUBLISHED e os
artefatos já existentes ficam preservados. Se uma futura publicação da NASA tornar
uma janela dispensada disponível, o planejamento a reativa como PENDING.

Uma janela disponível pode retornar zero focos na área consultada. A resposta original
é auditada na Bronze; esse caso evita carregar o limite de SC, consultar/preparar
geometrias de UC/ZA/Buffer de Abrangência e inserir relações no banco. Não se presume
que um ano inteiro esteja vazio com base numa única janela.

O schema canônico inclui o novo estado e uma atualização idempotente do CHECK para
bancos existentes. O verificador final distingue publicação de dispensa e continua
exigindo cobertura contínua de todos os intervalos disponíveis.
### Recruzamento após uma retomada

A Silver registra bronze_manifest_key para apontar a aquisição imutável original.
Assim, uma falha entre Bronze e Silver pode ser retomada por outro run sem obrigar
a UC nova a chamar a NASA. Para partições antigas sem esse campo, o recruzamento
localiza na mesma janela/produto o manifesto Bronze com source_checksum correspondente
e valida SHA-256 e tamanho do CSV. Partições Silver sem detecções são ignoradas.
O coordenador de replay consulta a cota compartilhada antes de cada página e espera
quando restar menos de 20% de margem. A verificação é repetida a cada 30 segundos,
com limite de 660 segundos, antes de iniciar outra página. Isso complementa a espera
do cliente após uma rejeição confirmada por cota. Outros HTTP 400 continuam falhando
com o texto da resposta sanitizado, para distinguir contrato, limitação e falha externa.
Não registrar MAP_KEY nem URL autenticada nos diagnósticos.