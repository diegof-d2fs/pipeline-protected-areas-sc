# Registro de decisões arquiteturais

Os status `PROPOSTO` e `PENDENTE` não autorizam implementação. A aprovação do responsável deve ser registrada com data antes da mudança funcional.

## ADR-001 — Produto FIRMS

Status: **APROVADO — 30/08/2026**

### Contexto

O placeholder não fixa um produto. SNPP será descontinuado em 1º/11/2026 e possui alerta de qualidade após 09/03/2026. VIIRS oferece resolução nominal de 375 m contra 1 km do MODIS. As amostras Brasil em `F:\Univali\IA_2`, filtradas para SC no período comum de 01/02–12/04/2025, produziram 775 detecções NOAA-20, 756 NOAA-21, 771 SNPP e 195 MODIS.

### Evidências

A [Area API oficial](https://firms.modaps.eosdis.nasa.gov/api/area/) lista NOAA‑20, NOAA‑21, SNPP e MODIS e publica o aviso de descontinuação.

### Alternativas

MODIS NRT; SNPP; NOAA‑20; NOAA‑21; combinação de produtos.

### Decisão

Operação diária: `VIIRS_NOAA20_NRT` + `VIIRS_NOAA21_NRT`, preservando a origem e deduplicando apenas replays da mesma detecção/produto. Para o histórico desde 2015, usar Standard Processing via API quando disponível, incluindo SNPP e MODIS como séries identificadas separadamente; arquivos locais são fallback/reconciliação. Não misturar contagens de sensores sem informar disponibilidade/cobertura.

### Consequências e impacto no TCC 3

Duas extrações por janela e maior continuidade operacional. O texto do TCC não pode tratar “FIRMS” como sensor único.

## ADR-002 — Aquisição e assincronia FIRMS

Status: **APROVADO — 30/08/2026**

### Contexto e evidências

A FIRMS Area API devolve CSV na própria resposta; não expõe submit/job/polling. Ela aceita bbox, fonte, 1–5 dias e data opcional, exige MAP_KEY e possui quota.

### Decisão

Usar chamada HTTP síncrona com timeout, retry/backoff e idempotência por janela/produto/checksum. A DAG é diária; se somente uma das duas fontes operacionais concluir, publica o resultado disponível com estado de qualidade `DEGRADED` e alerta. A DAG falha somente quando ambas as fontes falham. Submit e polling são “não aplicáveis”. Não criar sensor FIRMS sem fonte assíncrona real.

### Consequências

DAG mais simples, sem ocupar slot esperando job inexistente. 429/5xx precisam de tratamento explícito.

## ADR-003 — Confiança FIRMS

Status: **APROVADO — 30/08/2026**

### Contexto e evidências

VIIRS usa `l/n/h`; MODIS usa 0–100. A NASA afirma que o corte ótimo depende da aplicação e não é uma probabilidade estatística universal.

### Alternativas

VIIRS `h`; VIIRS `n+h`; incluir MODIS `>=70`; manter todos e filtrar apenas no Gold.

### Decisão

VIIRS: Bronze preserva a resposta original; Silver mantém todas as detecções espaciais de SC com `l`, `n` e `h`; Gold e PostGIS recebem exclusivamente `h`.

MODIS: Bronze preserva a resposta original; Silver recebe confiança de `35` a `70`, inclusive; Gold e PostGIS recebem confiança estritamente maior que `70`. Valores menores que `35` permanecem somente na Bronze. A escala 0–100 de MODIS continua distinta da escala categórica VIIRS; a regra é uma política de publicação, não uma conversão entre sensores.

### Impacto no TCC 3

O limiar de 75% do TCC 2 deve ser corrigido ou qualificado por produto. A Gold não representa todas as detecções válidas da Silver: ela representa a política de publicação aprovada.

## ADR-004 — Aquisição MapBiomas

Status: **APROVADO — 30/08/2026**

### Contexto e evidências

O usuário disponibilizou em `E:\mapbiomas11` o pacote oficial da Coleção 11 v1, incluindo o GeoTIFF Brasil 2025, CSV com 33 classes, QMLs e documentação. O raster foi validado como GeoTIFF de banda única `uint8`, EPSG:4326, 154.470 × 146.501 pixels, LZW e SHA-256 `0035BF482E8E6FAB6C623275485D2891ED19713B5F258D194A7BB0764D8392DB`. O limite de SC foi fornecido em `X:\base_teste`, identificado como IBGE 2025 e validado como `MultiPolygon` EPSG:4674. A página oficial confirma Coleção 11, série 1985–2025 e GeoTIFF de banda única.

### Alternativas

Pacote oficial pré-provisionado; Earth Engine batch; download automatizado futuro; scraping não documentado.

### Decisão

Proposta: usar o pacote oficial local como fonte do primeiro incremento. Copiar os dez arquivos-fonte originais para a Bronze gerenciada, sem duplicar os QMLs já contidos no ZIP; publicar também os sete arquivos `limites_SC` em Boundary Bronze; recortar o GeoTIFF Brasil para SC na Silver; publicar COG e estatísticas por UC/ZA/Buffer de Abrangência na Gold/PostGIS. Não implementar GEE, sensor assíncrono ou dependências Google agora.

**Atualização 31/08/2026:** os dados brutos deixaram os drives externos (`E:\mapbiomas11`, `X:\base_teste`) e passaram para a zona `raw` do data lake do projeto — `airflow/data/raw/mapbiomas/` (GeoTIFFs anuais + companheiros da Coleção 11 + `MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx`) e `airflow/data/raw/ibge/` (`limites_SC.*`), ambos dentro de `airflow/data/` (gitignorado). Os anos históricos (1985–2024) são exportados do GEE (`Export.image.toDrive`, `scale: 30`, `crs: EPSG:4326`, COG); o Brasil sai em ~9 peças e só a peça sudeste cobre SC. A Bronze gerenciada continua sendo a cópia canônica com checksum; a zona `raw` é landing descartável.

### Consequências

Remove credenciais e espera externa do MVP, reduz risco e usa exatamente o produto oficial já obtido. Exige limite estadual oficial, manifesto/checksums e ingestão atômica. Novos anos dependem de novos GeoTIFFs, mas reutilizam o mesmo contrato.

## ADR-005 — Cálculo de área MapBiomas

Status: **APROVADO — 30/08/2026**

### Contexto

O raster real está em EPSG:4326 com pixel angular de `0,0002694945852359°`. O produto possui resolução nominal de 30 m, mas o [FAQ oficial do MapBiomas](https://brasil.mapbiomas.org/faq/?tema=conceito) esclarece que a grade original em latitude/longitude não tem área constante e que contar pixels multiplicando por 900 m² não é adequado em escala regional. Em SC, uma célula varia aproximadamente de 0,07814 a 0,08062 ha. Reprojetar o raster categórico também altera a grade e pode alterar contagens.

### Decisão

Preservar a grade EPSG:4326; mascarar cada AOI por centro do pixel (`all_touched=False`); calcular a área elipsoidal da célula por linha com `pyproj.Geod`; somar por classe e converter para hectares. Valor 0 é NoData e não entra na área classificada. A carga definitiva deve reconciliar os totais de SC com a estatística oficial da Coleção 11 e falhar fora da tolerância aprovada; divergência relevante exige comparação com `ee.Image.pixelArea()`.

### Consequências

Resultados reprodutíveis sem reclassificação, com método `geodesic_native_grid_v1` registrado. AOIs pequenos terão diferença de borda mensurada. Não vetorizar classes nem atribuir área a UC pontual.

## ADR-006 — Storage local preparado para S3

Status: **PROPOSTO — aguardando aprovação**

### Decisão

Manter filesystem local no MVP; modelar referências como chaves POSIX relativas, manifestos e checksums. Criar interface somente nas bordas novas que necessitam armazenamento, evitando reescrever services funcionais agora.

### Consequências

Migração futura incremental para S3, sem caminhos Windows no domínio. Não configura S3 nesta etapa.

## ADR-007 — Retenção de task logs Airflow

Status: **PROPOSTO — aguardando aprovação**

### Contexto

O bind mount `airflow/logs` já possuía aproximadamente 1.919 arquivos/299 MB na auditoria e não tem retenção.

### Decisão

30 dias, configurável, limpeza semanal domingo 03:00, dry-run por padrão na primeira implantação. Excluir somente arquivos resolvidos sob `AIRFLOW__LOGGING__BASE_LOG_FOLDER`, nunca diretórios amplos ou logs em uso.

### Consequências

UI perde logs locais antigos; a retenção deve ser revisada quando remote logging for habilitado.

## ADR-008 — Retenção do metadata DB Airflow

Status: **PROPOSTO — aguardando aprovação**

### Contexto e evidências

Airflow 2.8.4 oferece [`airflow db clean`](https://airflow.apache.org/docs/apache-airflow/2.8.4/cli-and-env-variables-ref.html) com timestamp, dry-run, archive/skip-archive e seleção de tabelas.

### Decisão

90 dias, configurável, usando somente o CLI oficial. Primeiro dry-run e backup; execução real com archive padrão inicialmente. Arquivos de archive serão tratados por política explícita posterior.

### Consequências

Histórico antigo sai das tabelas primárias, preservando 90 dias para demonstração e diagnóstico. A DAG não executa SQL manual.

## ADR-009 — Retenção Medallion

Status: **PENDENTE — manter comportamento atual até aprovação**

### Contexto

A DAG atual apaga Silver e Gold após 30 dias, usa `mtime` como fallback e não oferece dry-run. Bronze não é apagada.

### Alternativas

30 dias em ambas; Silver 30/Gold maior; retenção por domínio; somente quota/manual.

### Decisão

Não alterar agora. Proposta: tornar configuração e dry-run obrigatórios e avaliar Gold por valor analítico/reprodutibilidade antes de manter o mesmo prazo de Silver.

### Consequências

Cleanup Medallion permanece responsabilidade separada da manutenção Airflow.

## ADR-010 — Bootstrap do schema

Status: **APROVADO — consolidar antes do primeiro deploy de produção**

### Contexto

O sistema existe apenas em desenvolvimento e nunca foi implantado em produção. O banco de desenvolvimento pode ser apagado e recriado. O `init_db.sql` ainda não contém todo o schema usado pelos services dirigidos; migrations 001–005 vivem na API.

### Decisão

Enquanto não existe instalação de produção, consolidar o estado final desejado do schema diretamente no baseline `init_db.sql`, incluindo as alterações hoje distribuídas nas migrations 001–005 e os novos objetos temáticos aprovados. Ajustar ou substituir migrations de desenvolvimento que se tornarem redundantes. `init_mutation_db.sql` permanece restrito ao banco de mutação/auditoria e o schema de metadados continua sob as migrations oficiais do Airflow.

Antes do primeiro deploy de produção, executar um teste automatizado em bancos vazios que comprove que os scripts de inicialização criam integralmente o schema esperado. Depois desse primeiro deploy, mudanças subsequentes passam a ser exclusivamente migrations incrementais e imutáveis.

### Consequências

O desenvolvimento pode usar reset total sem carregar dívida histórica artificial. O primeiro build de produção terá um baseline completo, reproduzível e testado. A consolidação exige coordenar o runner de migrations da API para que ele não reaplique DDL incorporado ao baseline e preservar somente migrations ainda necessárias.

## ADR-011 — Runtime geoespacial do Airflow

Status: **APROVADO — 30/08/2026**

### Contexto

Scheduler, webserver e triggerer executam Python 3.8.19 com versões geoespaciais consistentes entre si, mas inferiores aos mínimos declarados no `requirements.txt`. O Compose usa `_PIP_ADDITIONAL_REQUIREMENTS` sem pins e reinstala dependências no startup. A [documentação oficial do Airflow](https://airflow.apache.org/docs/docker-stack/entrypoint.html) reserva esse mecanismo para teste/depuração e recomenda incorporar dependências em imagem própria.

### Decisão proposta

Construir imagem imutável baseada em tag explícita do Airflow 2.8.4 e versão de Python suportada, com lock testado para Rasterio/GDAL, NumPy, PyProj/PROJ, Shapely, GeoPandas e Fiona. Avaliar `exactextract` em Python 3.9+ para cobertura fracionária e validação independente, sem substituir silenciosamente o cálculo elipsoidal.

### Consequências

Todos os componentes Airflow usarão o mesmo ambiente, sem depender da disponibilidade futura do PyPI durante restart. O build terá `pip check`, smoke tests geoespaciais e DAG import test. Adicionar uma biblioteca exigirá evidência de correção, compatibilidade, manutenção e benefício operacional.

## ADR-012 — Disponibilização do raster Gold

Status: **APROVADO — COG canônico e PostGIS Raster in-db**

### Decisão proposta

Manter o COG de SC como artefato canônico na Gold e registrar no PostGIS um catálogo `mapbiomas_raster_asset` com chave relativa, checksum, metadados e footprint. A entrega externa ocorre pela camada de storage/API. As áreas por classe/AOI ficam em tabela fato ligada ao asset. Carregar também uma representação derivada tiled in-db, própria para QGIS e consultas PostGIS Raster — implementada como **uma tabela física por ano** `mapbiomas_raster_<ano>` (com `AddRasterConstraints`), não uma tabela unificada: view por ano não funciona no QGIS (`srid 0` em `raster_columns`) e uma tabela unificada seria dado duplicado, já que a série temporal está em `mapbiomas_clip`.

### Consequências

O produto atende tanto download interoperável quanto consulta raster no banco. A duplicação é intencional: COG é o artefato portátil/autoritativo; tiles PostGIS são a camada de consulta. A carga só será consolidada após benchmark de tamanho, desempenho e backup/restore. Out-db permanece fora do primeiro incremento.

## ADR-013 — Geometria analítica de ZA e Buffer de Abrangência

Status: **APROVADO — 30/08/2026**

### Decisão

As análises temáticas publicam três categorias mutuamente exclusivas: UC, ZA exclusiva e Buffer de Abrangência exclusiva. Para ZA/Buffer de Abrangência, a geometria de análise é sempre `zona − UC`. Buffer de Abrangência também recebe diferença final no PostGIS para eliminar resíduos de reprojeção. Quando uma ZA Bronze não contém identificador canônico de UC, a associação automática seleciona a UC com maior área de interseção; candidatas e decisão são auditadas em `za_oficial_fonte` e `za_oficial_uc`.

### Evidência e consequência

A validação da Frente 0 confirmou 11 UCs, 5 ZAs e 6 Buffers de Abrangência ativos, sem sobreposição positiva Buffer de Abrangência–UC. PRODES e MapBiomas Alerta passaram a operar sobre a partição exclusiva. Cinco recortes UC do MapBiomas Alerta ainda apresentam resíduos microscópicos de reprojeção; a diferença final no PostGIS para esse domínio ficou registrada para a estabilização final, pois não altera áreas analíticas relevantes.

## ADR-014 — MapBiomas sempre processa todos os anos disponíveis

Status: **APROVADO E IMPLEMENTADO — 13/09/2026**

Uma execução descobre a união dos GeoTIFFs raw, partições Bronze válidas e assets
`PUBLISHED` da coleção/versão. O campo `year` recebido no contexto não filtra a
série. Dynamic task mapping cria uma execução por ano; referência, legenda e
snapshot cadastral são compartilhados. Assim, toda UC nova recebe a série histórica
disponível, com reaproveitamento dos rasters publicados.

## ADR-015 — Baseline cadastral e raster anual canônicos

Status: **APROVADO E IMPLEMENTADO — 13/09/2026**

`init_db.sql` contém vigência/autoria da UC, `uc_geometry_version`,
`cadastral_event` e `ensure_mapbiomas_raster_year(integer)`. O baseline foi
validado por aplicação e reaplicação em banco vazio. Estruturas antigas sem
consumidor (`outbox_event` e `api_idempotency`) não foram incorporadas.

## ADR-016 — Duplicidade em lotes de criação de UC

Status: **APROVADO E IMPLEMENTADO — 13/09/2026**

Uploads de novas UCs usam `reject_batch` por padrão, preservando atomicidade quando
qualquer feição já existe. O cliente pode solicitar `skip_duplicates` para publicar
somente as feições inéditas. O original imutável, o resultado por feição e os
vínculos de duplicidade preservam a auditoria; o canônico aceito e a Bronze recebem
somente o subconjunto processável. Um lote integralmente duplicado retorna conflito
sem escrita na Bronze e sem DAG. A opção não se aplica aos endpoints de atualização.
