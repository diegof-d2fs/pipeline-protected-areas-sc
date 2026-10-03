# Padrões de desenvolvimento

## Princípio: melhor prática de engenharia de dados (critério de banca)

Este projeto é o TCC do responsável e será avaliado em banca final. Toda decisão de
estrutura, layout ou fluxo deve seguir a **melhor prática de engenharia de dados
possível** — isso é critério de avaliação, não preferência. Atalhos estruturais,
inconsistências e escolhas "funciona mas não é o padrão" são passivo de banca.

Regras práticas:

- Ao decidir layout/fluxo, propor primeiro o padrão convencional (medallion
  `raw → bronze → silver → gold`; uma única zona `raw` gitignorada dentro do
  projeto; idempotência por checksum; sem passos manuais; sem drives externos;
  camadas imutáveis e auditadas; contratos documentados; DAG que resolve o próprio
  trabalho), não o expediente.
- Sinalizar *smells* proativamente, mesmo sem ser perguntado, e registrá-los aqui.

### Críticas / dívidas de engenharia de dados registradas

- **[RESOLVIDO 31/08/2026]** Dados brutos que alimentam a Bronze estavam em drives
  externos (`E:\mapbiomas11`, `X:\base_teste`) — frágil e não reproduzível.
  Movidos para `airflow/data/raw/{mapbiomas,ibge}/` (dentro de `airflow/data/`, no
  `.gitignore`); mounts externos removidos do Compose.
- **[ABERTO]** Arquivos companheiros da Coleção 11 (legenda, PDFs, QMLs, ZIPs) são
  copiados para a Bronze uma vez por partição de ano — redundância. São de coleção,
  não de ano; deveriam ficar numa partição `collection=NN/_companions/` única.
- **[RESOLVIDO 13/09/2026]** `DAG_MAPBIOMAS` descobre todos os anos disponíveis
  em `raw/mapbiomas/`, `raw/mapbiomas/tifs/`, Bronze e no catálogo PostGIS. O
  mapeamento anual reaproveita publicações idênticas; `dag_run.conf.year` não
  reduz a série durante reprocessamento cadastral.

## Controle de versão entre os três repositórios

O projeto vive em três repositórios Git separados — `pipeline-protected-areas-sc` (este),
`fast-api-protected-areas-sc` e `frontend-protected-areas-sc` — cada um com seu próprio remoto no
GitHub (`github.com/diegof-d2fs/...`).

## Baseline e migrations

O sistema está exclusivamente em desenvolvimento e ainda não possui banco de produção. Até o primeiro deploy produtivo:

- `init_db.sql` é o baseline canônico do banco principal e deve representar integralmente o schema vigente;
- `init_mutation_db.sql` é o baseline canônico do banco de mutação/auditoria;
- o banco de desenvolvimento pode ser apagado e recriado para validar os baselines;
- migrations criadas durante o desenvolvimento podem ser corrigidas, consolidadas ou absorvidas pelo baseline, desde que os consumidores sejam atualizados em conjunto;
- o banco de metadados do Airflow continua sendo criado e evoluído pelas ferramentas oficiais do Airflow.

O gate anterior ao primeiro deploy de produção deve criar bancos vazios, aplicar apenas os baselines correspondentes e validar tabelas, colunas, tipos, chaves, constraints, índices, extensões, funções, triggers e permissões esperadas. Após esse deploy, os baselines tornam-se imutáveis para aquela linha de instalação e toda alteração nova deve usar migration incremental versionada.

## Documentação Python

Classes, métodos e funções públicas devem possuir docstrings claras quando o nome e a assinatura não forem suficientes para expressar o contrato. Funções privadas também devem ser documentadas quando encapsularem regra de negócio, comportamento geoespacial, idempotência, efeitos colaterais ou condições de erro não óbvias.

As docstrings devem, conforme aplicável, descrever:

- responsabilidade e limites do componente;
- parâmetros e unidades;
- valor retornado;
- exceções relevantes;
- efeitos colaterais;
- invariantes e decisões de borda necessárias ao uso correto.

Comentários internos devem explicar o motivo de uma restrição, fórmula ou solução não evidente. Não devem repetir o código, narrar alterações históricas, registrar changelog, mencionar prompts ou ferramentas de geração, atribuir autoria artificial ou humana, nem citar número de fase, item ou requisito de um documento de planejamento (SDD, PRD, plano de implementação). Um comentário deve continuar correto e útil para alguém que nunca viu esses documentos e não sabe que este é um projeto de TCC. Histórico, decisões arquiteturais e o vínculo com fases/requisitos pertencem ao Git, aos ADRs e à documentação do projeto — nunca ao código-fonte em si. Válido para os três repositórios do produto (`pipeline-protected-areas-sc`, `fast-api-protected-areas-sc`, `frontend-protected-areas-sc`), não só para este.

Toda documentação deve permanecer sincronizada com o comportamento atual. Uma mudança funcional que invalide uma docstring, comentário, contrato ou ADR só está concluída depois da atualização correspondente.

## Dependências e imagens

Dependências Python de execução devem ser instaladas durante o build de uma imagem versionada, nunca descobertas e instaladas sem pins durante o startup de um serviço. O arquivo de lock ou conjunto equivalente deve ser gerado e validado para a versão exata do Python, Airflow e plataforma do container.

### Baseline auditado de imagens

O baseline das frentes em implementação foi auditado em 30/08/2026. A imagem Airflow contém `GeoPandas`, `Shapely`, `PyProj`, `Fiona`, `Rasterio`, `NumPy`, `Pandas`, `PyArrow`, `OpenPyXL`, `psycopg2`, `requests`, `tenacity`, `pytest` e o binário `gdalinfo` provido por `gdal-bin`. Os bancos usam a imagem oficial `postgis/postgis:17-3.5`, sem customização, com `postgis_raster` e o driver GTiff habilitado por configuração do servidor. A carga Raster é responsabilidade do loader versionado da DAG, por `ST_FromGDALRaster` e `ST_Tile`; `raster2pgsql` não é disponibilizado pelo pacote compatível dessa imagem e não será compilado de outra versão. Scheduler, webserver, triggerer e testes usam a mesma imagem Airflow; somente o banco principal usa a imagem PostGIS estendida, pois o banco de mutações não processa raster.

Não introduzir `exactextract` enquanto a cobertura fracionária não for aprovada, nem `boto3`/provider AWS enquanto o adapter S3 não estiver em implementação. QGIS é cliente externo do pesquisador e não integra as imagens do pipeline. Qualquer nova biblioteca ou binário exige atualizar este inventário, o Dockerfile/requirements correspondente e seu teste de build antes de iniciar código dependente.

**Regra obrigatória de implementação:** quando uma dependência for necessária para uma capacidade aprovada do produto, ela deve entrar na imagem versionada responsável (`Dockerfile`, `requirements` e Compose quando aplicável), com pin/versão compatível e validação no build. É proibido substituir essa inclusão por instalação manual em container em execução, imagem efêmera, comando pontual ou outro atalho não reproduzível. Provas de conceito isoladas só podem ocorrer antes da decisão de adoção e nunca constituem a implementação entregue.

Uma dependência nova só deve entrar quando oferecer uma capacidade necessária ou reduzir risco técnico de forma demonstrável. A avaliação deve considerar correção numérica, maturidade, manutenção, licença, wheels para o runtime, compatibilidade binária com NumPy/GDAL/PROJ/GEOS, consumo de memória e comportamento em datasets grandes.

O gate da imagem deve executar, no mínimo:

- instalação sem resolução implícita durante startup;
- `pip check`;
- import e registro das versões de Rasterio/GDAL, PyProj/PROJ, Fiona/GDAL, Shapely, GeoPandas e NumPy;
- smoke test de leitura raster, transformação de CRS e área elipsoidal;
- importação das DAGs sem erro;
- verificação de que scheduler, webserver, triggerer e workers usam a mesma imagem por digest.

Durante builds e testes Docker, listar imagens, containers e volumes criados pela frente. Depois da consolidação, remover somente containers parados, imagens intermediárias/dangling e volumes temporários identificados como pertencentes aos testes do projeto. Manter as imagens oficiais de base e a imagem customizada consolidada necessária ao projeto. Nunca executar prune amplo sem apresentar previamente os alvos.

Qualquer reset, truncate, drop ou exclusão de registros no banco de desenvolvimento deve ser comunicado antes da execução. O responsável pode optar por executar no DBeaver ou autorizar um comando Docker explícito. O comando deve indicar banco e objetos exatos; nenhuma limpeza de banco é implícita em testes ou scripts de bootstrap.

Documentação deve ser atualizada ao final de cada frente implementada e testada. Alterar somente contratos, ADRs, plano, testes e handoff afetados pelo resultado; evitar reescrever documentos não relacionados ou acumular um handoff narrativo redundante.

## Definição de pronto

Nenhuma frente, DAG ou task é considerada pronta, concluída ou "executada ponta a ponta" antes de todas as etapas serem validadas, **inclusive a publicação dos resultados no PostgreSQL/PostGIS**. Artefatos de arquivo Silver/Gold (Parquet, CSV, COG, manifestos) são passos intermediários, não o produto final. Ao avaliar ou relatar status, consultar as tabelas de destino e confirmar que as linhas esperadas existem; a redação do handoff é um retrato a verificar, não fonte de verdade. Todo pipeline temático precisa de uma etapa explícita de publicação no PostGIS pós-Gold — catálogo/seed, tabela fato e, quando aplicável, raster —, idempotente e validada, antes do encerramento da frente. A redação da documentação deve ser proporcional ao que foi de fato validado.

Ao final de cada fase, revisar os artefatos criados durante desenvolvimento/testes (tabelas temporárias, `.tif` intermediários, dumps, scripts descartáveis, containers, imagens e volumes Docker) e remover tudo que foi criado apenas para teste e não faz parte da arquitetura final — depois de confirmar tecnicamente que aquilo não é recurso legítimo da aplicação. Estruturas criadas "por enquanto" que não são o desenho definitivo contam como descartáveis. Nunca prune amplo; sempre apresentar os alvos antes de remover.

## Raster analítico

Para o raster analítico principal, o tamanho do tile é apenas particionamento físico e nunca deve alterar a resolução espacial. A divisão em tiles particiona os pixels; não reamostra. O raster principal preserva a grade, a origem, a resolução nativa (`ST_ScaleX/Y`), o `dtype` e os valores de classe do GeoTIFF de origem, sem `ST_Resample`/`ST_Transform`/reprojeção e sem interpolação (bilinear, cúbica, lanczos, average). Se alguma reamostragem for inevitável em produto derivado de raster categórico, usar `nearest` e mantê-lo explicitamente separado do raster analítico. Overviews só devem existir quando houver necessidade real demonstrada, sempre como camada de visualização separada, nunca como fonte analítica nem confundíveis com o raster principal.
