# Inventário dos pipelines

| DAG | Estado | Fonte | Frequência atual | Service | Dependências | Saídas |
|---|---|---|---|---|---|---|
| `DAG_UCS` | IMPLEMENTADA | Bronze UC/manifesto | diária | `UcsPipelineService` | nenhuma | Silver, Gold, UC/histórico/PostGIS, DQ |
| `DAG_ZA_BUFFER` | IMPLEMENTADA | Bronze ZA/UC | diária | `ZaBufferPipelineService` | UC | Silver, Gold, ZA/Buffer de Abrangência/PostGIS, DQ |
| `DAG_PRODES` | IMPLEMENTADA | Bronze PRODES | 03:00 diária | `ProdesPipelineService` | UC e ZA/Buffer de Abrangência | Silver, Gold, `prodes_clip`, DQ |
| `DAG_MAPBIOMAS_ALERTA` | IMPLEMENTADA — resíduo geométrico pendente | Bronze alerta | a cada 6 h | `MapbiomasAlertaPipelineService` | UC e ZA/Buffer de Abrangência | Silver, Gold, `mapbiomas_alerta_clip`, DQ |
| `DAG_FIRMS` | IMPLEMENTADA | FIRMS Area API, NOAA‑20/NOAA‑21 NRT | 04:30 UTC diária | `FirmsPipelineService` | snapshot ativo UC e ZA/Buffer de Abrangência | Bronze, Silver, Gold, `firms_clip`, DQ |
| `DAG_FIRMS_BACKFILL` | IMPLEMENTADA | FIRMS Area API/SP | manual, em páginas de janelas ≤5 dias | `FirmsPipelineService` + `FirmsAreaClient` | `firms_backfill_window` e snapshot ativo UC/ZA/Buffer de Abrangência | Bronze imutável, Silver, Gold, `firms_clip`, DQ |
| `DAG_MAPBIOMAS` | IMPLEMENTADA — carga PostGIS, reprocessamento cadastral e E2E ZIP/GeoJSON reais comprovados | pacote oficial Coleção 11 para todos os anos descobertos em raw/Bronze/PostGIS, limite IBGE/SC 2025 e referência estadual oficial | manual e acionada pelo fluxo cadastral; dynamic mapping anual | `MapbiomasPipelineService` | snapshot exclusivo UC/ZA/Buffer de Abrangência por importação; referência e legenda compartilhadas | Bronze imutável, Silver alinhada, COG/QML/legenda Gold, estatísticas reconciliadas, `mapbiomas_legend_class`, `mapbiomas_raster_asset`, `mapbiomas_raster_<ano>` e `mapbiomas_clip` |
| `DAG_CLEANUP_MEDALLION_RETENTION` | PARCIAL | filesystem Silver/Gold | 02:00 diária | função na própria DAG | nenhuma | exclusão e log simples |
| `DAG_CLEANUP_AIRFLOW` | PLANEJADA | logs + metadata | domingo 03:00 proposta | service/funções pequenas | nenhuma | summary auditável |

## Padrão confirmado

Os pipelines especializados usam extração, validação, transformação, carga Silver, Gold e PostGIS, com passagem de artefatos leves por XCom. PRODES e MapBiomas Alerta paralelizam cruzamentos UC e zona antes da consolidação. FIRMS e MapBiomas possuem services próprios; FIRMS mapeia produto/janela e MapBiomas mapeia ano.

### Partição espacial implementada

`DAG_ZA_BUFFER`, `DAG_PRODES` e `DAG_MAPBIOMAS_ALERTA` usam a partição por UC: `UC`, `ZA − UC` ou `Buffer de Abrangência − UC`. Um polígono-fonte que atravesse categorias gera um recorte e uma detecção/relação em cada categoria atingida; os recortes não podem sobrepor-se e suas áreas são calculadas separadamente. Buffer de Abrangência recebe diferença final no PostGIS para eliminar resíduos de reprojeção. O mesmo endurecimento final permanece pendente para cinco recortes UC do MapBiomas Alerta.

## Dependências e reprocessamento

Execuções periódicas das bases temáticas são independentes de uma execução de UC no mesmo horário: elas usam e validam o snapshot cadastral ativo já publicado. Após alteração cadastral dirigida pela API, o fluxo segue outra entrada: `DAG_UCS` conclui a UC, `DAG_ZA_BUFFER` resolve ZA/Buffer de Abrangência, `validate_zone_readiness` exige exatamente uma zona válida por UC ativa e somente então são disparadas e aguardadas `DAG_PRODES`, `DAG_MAPBIOMAS_ALERTA` e `DAG_MAPBIOMAS`. As três podem executar em paralelo depois dessa barreira. FIRMS permanece excluído. MapBiomas sempre descobre todos os anos disponíveis para a coleção/versão nas fontes raw, partições Bronze válidas e assets `PUBLISHED`; um `year` herdado da API não filtra a série. O manifesto identifica as UCs, e o service congela suas geometrias e zonas persistidas em um snapshot por importação. Estatísticas Silver/Gold são isoladas por importação, os fatos são publicados em `mapbiomas_clip` para cada ano e rasters anuais existentes são reutilizados. Os E2E reais com lote ZIP e lote GeoJSON de três UCs foram comprovados em 13/09/2026.

## Regra de transição entre frentes

Toda frente só pode ser encerrada após: atualizar os artefatos técnicos pertinentes; registrar resultados, pendências e evidências na documentação; revisar contratos, riscos, testes e plano das frentes seguintes à luz dos aprendizados; e explicitar mudanças de escopo ou pré-requisitos antes do início da próxima frente. A frente seguinte não deve reutilizar pressupostos invalidados pela validação anterior.

### Aprendizados obrigatórios da Frente 0

- A Bronze é a fonte de entrada das transformações; PostGIS é persistência e consulta, não substituto do lote de origem.
- Para cada UC, a existência de ZA oficial é determinada somente pela união Bronze de ZA (carga inicial + uploads posteriores pela API). Sem correspondência Bronze, deriva-se Buffer de Abrangência de 3 km. Uma divergência no PostgreSQL deve falhar como inconsistência de projeção, nunca substituir essa decisão.
- Uma temática periódica nunca deve usar `ExternalTaskSensor` esperando uma DAG cadastral da mesma data lógica. FIRMS segue esse contrato e usa o snapshot cadastral ativo já publicado.
- Serviços vetoriais devem aceitar GeoJSON, Shapefile e ZIP quando esses formatos fizerem parte do contrato Bronze.
- Toda escrita temporária deve criar seu diretório-pai idempotentemente; cargas pesadas devem ser testadas no runtime Airflow real.
- Relações espaciais precisam de regra determinística, auditoria e validação pós-carga. Para ZA sem identificador canônico, aplica-se maior área de interseção.
- Geometrias analíticas devem ser exclusivas e validadas após reprojeção/persistência; resíduos numéricos conhecidos ficam documentados como dívida técnica, nunca ocultados.
- Reprocessamentos padrão de UC, ZA e Buffer de Abrangência devem consolidar todos os lotes Bronze canônicos, mantendo seleção de lote único somente para backfill dirigido. A carga deve ser idempotente por identidade natural.
- O Buffer de Abrangência deve ser rederivada quando a UC ou a ZA oficial mudar; qualquer Buffer de Abrangência ativo de UC que passou a ter ZA oficial deve ser desativada. ZA e Buffer de Abrangência são recortadas contra todas as UCs, não apenas sua UC associada.
