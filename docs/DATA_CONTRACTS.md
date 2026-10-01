# Contratos de dados

## Princípios comuns

- Outputs são imutáveis por `run_id`/versão; publicação usa diretório temporário e rename/move atômico quando local.
- Todo artefato possui `schema_version`, domínio, período, origem, checksum, timestamps UTC e versão do pipeline.
- XCom transporta somente referências/metadados, não CSV, GeoDataFrame ou raster.
- CRS persistente vetorial: EPSG:4674. Operação métrica explicita CRS/método.
- Segredos nunca aparecem em manifesto, quality ou URL registrada.
- Rejeições mantêm código estável, mensagem, estágio, origem e amostra limitada; contagens totais não dependem do limite da amostra.

## UC, ZA oficial e Buffer de Abrangência

### Criação de UCs em lote

O upload de novas UCs aceita uma ou mais feições e declara `duplicate_policy` no
multipart. `reject_batch` é o padrão: qualquer identidade forte ou geometria já
existente rejeita o lote inteiro antes da Bronze. `skip_duplicates` é opt-in e
publica somente as feições inéditas; cada item ignorado permanece no original em
quarentena e é identificado por `feature_index`, estado `SKIPPED_DUPLICATE` e
`duplicate_matches`. Se todas as feições forem duplicadas, a publicação responde
`409 UC_ALREADY_EXISTS` e não cria partição Bronze nem dispara DAG.

O artefato canônico aceito e o manifesto Bronze contêm apenas as feições aptas ao
processamento. O arquivo original nunca é reescrito. A política integra o fingerprint
de idempotência e é válida exclusivamente para `entity_type=uc` com
`operation=create`; atualizações cadastrais continuam no contrato próprio.

A união canônica de ZA oficial na Bronze é a única fonte para decidir se uma UC possui
ZA oficial. Ela reúne a carga inicial e toda ZA posteriormente publicada pela API. Para
uma UC sem correspondência nessa união, `DAG_ZA_BUFFER` deriva um Buffer de Abrangência de 3 km a partir da
geometria Bronze da UC. `za_oficial` e `buffer_abrangencia` no PostgreSQL são projeções de
destino: divergência com a Bronze é erro de consistência e não altera a elegibilidade.

Antes de qualquer trigger temático dirigido, `validate_zone_readiness` exige que cada
UC ativa possua exatamente uma geometria válida e ativa: ZA oficial ou Buffer de Abrangência. Zero zonas,
duas zonas ou geometria inválida bloqueiam PRODES, MapBiomas Alertas e MapBiomas Uso e
Cobertura no fluxo dirigido. FIRMS aplica a mesma validação em snapshot `REPEATABLE READ`
antes de construir suas relações independentes.

## FIRMS

### Bronze manifest v1

Campos obrigatórios: `schema_version`, `domain=firms`, `run_id`, `source_product`, `request_bbox`, `start_date`, `day_range`, `requested_at`, `received_at`, `http_status`, `content_type`, `object_key`, `checksum_sha256`, `bytes`, `record_count` e `sanitized_endpoint`.

### Silver record v1

Campos obrigatórios: `detection_id`, `source_product`, `source_version`, `satellite`, `instrument`, `acquired_at_utc`, `latitude`, `longitude`, `confidence_raw`, `confidence_scheme`, `confidence_class`, `frp_mw`, `daynight`, `geom`, `source_checksum`, `run_id`. `confidence_score` é obrigatório para MODIS e nulo para VIIRS; brilho, scan, track e tipo inferido permanecem opcionais conforme produto. Silver aceita VIIRS `l/n/h` e MODIS `>=35` após o recorte de SC.

### Gold/PostGIS v1

Silver + `id_uc`, `id_za_oficial`, `id_buffer_abrangencia`, `tipo_cruzamento`, `relation_id` e timestamps de camada. Invariantes: `id_uc` obrigatório; no máximo um id de zona; Gold/PostGIS aceita somente VIIRS `h` ou MODIS com escore `>70`; constraint natural impede replay duplicado.

### Controle de backfill v1

`firms_backfill_window` registra `source_product`, `start_date`, `end_date`, `processing_state`, `attempt_count`, `run_id`, checksum/manifesto publicados, timestamps e erro saneado. A chave natural `source_product + start_date + end_date` impede executar duas vezes a mesma janela. A tabela não guarda payload, URL autenticada ou segredo. Se uma tentativa falhar depois da aquisição, o checkpoint preserva manifesto e checksum para retomar do Bronze imutável.

## MapBiomas

### Coordenadas do dataset

`collection`, `version` e `year` são parâmetros de cada task anual. A DAG descobre todos os anos disponíveis da coleção/versão nas fontes raw, Bronze válida e assets `PUBLISHED`; `dag_run.conf.year` não filtra o conjunto. Todo caminho e registro deriva do dataset descoberto. Cada `collection × version × year` é um asset imutável independente.

**Zona `raw`.** Todo dado bruto que alimenta a Bronze vive em `airflow/data/raw/` (dentro de `airflow/data/`, no `.gitignore`), não em drives externos: `raw/mapbiomas/` (GeoTIFFs anuais + 9 companheiros da Coleção 11 + `MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx`) e `raw/ibge/` (`limites_SC.*`). O `bootstrap_bronze` copia `raw` → Bronze com SHA-256 e manifesto; a Bronze é a cópia canônica imutável e a zona `raw` é landing descartável. Defaults em `PipelineConfig`: `./data/raw/mapbiomas`, `./data/raw/ibge`.

### Bronze manifest v1

`domain=mapbiomas_lulc`, `product=coverage_30m`, `collection`, `version`, `year`, `published_date`, `source_kind=official_download`, inventário completo de objetos, SHA-256 e bytes por arquivo, licença/citação e metadados GDAL. Para o raster: `crs=EPSG:4326`, transform, width, height, dtype, banda, blocos, compressão, NoData lógico 0 e checksum GDAL.

### Legenda v1 (`mapbiomas_legend_class`)

Seed versionado em `airflow/dags/scripts_python/data/mapbiomas_legend_col11.json`, derivado uma única vez do CSV terminal oficial e do PDF de códigos, nunca parseado em runtime. 41 classes por `collection`/`version`: 33 terminais (idênticas aos valores do raster além do 0) e 8 nós agregados (`1, 10, 14, 22, 26, 18, 19, 36`). Campos: `class_code`, `parent_class_code`, `hierarchy_level`, `name_pt_br`, `name_en`, `color_hex`, `is_terminal`, `is_active`, `source_checksum`. Correções de proveniência aplicadas: a classe 48 (Outras Lavouras Perenes), impressa como 3.2.1.4 no PDF, tem pai corrigido para 36 (Lavoura Perene); `Coffe→Coffee`, `Rocky Outrcrop→Rocky Outcrop`, `Marismas→Marisma`. `seed_legend_postgres` reescreve linhas apenas quando o `source_checksum` do seed muda; o indicador beta não é coluna da tabela (aplicado via `MapbiomasPipelineService.BETA_CLASS_IDS`).

### Silver raster v1

Uma banda categórica inteira `classification`, recorte vetorial de SC preservando a grade EPSG:4326 da fonte, transform alinhado, NoData explícito 0 e sidecar de proveniência. Classes desconhecidas bloqueiam o lote; classes oficiais ausentes em SC são válidas. Nenhum valor é silenciosamente remapeado e nenhuma interpolação altera os códigos.

### Gold raster v1

COG de SC com os mesmos valores da Silver, overviews `nearest neighbour`, NoData 0, coleção/versão/ano/checksum e QML/legenda como sidecars. É um produto final obrigatório, mantido na Gold e registrado em `mapbiomas_raster_asset` por chave relativa, metadados, footprint e checksum. O catálogo aponta para o arquivo; não armazena paths Windows.

### PostGIS raster tiles v1

O raster analítico in-db é **uma tabela física por ano**, `mapbiomas_raster_<ano>` (ex.: `mapbiomas_raster_2025`), criada pela função canônica `ensure_mapbiomas_raster_year(integer)` e mantida por `load_raster_postgres`. **Não há tabela unificada de todos os anos** nem view/partição: a série temporal por classe/AOI está em `mapbiomas_clip` (`reference_year`), não em uma união de rasters. Cada tabela anual é catalogada em `mapbiomas_raster_asset`, tem PK, FK, índice GiST e `AddRasterConstraints`. Durante reprocessamento cadastral, um raster `PUBLISHED` válido é somente reutilizado; criação/recriação ocorre na publicação inicial ou recuperação de carga incompleta.

Colunas: `id_raster_tile` (BIGSERIAL PK), `id_raster_asset`, `collection_code`, `collection_version`, `reference_year`, `tile_row`, `tile_col`, `rast`, `created_at`; UNIQUE `(id_raster_asset, tile_row, tile_col)`.

**O tiling é apenas particionamento físico, nunca reamostragem.** Cada tile é uma janela verbatim do COG Gold do ano: mesma grade e resolução nativa EPSG:4326 (`ST_ScaleX` = `0,00026949458523585647`), mesma origem (`ST_UpperLeftX/Y` idênticos ao COG), sem `ST_Resample`/`ST_Transform`/reprojeção, banda `8BUI`, NoData 0. Os valores de classe são byte-idênticos ao COG do ano (mesma contagem por classe via `ST_ValueCount`). Tiles de borda mantêm o tamanho natural menor, de modo que a união (`ST_Extent`) coincide exatamente com a extensão do COG e a soma das áreas de pixel dos tiles iguala `largura × altura` do COG (sem gaps, sem overlaps, sem padding). Anos diferentes do MapBiomas ficam na mesma grade WGS84 (offset inteiro de pixel), então os `ST_Extent` por ano coincidem e as áreas são comparáveis ano a ano.

`RASTER_TILE_SIZE` (512; sobrescrevível por `dag_run.conf.raster_tile_size` no benchmark da Fase E) é pixels por tile, não fator de resolução. A carga é in-db por `ST_FromGDALRaster` (sem `raster2pgsql`), transacional e idempotente pelo `checksum_sha256` do asset: replay idêntico não recarrega, carga com contagem divergente é limpa e refeita. O conjunto é registrado em `raster_columns` com índice GiST sobre `ST_ConvexHull(rast)` e constraints `srid, scale, same_alignment, num_bands, pixel_types, nodata_values, extent` (sem `blocksize`, pois tiles de borda variam). Publicação incompleta mantém o asset em `STAGED`.

**Overviews não são criados in-db.** O COG já carrega pirâmides internas para clientes de arquivo, e decimação `nearest` agressiva de raster categórico descarta classes raras (por isso `o_2/o_8/o_32` foram removidas). Qualquer overview in-db futuro deve ser camada separada, explicitamente não analítica, com extensão correta e decidido no benchmark/Frente 5.

### Gold statistics v1 e `mapbiomas_clip`

Chave natural: `id_raster_asset + id_legend_class + aoi_type + id_uc + coalesce(id_za_oficial,0) + coalesce(id_buffer_abrangencia,0) + aoi_geometry_sha256 + area_method_version`. Medidas por linha (uma por classe por AOI): `pixel_count`, `class_area_ha` (área da classe no AOI), `classified_area_ha` (total classificado do AOI), `aoi_area_ha_geodesic`, `coverage_ratio`, `area_method`, `area_method_version`, `boundary_policy=pixel_center` e `run_id`. `mapbiomas_clip` também denormaliza `collection_code`, `collection_version` e `reference_year` (do asset) para consulta multi-ano sem join. Classe 0 é proibida. Para `ZA` e `BUFFER_ABRANGENCIA`, a geometria hashada é sempre `zona − UC`; `UC` pontual não gera linha de área.

`coverage_ratio` pode passar de 1 por efeito de borda do método por centro de pixel; a constraint aceita até `1,05` e rejeita acima disso.

A carga (`load_area_statistics_postgres`) resolve o `id_uc` externo do Parquet para `uc.id_uc`, o `id_za_oficial`/`id_buffer_abrangencia` ativo por UC, o `id_legend_class` e o `id_raster_asset` (pelo checksum do COG); exige legenda e raster já carregados. `aoi_geometry_sha256` é o SHA-256 da geometria de cada AOI no snapshot; `aoi_geometry_version` referencia o checksum do snapshot. `ON CONFLICT DO NOTHING` garante replay sem duplicação. Um resultado estatístico nunca aponta para asset ausente, rejeitado ou não publicado.

## Quality summary v1

Campos comuns: `dag_id`, `run_id`, `domain`, `stage`, `started_at`, `finished_at`, `status`, `input_objects`, `input_records`, `accepted_records`, `rejected_records`, `duplicate_records`, `output_records`, `reason_counts`, `checks`, `artifacts` e `pipeline_version`.

Para raster, records podem significar pixels/tiles/AOIs e devem ter nomes específicos adicionais. Para FIRMS, o total de relações pode exceder o número de detecções; por isso, `source_detections` e `spatial_relations` são métricas diferentes.

## Retenção

Bronze é fonte de reprocessamento e não entra no cleanup atual. Silver/Gold, quality, logs e metadata possuem ciclos distintos. Nenhum contrato autoriza exclusão antes dos ADRs de retenção serem aprovados.
