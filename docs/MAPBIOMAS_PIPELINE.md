# Plano de engenharia — MapBiomas Uso e Cobertura do Solo

> **Estado autoritativo em 13/09/2026:** implementado até Medallion e PostGIS.
> A DAG descobre e processa todos os anos disponíveis em raw, Bronze e catálogo
> publicado, inclusive em reprocessamento cadastral. As seções iniciais abaixo
> são histórico do plano e não prevalecem sobre este contrato.

## 1. Decisão arquitetural desta revisão

**APROVADO para implementação após a Frente 1 de ambiente reproduzível.**

O primeiro incremento deve usar o pacote oficial da Coleção 11 já baixado em `E:\mapbiomas11`. Não há necessidade de Google Earth Engine, Google Cloud Storage, job assíncrono, polling ou credencial externa para processar 2025.

O fluxo recomendado é:

```text
pacote oficial local da Coleção 11
  -> Bronze: GeoTIFF Brasil 2025 + legendas + documentos + manifest
  -> Silver: GeoTIFF categórico recortado exatamente para Santa Catarina
  -> Gold raster: GeoTIFF/COG de SC + estilos e legenda
  -> Gold tabular: hectares por classe e por AOI
  -> PostgreSQL/PostGIS: estatísticas por UC, ZA ou Buffer de Abrangência
```

O caminho `E:\mapbiomas11` é uma origem de ingestão, não deve ser uma dependência permanente da DAG. Na primeira ingestão, o pacote deve ser copiado para a Bronze gerenciada e identificado por checksum. Todas as execuções posteriores leem a Bronze.

### Atualização pelo aprendizado da Frente 0

O pipeline deve consumir AOIs exclusivos produzidos a partir da Bronze e manter a relação `UC`, `ZA − UC` e `Buffer de Abrangência − UC` como contrato de cálculo. A validação de aceite deve ocorrer no runtime Airflow e confirmar, após persistência, contenção geométrica e ausência de resíduos relevantes. Escritas temporárias e artefatos vetoriais auxiliares devem criar seus diretórios-pai idempotentemente. O resíduo microscópico identificado em MapBiomas Alerta será tratado na estabilização final e não altera o método de área aprovado para MapBiomas Uso e Cobertura.

## 2. Evidências confirmadas

### 2.1 Fonte e versão

- Coleção: **11**.
- Versão dos materiais: **v1**, publicada em 12/08/2026.
- Produto: **MapBiomas Cobertura 30 m**.
- Série oficial: **1985–2025**.
- Arquivo disponível para este incremento: **Brasil, ano 2025**.
- Licença: dados públicos e gratuitos sob **CC BY 4.0**, com citação da fonte.

A página oficial confirma que a Coleção 11 é a mais recente, que os mapas anuais são GeoTIFFs de banda única e que os valores dos pixels representam os códigos da legenda: [MapBiomas — Cobertura 30 m](https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/).

### 2.2 Pacote local lido

Todos os 12 arquivos em `E:\mapbiomas11` foram inventariados e lidos/inspecionados:

| Arquivo | Papel no pipeline |
|---|---|
| `brazil_coverage-col11_2025.tif` | raster categórico Brasil 2025; dado-fonte principal |
| `legend_code_mapbiomas_brazil_collection_11.csv` | legenda terminal oficial, 33 classes, PT-BR/EN e cores |
| `CodigosDeLegenda_LegendCodes_MapBiomas_Brazil_Collection11_PDF.pdf` | hierarquia, códigos agregados e terminais |
| `DESCRICAO_DA_LEGENDA_MAPBIOMAS_BRASIL_COLECAO_11_PDF.pdf` | descrição detalhada por classe e bioma |
| `Factsheet-Colecao-11-12082026-1.pdf` | escopo 1985–2025, método, licença e resultados oficiais |
| `COVERAGE_QGIS_COL11_PT_EN.zip` e QMLs extraídos | estilos de cobertura PT-BR/EN |
| `legend_code_n_classes_*` e respectivo ZIP | legenda de número de classes; referência de outro produto |
| `legend_code_n_changes_*` e respectivo ZIP | legenda de número de mudanças; referência de outro produto |

Os arquivos de “número de classes” e “número de mudanças” não descrevem o raster anual de cobertura de 2025 e não participam do cálculo deste MVP. Devem ser preservados na Bronze como parte do pacote, mas não carregados como legenda do raster `coverage`.

### 2.3 Metadados reais do GeoTIFF

| Propriedade | Valor confirmado |
|---|---|
| Tamanho | 763.286.575 bytes |
| SHA-256 | `0035BF482E8E6FAB6C623275485D2891ED19713B5F258D194A7BB0764D8392DB` |
| Driver | GeoTIFF |
| Dimensões | 154.470 × 146.501 pixels |
| Bandas | 1 |
| Tipo | `uint8` / Byte |
| CRS | EPSG:4326 — WGS 84 |
| Pixel | `0,0002694945852359°` × `-0,0002694945852359°` |
| Origem | `(-74,0209997484; 5,4230395387)` |
| Extensão | oeste `-74,0210`, leste `-32,3922`, norte `5,4230`, sul `-34,0582` |
| Blocos | 256 × 256 |
| Compressão | LZW, predictor 2 |
| NoData embutido | ausente |
| Checksum GDAL | 52096 |

O raster contém as 33 classes terminais presentes no CSV: `3, 4, 5, 6, 7, 9, 11, 12, 15, 20, 21, 23, 24, 25, 29, 30, 31, 32, 33, 35, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75, 77, 84, 91`, além do valor `0`.

O valor `0` não aparece na legenda oficial das 33 classes e ocupa principalmente o retângulo exterior ao território mapeado. Neste pipeline ele deve ser normalizado como **NoData**, nunca contado como classe de uso ou cobertura.

## 3. Classes e legenda

### 3.1 Classes terminais do raster

| Código | Nome PT-BR | Observação |
|---:|---|---|
| 3 | Formação Florestal | classe terminal |
| 4 | Formação Savânica | classe terminal |
| 5 | Mangue | classe terminal |
| 6 | Floresta Alagável | classe terminal |
| 7 | Savana Alagada (beta) | nova/beta; sobretudo Pantanal |
| 9 | Silvicultura | classe terminal |
| 11 | Campo Alagado e Área Pantanosa | classe terminal |
| 12 | Formação Campestre | relevante em SC |
| 15 | Pastagem | classe terminal |
| 20 | Cana | classe terminal |
| 21 | Mosaico de usos | classe terminal |
| 23 | Praia, Duna e Areal | classe terminal |
| 24 | Área Urbanizada | classe terminal |
| 25 | Outras Áreas não Vegetadas | classe terminal |
| 29 | Afloramento Rochoso | classe terminal |
| 30 | Mineração | classe terminal |
| 31 | Aquicultura | classe terminal |
| 32 | Apicum | classe terminal |
| 33 | Rio, Lago e Oceano | classe terminal |
| 35 | Dendê | classe terminal |
| 39 | Soja | classe terminal |
| 40 | Arroz | classe terminal |
| 41 | Outras Lavouras Temporárias | classe terminal |
| 46 | Café | classe terminal |
| 47 | Citrus | classe terminal |
| 48 | Outras Lavouras Perenes | classe terminal |
| 49 | Restinga Arbórea | relevante no litoral de SC |
| 50 | Restinga Herbácea ou Arbustiva | relevante no litoral de SC |
| 62 | Algodão (beta) | classe beta |
| 75 | Usina Fotovoltaica | classe terminal |
| 77 | Formação Herbáceo Arbustiva | nova na Coleção 11 |
| 84 | Marisma (beta) | em 2025 mapeada apenas no Pampa |
| 91 | Parque eólico (beta) | nova/beta |

### 3.2 Hierarquia

O PDF oficial também apresenta nós agregados, como Floresta, Vegetação Herbácea e Arbustiva, Agropecuária, Área Não Vegetada, Corpos d'Água, Agricultura e Lavouras. Esses códigos agregados não aparecem como pixels no GeoTIFF 2025 inspecionado.

O pipeline deve:

1. usar o CSV oficial como contrato das classes terminais aceitas;
2. manter a hierarquia em uma dimensão versionada para agregações futuras;
3. nunca substituir um código terminal por agregado na Silver;
4. calcular o nível terminal e derivar totais hierárquicos somente na Gold/consulta;
5. preservar os indicadores de classe beta.

Não se deve parsear PDF durante a execução da DAG. A implementação deve criar um seed machine-readable revisado e testado, derivado uma única vez dos materiais oficiais, com a proveniência registrada.

### 3.3 Inconsistências encontradas nos materiais

- O CSV terminal escreve `Rocky Outrcrop`; o QML/PDF usa `Rocky Outcrop`.
- O CSV terminal escreve `Coffe`; o QML/PDF usa `Coffee`.
- Há pequenas diferenças de capitalização e singular/plural entre CSV, QML e PDF.
- O QML chamado PT-BR do produto “número de mudanças” contém rótulos em inglês e tem o mesmo CRC do QML EN.
- A representação textual da hierarquia no PDF possui numeração que deve ser revisada ao gerar o seed, especialmente no ramo de lavouras perenes.

Essas diferenças não alteram `class_id`. A dimensão de legenda deve guardar `source_name_*` exatamente como recebido e `display_name_*` normalizado, com uma tabela de correções explícita e testada. O CSV terminal é autoritativo para os códigos aceitos; QML/PDF orientam apresentação, hierarquia e descrição.

## 4. Topologia proposta da DAG

```text
start
  -> discover_source_package
  -> checksum_and_validate_package
  -> publish_bronze_atomically
  -> validate_bronze_raster
  -> load_sc_boundary
  -> clip_sc_preserving_native_grid
  -> validate_silver_raster
  -> publish_silver
  -> load_active_aois
  -> compute_zonal_statistics
  -> [publish_gold_raster, publish_gold_statistics, load_postgres]
  -> write_quality_summary
  -> end
```

A DAG permanece fina. Toda regra de raster, legenda, área, AOI, idempotência e publicação pertence ao `MapbiomasPipelineService`.

## 5. Aquisição e Bronze

### 5.1 Ingestão inicial

O diretório externo deve ser informado por configuração, por exemplo `MAPBIOMAS_SOURCE_DIR`, apenas para o bootstrap. O service valida a lista de arquivos, calcula SHA-256 e publica em uma partição temporária. A partição só é renomeada para o destino definitivo depois de todos os arquivos serem copiados e revalidados.

Layout proposto:

```text
bronze/mapbiomas_lulc/
  collection=11/
    version=v1/
      year=2025/
        import_id=<uuid>/
          source/
            brazil_coverage-col11_2025.tif
            legend_code_mapbiomas_brazil_collection_11.csv
            CodigosDeLegenda_LegendCodes_MapBiomas_Brazil_Collection11_PDF.pdf
            DESCRICAO_DA_LEGENDA_MAPBIOMAS_BRASIL_COLECAO_11_PDF.pdf
            Factsheet-Colecao-11-12082026-1.pdf
            COVERAGE_QGIS_COL11_PT_EN.zip
          reference/
            legend_code_n_classes_mapbiomas_brazil_collection_11.csv
            legend_code_n_changes_mapbiomas_brazil_collection_11.csv
            ESTILO_QGIS_N_CLASSES_COL11_PT-BR_EN.zip
            ESTILO_QGIS_N_CHANGES_COL11_EN_PT-BR.zip
          manifest.json
```

Esse inventário contém os dez arquivos-fonte originais disponíveis em `E:\mapbiomas11`. Os dois QMLs existentes na subpasta `COVERAGE_QGIS_COL11_PT_EN` não serão copiados novamente para a Bronze, porque já estão preservados byte a byte dentro de `COVERAGE_QGIS_COL11_PT_EN.zip`. Eles serão extraídos de forma segura e reprodutível para a Gold. Os materiais `n_classes` e `n_changes` não participam do cálculo de cobertura 2025, mas permanecem em `reference/` por fazerem parte do pacote oficial disponibilizado.

A cópia inicial será feita de `E:\mapbiomas11` para o path gerenciado pelo `PipelineConfig`, que no desenvolvimento local resolve por padrão para `airflow/data/bronze`. A publicação deve usar staging na mesma raiz, cópia por streaming, SHA-256 antes/depois e rename atômico. O path externo não será persistido como dependência operacional.

### 5.2 Manifesto Bronze

Campos obrigatórios:

- `schema_version`;
- `domain=mapbiomas_lulc`;
- `product=coverage_30m`;
- `collection=11`;
- `version=v1`;
- `year=2025`;
- `published_date=2026-08-12`;
- `source_kind=official_download`;
- nomes, tamanhos e SHA-256 de todos os arquivos;
- metadados GDAL do raster;
- checksum da legenda;
- quantidade esperada de classes terminais;
- data/hora UTC de ingestão;
- licença e citação;
- versão do pipeline.

O manifesto não deve depender do nome do arquivo para identificar coleção e ano; esses valores devem ser validados contra configuração e conteúdo do pacote.

### 5.3 Idempotência da Bronze

Chave lógica: `product + collection + version + year + raster_sha256 + legend_sha256`.

- Mesmo conjunto de checksums: replay bem-sucedido, sem duplicar partição.
- Mesmo ano/versão com checksum diferente: nova revisão de fonte, bloqueada para aprovação ou publicada como novo `source_revision`.
- Arquivo incompleto ou checksum divergente: rejeição antes da Bronze definitiva.

## 6. Validação raster

Antes do recorte, validar:

1. driver GeoTIFF;
2. exatamente uma banda;
3. dtype inteiro `uint8`;
4. EPSG:4326;
5. dimensões e transform esperados para este source revision;
6. bloco 256 × 256 e compressão legível;
7. ausência de corrupção por leitura/checksum GDAL;
8. valores únicos pertencentes a `{0} ∪ classes_terminais`;
9. todas as classes desconhecidas como erro fatal de contrato;
10. valor 0 tratado como NoData lógico;
11. coerência entre CSV e QML de cobertura;
12. 33 classes terminais no contrato, mesmo que um recorte local não contenha todas.

Não bloquear o recorte de SC se uma classe oficial não ocorrer em SC; a validação exige que todo valor encontrado seja conhecido, não que todas as classes nacionais apareçam no estado.

## 7. Limite de Santa Catarina

O limite oficial está disponível em `X:\base_teste` e foi confirmado pelo responsável como **IBGE 2025**. A validação de `limites_SC.geojson` encontrou:

- uma feature, código UF 42 e nome Santa Catarina;
- CRS SIRGAS 2000/EPSG:4674;
- geometria `MultiPolygon` válida, não vazia, com 79 partes;
- bounds `(-53,8371493; -29,3550659; -48,3278753; -25,9557228)`;
- área declarada de 95.736,1 km²;
- área elipsoidal GRS80 de 95.736,09391 km²;
- SHA-256 do GeoJSON `A0B6168656478B9EB85219C50CB7878FFC5868B8F9A8BF6309A22F6694DE668E`.

O pacote Boundary Bronze deve preservar somente os arquivos `limites_SC`, sem copiar as UCs de teste existentes no mesmo diretório:

```text
bronze/reference_boundaries/
  source=ibge/edition=2025/code=42/import_id=<uuid>/
    source/
      limites_SC.geojson
      limites_SC.shp
      limites_SC.shx
      limites_SC.dbf
      limites_SC.prj
      limites_SC.cpg
      limites_SC.qmd
    manifest.json
```

O manifesto deve registrar fonte IBGE, edição 2025, data de ingestão, código UF 42, CRS, contagem, tipo geométrico, bounds, área declarada/calculada, bytes e SHA-256 de cada arquivo. O GeoJSON será a entrada canônica do recorte; o conjunto Shapefile e o QMD ficam preservados como pacote-fonte e metadados. A geometria não será embutida como coordenadas hard-coded na DAG. Bbox pode otimizar a leitura, mas a Silver deve ser mascarada pela geometria real.

## 8. Silver — recorte de Santa Catarina

### 8.1 Regra de processamento

1. Transformar somente o vetor de SC para EPSG:4326.
2. Calcular a janela raster mínima que envolve SC, alinhada aos pixels da fonte.
3. Ler apenas os blocos dessa janela.
4. Aplicar máscara vetorial na grade original.
5. Manter valores terminais sem reclassificação.
6. Preencher exterior de SC com NoData 0.
7. Escrever GeoTIFF tiled/compressed com transform derivado da janela original.

Não reprojetar o raster Brasil para EPSG:4674 ou EPSG:31982 na Silver. Reprojetar um raster categórico altera a grade e pode modificar a contagem de classes. Se qualquer reprojeção for criada para visualização, ela será produto derivado separado, sempre com `nearest neighbour`.

### 8.2 Layout

```text
silver/mapbiomas_lulc/
  collection=11/version=v1/year=2025/run_id=<run>/
    mapbiomas_sc_coverage_col11_2025.tif
    legend_terminal_col11.csv
    legend_hierarchy_col11.csv
    raster_metadata.json
```

### 8.3 Critérios de aceite da Silver

- grade perfeitamente alinhada à fonte Brasil;
- CRS EPSG:4326;
- dtype Byte;
- NoData explícito 0 no output;
- apenas classes oficiais e NoData;
- exterior de SC sem classes;
- interior de SC com cobertura compatível;
- checksum, extensão, transform e contagens por classe registrados;
- nenhuma interpolação bilinear/cúbica.

## 9. AOIs: UC, ZA e Buffer de Abrangência

### 9.1 Seleção

Para o processamento inicial:

- todas as UCs ativas;
- todas as ZAs oficiais ativas;
- todas os Buffers de Abrangência ativos;
- a constraint de precedência deve garantir que uma UC não possua ZA e Buffer de Abrangência simultaneamente ativos.

Cada AOI é calculada separadamente:

- `UC`: geometria vigente da UC;
- `ZA`: `geometria da ZA oficial ativa − geometria da UC`;
- `BUFFER_ABRANGENCIA`: `geometria do Buffer de Abrangência ativo − geometria da UC`.

A zona de análise é sempre o entorno exclusivo. A diferença é aplicada pelo pipeline mesmo se a geometria persistida da ZA/Buffer de Abrangência tocar ou sobrepuser a UC. Esse requisito evita dupla contagem entre as três categorias publicadas: UC, ZA e Buffer de Abrangência.

### 9.2 Geometrias pontuais

Uma UC cadastrada como `Point` não possui área e não deve receber estatística de cobertura para o AOI `UC`. O pipeline registra `AOI_NON_AREAL` no data quality e calcula normalmente o Buffer de Abrangência poligonal associada. Não criar buffer artificial para representar área da UC pontual.

### 9.3 Histórico e identidade

Cada resultado deve registrar:

- tipo e id do AOI;
- `id_uc` pai;
- `id_za_oficial` ou `id_buffer_abrangencia`, quando aplicável;
- número da versão geométrica ou hash da geometria;
- período de vigência;
- status ativo no instante do cálculo;
- checksum do raster.

Assim, uma alteração cadastral gera uma nova versão das estatísticas sem sobrescrever o resultado calculado para a geometria anterior.

## 10. Cálculo de área por classe

### 10.1 Resolução nominal de 30 m e área efetiva da célula

O produto é corretamente descrito como **Cobertura 30 m** porque deriva da série Landsat de resolução espacial nominal de 30 m. Isso não significa, porém, que cada célula do arquivo entregue tenha área constante de 900 m². O GeoTIFF está armazenado na grade geográfica WGS84/EPSG:4326, com passo angular de `0,0002694945852359°`. O próprio MapBiomas explica que associar cada pixel a 900 m² é uma aproximação e recomenda considerar a distorção da grade em latitude, usando `ee.Image.pixelArea()` no Earth Engine ou método métrico equivalente fora dele: [FAQ oficial — Qual a área de um pixel?](https://brasil.mapbiomas.org/faq/?tema=conceito).

Uma medição manual no QGIS próxima de 30 m é, portanto, compatível com a resolução nominal do produto. Na latitude de Santa Catarina, o passo norte–sul é aproximadamente 29,86 m, enquanto o passo leste–oeste diminui com a latitude:

| Latitude de referência | Lado leste–oeste | Lado norte–sul | Área aproximada |
|---:|---:|---:|---:|
| 25,9° S | 27,00 m | 29,86 m | 0,08062 ha |
| 27,5° S | 26,63 m | 29,86 m | 0,07952 ha |
| 29,4° S | 26,16 m | 29,87 m | 0,07814 ha |

As dimensões foram calculadas no elipsoide WGS84 a partir do transform real do GeoTIFF. A forma como o QGIS exibe a medição também depende do CRS do projeto, da configuração elipsoidal, do segmento escolhido e do arredondamento.

Assim, na latitude de Santa Catarina, a área geodésica aproximada de uma célula varia de:

- 0,08062 ha em torno de 25,9° S;
- 0,07952 ha em torno de 27,5° S;
- 0,07814 ha em torno de 29,4° S.

Multiplicar toda contagem por 0,09 ha superestimaria as áreas em SC em aproximadamente 11,6% a 15,2%, antes mesmo de considerar pixels de borda.

### 10.2 Método recomendado

**Máscara na grade nativa + área elipsoidal por célula/linha**, equivalente em finalidade ao uso de `pixelArea()` recomendado pelo MapBiomas, sem reprojetar nem reamostrar o raster categórico.

Para cada AOI:

1. transformar a geometria para EPSG:4326;
2. obter sua janela alinhada no raster Silver;
3. criar uma máscara `geometry_mask` com política de centro do pixel (`all_touched=False`);
4. agrupar pixels incluídos por classe e por linha;
5. calcular com `pyproj.Geod` a área elipsoidal da célula naquela linha;
6. somar `pixel_count_linha × cell_area_linha` por classe;
7. converter m² para hectares;
8. calcular total classificado, NoData e cobertura do AOI.

Como a grade é regular em longitude/latitude, todas as células da mesma linha têm a mesma área geodésica. Isso permite precisão adequada sem calcular um polígono geodésico para cada pixel individual.

O método só será aceito para carga definitiva após uma validação independente em duas escalas:

1. teste numérico de células conhecidas em diferentes latitudes, comparado com cálculo elipsoidal de referência;
2. total por classe e total classificado de Santa Catarina comparados com a estatística oficial da Coleção 11, com tolerância e diferenças de borda explicitamente documentadas.

Se a reconciliação estadual exceder a tolerância definida no teste, a carga deve falhar. Nesse caso, o método será confrontado com uma execução de referência baseada em `ee.Image.pixelArea()` antes de qualquer publicação no PostGIS.

### 10.3 Política de borda

Proposta MVP: incluir a célula quando o centro do pixel estiver dentro do AOI (`all_touched=False`). Essa política é determinística, evita superestimação sistemática em polígonos estreitos e deve constar em cada resultado.

Para AOIs muito pequenas, calcular também métricas de incerteza:

- área vetorial geodésica do AOI;
- área total dos pixels selecionados;
- diferença absoluta e relativa;
- quantidade de pixels de borda, quando viável.

Cobertura fracionária de pixels é uma evolução, não requisito do primeiro incremento.

### 10.4 Classes ausentes e NoData

- Produzir somente linhas para classes presentes (`area_ha > 0`) na tabela fato.
- Disponibilizar a dimensão completa com as 33 classes para consultas que precisem exibir zero.
- Não produzir uma linha de classe 0.
- Registrar NoData separadamente no summary de cobertura.

## 11. Gold raster

Layout:

```text
gold/mapbiomas_lulc/
  collection=11/version=v1/year=2025/run_id=<run>/
    raster/
      mapbiomas_sc_coverage_col11_2025_cog.tif
      ESTILO_QGIS_COL11_PT.qml
      ESTILO_QGIS_COL11_EN.qml
      legend_terminal_col11.csv
```

O raster Gold deve ser um Cloud Optimized GeoTIFF validado, com:

- mesma grade e valores da Silver;
- tiles e overviews por vizinho mais próximo;
- NoData 0;
- metadados de coleção, versão, ano e source checksum;
- QMLs/legenda disponibilizados como sidecars.

Silver e Gold podem conter pixels equivalentes, mas possuem papéis distintos: Silver é o recorte técnico validado; Gold é o pacote publicado para consumo, visualização e futura distribuição em S3.

O COG de Santa Catarina é um produto final obrigatório, não apenas um intermediário do cálculo. Ele deve permanecer disponível na Gold com chave estável e versionada, ser abrível diretamente no QGIS/ArcGIS e possuir registro correspondente no catálogo PostGIS. A disponibilidade externa pode ser implementada por endpoint de download/streaming da API no filesystem local e, futuramente, por URL pré-assinada do S3; o banco não deve expor um path Windows ao consumidor.

## 12. Gold tabular

Produzir Parquet e CSV:

```text
statistics/mapbiomas_area_by_aoi_class_col11_2025.parquet
statistics/mapbiomas_area_by_aoi_class_col11_2025.csv
```

Contrato mínimo por linha:

- `collection`;
- `version`;
- `year`;
- `raster_sha256`;
- `aoi_type` (`UC`, `ZA`, `BUFFER_ABRANGENCIA`);
- `id_uc`;
- `id_za_oficial`;
- `id_buffer_abrangencia`;
- `aoi_geometry_version`;
- `aoi_geometry_sha256`;
- `class_id`;
- `class_name_pt_br`;
- `class_name_en`;
- `class_hex_color`;
- `class_is_beta`;
- `pixel_count`;
- `area_ha`;
- `aoi_area_ha_geodesic`;
- `classified_area_ha`;
- `coverage_ratio`;
- `area_method=geodesic_native_grid_v1`;
- `boundary_policy=pixel_center`;
- `run_id` e timestamps UTC.

## 13. PostgreSQL/PostGIS

### 13.1 Evolução recomendada

Não vetorizar as classes e não preencher `geom` de `mapbiomas_clip` com milhões de polígonos. O dado espacial autoritativo permanece raster; o PostGIS guarda catálogo e estatísticas.

Proposta mínima:

1. `mapbiomas_raster_asset`: coleção, versão, ano, escopo Brasil/SC, URI/chave, checksum, CRS, transform, dimensões e NoData.
2. `mapbiomas_legend_class`: legenda versionada, hierarquia, nomes, cor, nível, terminal/beta.
3. evoluir `mapbiomas_clip` como tabela fato de estatística por AOI/classe.

Isso produz dois contratos independentes e vinculados por `id_raster_asset`:

- o **raster Gold de SC**, catalogado no PostGIS por metadados, checksum e chave relativa para download;
- as **estatísticas de área por classe e AOI**, armazenadas relacionalmente e consultáveis por UC, ZA, Buffer de Abrangência, ano, coleção e classe.

Campos adicionais necessários em `mapbiomas_clip`:

- `id_raster_asset`;
- `aoi_type`;
- `aoi_geometry_version` e `aoi_geometry_sha256`;
- `pixel_count`;
- `area_method`;
- `boundary_policy`;
- `coverage_ratio`;
- `run_id`;
- `source_checksum`.

O DDL atual será reconstruído no baseline de desenvolvimento: `geom` e `ds_referencia_raster` não pertencem à tabela fato de áreas, pois as classes não serão vetoradas e a referência deve ser uma chave estrangeira para o asset. Além dos campos acima, a tabela deve registrar `aoi_area_ha_geodesic`, `classified_area_ha` e a versão do método de área; `id_classe` passa a referenciar a legenda versionada, sem copiar `nm_classe` como fonte de verdade.

Constraint natural proposta:

```text
id_raster_asset
+ aoi_type
+ id_uc
+ coalesce(id_za_oficial, 0)
+ coalesce(id_buffer_abrangencia, 0)
+ aoi_geometry_sha256
+ class_id
+ area_method_version
```

### 13.2 Integridade

- `UC` exige `id_uc` e zona nula.
- `ZA` exige `id_uc` + `id_za_oficial`, com `id_buffer_abrangencia` nulo.
- `BUFFER_ABRANGENCIA` exige `id_uc` + `id_buffer_abrangencia`, com `id_za_oficial` nulo.
- classe deve existir na legenda da coleção/versão do asset.

### 13.3 Opções para disponibilizar o raster pelo PostGIS

O PostGIS oferece `postgis_raster`; o executável `raster2pgsql` não faz parte da imagem do banco nem do RDS. Existem quatro opções:

| Opção | Funcionamento | Avaliação |
|---|---|---|
| Catálogo + COG Gold | PostGIS guarda metadados, footprint, checksum e chave; API/storage entrega o arquivo | obrigatório |
| Raster PostGIS in-db | pixels são carregados em tiles na coluna `raster` | **aprovado como camada adicional de consulta** |
| Raster PostGIS out-db | coluna `raster` referencia o COG externo | viável, porém exige mount estável no DB, habilitar out-db/GDAL e paths válidos para o servidor |
| `bytea`/large object | GeoTIFF inteiro vira blob genérico | não recomendado; perde o modelo raster espacial e dificulta streaming/manutenção |

A arquitetura aprovada é **COG canônico na Gold + catálogo + Raster PostGIS in-db tiled + entrega pela camada de storage/API**. A tabela `mapbiomas_raster_asset` deve conter ao menos:

- `id_raster_asset`, coleção, versão, ano e escopo `SC`;
- `storage_key` relativa, nunca path local absoluto;
- SHA-256, bytes, media type e nome sugerido para download;
- CRS, transform, largura, altura, bandas, dtype, NoData, compressão e overviews;
- footprint `geometry(Polygon, 4674)` ou envelope equivalente para busca espacial;
- status de publicação, `run_id` e timestamps.

Tabela raster proposta:

```text
mapbiomas_raster_tile
  id_raster_tile
  id_raster_asset
  tile_row
  tile_col
  rast raster
  created_at
```

Requisitos:

- habilitar `CREATE EXTENSION IF NOT EXISTS postgis_raster` no baseline;
- tiles regulares candidatos de 256 × 256, comparados com 128 e 512 no benchmark;
- padronizar tiles de borda quando necessário para constraints de bloco;
- uma banda `8BUI`, SRID 4326 e NoData 0;
- constraint única por asset/linha/coluna;
- índice GiST sobre `ST_ConvexHull(rast)`;
- `AddRasterConstraints` para SRID, escala, alinhamento, bloco, banda, pixel type e NoData;
- overviews categóricos produzidos sem interpolar classes e registrados no catálogo Raster;
- carga transacional/idempotente vinculada ao asset publicado;
- proibir alteração direta dos tiles por consumidores.

O tamanho definitivo do tile não será escolhido por preferência. O benchmark deve medir carga, espaço, consulta por ponto, interseção com UC/ZA/Buffer de Abrangência, renderização no QGIS, exportação GeoTIFF e backup/restore. `raster2pgsql` é a referência oficial de carga, mas sua integração deve ser comparada com uma carga controlada pelo service; em ambos os casos, argumentos, erros, transação e idempotência precisam ser observáveis.

### 13.4 Experiência para pesquisadores e QGIS

O produto deve permitir:

1. baixar o COG + QML e abrir sem banco;
2. conectar o QGIS ao PostgreSQL/PostGIS e adicionar a cobertura Raster de SC;
3. aplicar a legenda oficial de 33 classes;
4. consultar o valor/classe em um ponto;
5. filtrar coleção, ano e asset publicado;
6. consultar views de área por UC, ZA, Buffer de Abrangência e classe;
7. exportar um recorte para GeoTIFF sem alterar o original;
8. obter metadados, citação, coleção, ano e método de área.

Entregáveis de uso:

- QML PT-BR e EN para o COG e teste de aplicação ao Raster PostGIS;
- guia curto de conexão QGIS, download, simbologia e consultas;
- role PostgreSQL somente leitura para pesquisadores;
- views estáveis de catálogo, legenda e estatísticas;
- exemplos SQL de `ST_Value`, `ST_Intersects`, `ST_Clip` e exportação controlada;
- dataset de validação para conferir que QGIS, COG e PostGIS retornam a mesma classe.
- área e contagem não podem ser negativas.
- classe 0 é proibida na tabela fato.

## 14. Idempotência e reprocessamento

### 14.1 Execução completa

Primeira execução: Bronze Brasil 2025 → Silver SC → todas as UCs e zonas ativas → Gold/PostGIS.

### 14.2 Replay

Mesmo raster, AOI, versão geométrica, método e classe: `ON CONFLICT DO NOTHING` ou comparação determinística, sem duplicação.

### 14.3 Mudança cadastral

Atualização de implementação (13/09/2026): `DAG_ZA_BUFFER` inclui `DAG_MAPBIOMAS`
no encadeamento dirigido, aguardando sucesso ou falha até a carga PostGIS.
O manifesto Bronze identifica cada UC, incluindo identidades textuais. O snapshot
dirigido lê as geometrias já persistidas de UC e zona ativa pelos pais em transação
somente leitura; isso evita inferir novamente a ZA ou divergir do Buffer de Abrangência publicado.
Essa consulta cadastral é específica do fluxo dirigido; a entrada temática raster
continua proveniente do pacote Bronze/Silver/Gold anual.

Snapshots usam `aoi_snapshot/imports/import=<sha256(import_id)>` e estatísticas
Silver/Gold acrescentam a mesma partição à coleção/versão/ano. O campo `id_uc`
nesses snapshots é a chave interna PostGIS, indicada por
`identity_kind=postgis_id_uc` no manifesto. A carga verifica se a zona ativa ainda
corresponde à congelada. O fluxo processa todos os anos descobertos e requer o raster
anual já publicado, sem reconstruir sua tabela de tiles.
Testes automatizados complementam os ensaios concluídos com ZIP e GeoJSON reais.

Atualização (07/10/2026): a execução completa (sem `import_id`) segue o mesmo modelo.
Ela congela todas as UCs com `situacao = 'ATIVA'` e a zona ativa de cada uma (exatamente
uma ZA oficial ou um Buffer de Abrangência), lidas do PostGIS, inclusive as UCs cadastradas
pelo painel. Snapshot e estatísticas ficam em `runs/run=<sha256(run_id)>`: uma nova
tentativa da mesma execução reaproveita o que gravou, e uma execução posterior congela
o cadastro vigente naquele momento. O antigo snapshot `aoi_snapshot/version=1`, derivado
dos shapefiles legados da Bronze, deixou de ser usado: ele ignorava as UCs vindas da API.

Uma alteração de UC/ZA/Buffer de Abrangência não exige baixar ou republicar o raster. O fluxo atual recalcula os AOIs da importação para todos os anos disponíveis e reutiliza os assets anuais publicados.

Não adicionar `DAG_MAPBIOMAS` ao trigger da FastAPI nesta implementação sem aprovação específica. Primeiro comprovar processamento completo, replay e reprocessamento seletivo.

### 14.4 Nova coleção ou ano

Cada combinação coleção/versão/ano é um novo asset imutável. Coleção 12 não sobrescreve Coleção 11. Um GeoTIFF de outro ano passa pelo mesmo contrato, mas seus valores e legenda são validados contra a versão correspondente.

## 15. Data quality e observabilidade

### 15.1 Pacote/Bronze

- arquivos esperados/encontrados;
- bytes e SHA-256;
- coleção, versão, ano e data de publicação;
- status da cópia atômica.

### 15.2 Raster

- driver, banda, dtype, CRS, transform, dimensões, blocos e compressão;
- checksum GDAL e SHA-256;
- valores únicos e contagens por classe;
- pixels NoData;
- classes desconhecidas;
- integridade do COG e overviews.

### 15.3 SC e AOIs

- área classificada/NoData em SC;
- quantidade de UCs, ZAs e Buffers de Abrangência processados;
- Point UCs ignoradas com motivo;
- AOIs inválidos, vazios, fora da cobertura ou sem pixels;
- pixels e hectares por classe/AOI;
- soma das classes, área geodésica do AOI e diferença de borda;
- tempo e pico de memória por lote.

## 16. Estratégia de testes

### 16.1 Unitários

- parser/validação das 33 classes;
- classe 0 como NoData;
- classe desconhecida fatal;
- hierarquia e classes beta;
- cálculo geodésico por linha;
- máscara por centro do pixel;
- UC Polygon/MultiPolygon e Point;
- ZA e Buffer de Abrangência exclusivos derivados no processamento por `zona - UC`, com teste contra sobreposição residual;
- chave de idempotência e geometry hash.

### 16.2 Raster de teste

Criar raster EPSG:4326 pequeno, alinhado, com valores 0/3/15/24/33 e AOIs de resultado manualmente calculável. Validar:

- pixels interiores e de borda;
- variação de área por latitude;
- NoData;
- recorte preservando transform;
- Parquet/CSV e soma por classe.

### 16.3 Integração

- ingestão de pacote reduzido em Bronze temporária;
- recorte por limite SC simplificado;
- COG e QML sidecar;
- carga PostGIS e replay;
- reprocessamento de um único `id_uc`;
- falha antes da publicação não deixa partição definitiva.

### 16.4 Aceite com dados reais

- validar o SHA-256 do GeoTIFF fornecido;
- executar o recorte real de SC;
- comparar o total estadual por classe com a tabela oficial MapBiomas de estatísticas por UF da Coleção 11, admitindo diferença documentada de política de borda/método;
- revisar amostras no QGIS usando o QML oficial;
- conferir UCs/ZA/Buffer de Abrangência selecionadas com casos manuais.

## 17. Performance

- ler por janela/bloco; nunca carregar o raster Brasil inteiro em memória;
- recortar SC uma vez e reutilizar a Silver;
- processar AOIs em lotes, agrupados por janelas quando houver benefício medido;
- usar `numpy.bincount`/agregação vetorizada por linha e classe;
- limitar paralelismo para não multiplicar leitura do mesmo raster;
- armazenar apenas referências leves em XCom;
- usar cache de área geodésica por índice de linha do raster.

### 17.1 Stack geoespacial e runtime reproduzível

Auditoria do runtime em 30/08/2026 mostrou o mesmo conjunto nos containers scheduler, webserver e triggerer:

| Componente | Runtime atual |
|---|---:|
| Python | 3.8.19 |
| NumPy | 1.24.4 |
| GeoPandas | 0.13.2 |
| Shapely | 2.0.3 |
| PyProj / PROJ | 3.5.0 / 9.2.0 |
| Fiona / GDAL | 1.10.1 / 3.9.2 |
| Rasterio / GDAL | 1.3.11 / 3.9.2 |

O conjunto é funcional e `pip check` não encontrou dependências quebradas, mas não corresponde aos mínimos declarados no `requirements.txt`: o Compose instala pacotes sem versão por `_PIP_ADDITIONAL_REQUIREMENTS` a cada inicialização, e não instala o arquivo de requirements do repositório. A própria documentação do Airflow classifica esse mecanismo como adequado somente para testes e recomenda uma imagem customizada com dependências incorporadas para ambientes estáveis.

Stack recomendada para a implementação:

- **Rasterio/GDAL:** leitura por janela/bloco, máscara alinhada, metadados, recorte e escrita COG;
- **NumPy:** agregação vetorizada das classes e pesos de área;
- **PyProj/PROJ:** transformações e área/distância elipsoidal WGS84 com `Geod`;
- **Shapely:** validação, normalização e operações geométricas;
- **GeoPandas/Fiona:** acesso vetorial e integração com os formatos já usados pelo projeto;
- **exactextract:** candidato para cobertura fracionária eficiente e implementação de referência das estatísticas zonais de borda.

`exactextract` não será adicionado cegamente. A versão estável atual requer Python 3.9 ou superior, enquanto a imagem em execução usa Python 3.8. A avaliação deve ocorrer em uma imagem Airflow 2.8.4 com versão de Python explicitamente suportada e wheel binário disponível, comparando:

1. política por centro do pixel com Rasterio;
2. cobertura fracionária com `exactextract`;
3. área ponderada por uma grade auxiliar elipsoidal produzida com PyProj;
4. desempenho, memória e reconciliação com a estatística oficial de SC.

Não usar a aproximação esférica interna de uma biblioteca como substituta silenciosa do método elipsoidal aprovado. Se `exactextract` for adotado, a versão do método, a política de cobertura e o raster de pesos devem fazer parte do contrato e dos testes.

Antes da implementação funcional, criar uma imagem Airflow customizada e imutável, remover `_PIP_ADDITIONAL_REQUIREMENTS`, instalar um lock compatível com a mesma versão do Airflow e executar no build: imports geoespaciais, `pip check`, versões GDAL/PROJ, DAG import test e testes raster mínimos. As versões finais devem ser escolhidas pelo conjunto compatível aprovado no build, não por atualização independente de cada pacote.

## 18. Frequência

MapBiomas Cobertura é anual. O schedule diário atual das 04:00 deve ser substituído por execução manual/dataset-driven no MVP e, futuramente, por verificação anual de nova publicação. Para o pacote já presente, uma execução dirigida é suficiente.

Backfill 1985–2024: os GeoTIFFs são exportados do GEE e colocados em `airflow/data/raw/mapbiomas/tifs/`. **Alvo (pendente):** `DAG_MAPBIOMAS` sem `dag_run.conf` deve descobrir sozinha os anos presentes em `raw/mapbiomas/tifs/`, pular os já publicados (checksum vs Bronze / `mapbiomas_raster_asset.reference_year`) e processar só os novos, sem repetição — "só apertar play". Implementar via dynamic task mapping (`.expand()`) ou uma `DAG_MAPBIOMAS_BACKFILL` separada; manter `{"year": N}` para reprocessar um ano. A arquitetura já suporta novos anos sem alteração do contrato (parametrização feita em 30/08/2026, validada com 2025 e 1989).

## 19. Armazenamento e S3

No MVP, usar paths locais configurados pelo `PipelineConfig`. Manifestos e tabelas armazenam chaves POSIX relativas, não paths `E:\` ou `F:\`.

Na evolução S3:

- Bronze recebe o pacote oficial com as mesmas chaves;
- Silver/Gold recebem GeoTIFF/COG e tabelas;
- checksums e contratos permanecem iguais;
- COG permite leitura por range request;
- a regra de cálculo de área não muda.

## 20. Integração com a FastAPI

A FastAPI continua restrita ao ciclo cadastral de UC/ZA/Buffer de Abrangência e não recebe MapBiomas. Ela pode futuramente expor o status/resultado e disparar reprocessamento seletivo após alteração geométrica, mas nunca faz upload do raster nem grava estatísticas diretamente no PostGIS.

## 21. Arquivos a criar ou alterar após aprovação

### Existentes

- `airflow/dags/dag_mapbiomas.py`: substituir placeholder por orquestração fina.
- `airflow/dags/scripts_python/config.py`: source bootstrap, collection/version/year e boundary.
- `init_db.sql`: consolidar catálogo, legenda e evolução de `mapbiomas_clip` no baseline completo do primeiro deploy.
- `.env.example`: paths e opções sem dados sensíveis.
- `airflow/docker-compose.yaml` e requirements: garantir Rasterio/PyProj/Parquet no runtime.

### Novos

- `airflow/dags/scripts_python/mapbiomas_pipeline.py`: service de domínio.
- `airflow/dags/scripts_python/mapbiomas_legend.py`: contrato/hierarquia da legenda, somente se não couber no service sem perder clareza.
- `airflow/tests/test_mapbiomas_pipeline.py`.
- `airflow/tests/fixtures/mapbiomas/`: pacote/raster de teste e geometrias.
- DDL MapBiomas incorporado ao baseline de desenvolvimento; migrations incrementais serão exigidas somente após o primeiro deploy de produção.

Não é necessário criar `earth_engine_client.py`, sensor GEE ou dependências Google para este incremento.

## 22. Critérios de pronto

1. Pacote oficial publicado imutavelmente na Bronze.
2. GeoTIFF Brasil validado contra metadados e legenda.
3. Limite oficial de SC versionado.
4. Silver SC alinhada e sem reamostragem.
5. Gold COG utilizável no QGIS com estilo oficial.
6. Áreas geodésicas por classe para UC, ZA e Buffer de Abrangência.
7. Point UC tratada sem inventar área.
8. PostGIS idempotente e com histórico por geometria.
9. PostGIS Raster tiled registrado em `raster_columns`, indexado, restrito a leitura e equivalente ao COG.
10. QGIS abre COG e camada PostGIS com a mesma classe/simbologia em pontos de referência.
11. Benchmark de tiles, carga, consulta, armazenamento e backup/restore documentado.
12. Views por UC/ZA/Buffer de Abrangência e guia de uso para pesquisadores entregues.
13. DQ reconciliável e classes desconhecidas bloqueadas.
14. Testes unitários, raster de teste, integração e replay aprovados.
15. Comparação documentada com estatística oficial de SC.
16. Documentação e relatório do TCC atualizados com Coleção 11 e método real.

## 23. Pendências remanescentes

- aprovar centro do pixel no primeiro incremento ou cobertura fracionária com `exactextract`;
- decidir quando incluir reprocessamento MapBiomas no fluxo dirigido da API;
- **[crítica de eng. de dados]** subpasta `raw/mapbiomas/tifs/` só para GeoTIFFs anuais + split raster-anual/companheiros-da-coleção no `MapbiomasPipelineService`;
- **[crítica de eng. de dados]** companheiros da Coleção 11 copiados para a Bronze uma vez por coleção (`collection=11/version=1/_companions/`), não por ano;
- **[crítica de eng. de dados]** `DAG_MAPBIOMAS` sem config deve descobrir e processar só os anos pendentes de `raw/mapbiomas/tifs/` (sem repetição, checando Bronze/`mapbiomas_raster_asset`); `.expand()` ou `DAG_MAPBIOMAS_BACKFILL`.

## 24. Frentes incrementais de implementação

Cada frente termina com código, testes e atualização somente dos documentos afetados. Uma frente não começa acumulando mudanças não testadas da próxima.

### Frente 0 — correção transversal de partição UC/ZA/Buffer de Abrangência

1. derivar a geometria analítica de ZA/Buffer de Abrangência por `zona − UC`, sem reescrever silenciosamente a geometria histórica armazenada;
2. corrigir `ZaBufferPipelineService`, `ProdesPipelineService` e `MapbiomasAlertaPipelineService` para recortar fontes por cada categoria atingida;
3. garantir que uma fonte que atravesse UC e zona produza relações distintas, geometrias sem sobreposição e áreas separadas;
4. criar os testes de partição, precedência ZA/Buffer de Abrangência e regressão de replay;
5. antes de reprocessar dados existentes, apresentar contagens/áreas afetadas e solicitar autorização explícita.

### Frente 1 — fundação reproduzível, baseline e ingestão Bronze

1. criar a imagem Airflow geoespacial travada e seus smoke tests;
2. consolidar no `init_db.sql` o DDL aprovado: Raster, catálogo/legenda/tiles, tabela fato `mapbiomas_clip` e evolução de `firms_clip`;
3. implementar inventário, checksum, staging e publicação atômica;
4. copiar os dez arquivos MapBiomas e os sete arquivos `limites_SC` para suas partições Bronze;
5. validar schema vazio, replay idempotente, arquivo ausente, checksum divergente e falha durante cópia;
6. atualizar plano, contratos, testes e handoff apenas com o resultado comprovado.

Esta é a frente recomendada para começar: as decisões de borda, produtos Gold e schema foram aprovadas; a Frente 0 de partição UC/ZA/Buffer de Abrangência continua pré-requisito para qualquer cálculo por AOI.

### Frente 2 — Silver SC

Validar o raster Bronze, carregar o limite IBGE 2025, recortar SC na grade nativa e provar alinhamento, classes, NoData e idempotência. Não inclui ainda estatísticas por AOI nem banco.

Aprendizado da Frente 1: em bind mounts Windows, o `manifest.json` é o commit de publicação; diretórios sem manifesto não são consumíveis. A Silver seguirá esse contrato, sem depender de rename atômico de diretório.

**Concluída em 30/08/2026.** A DAG publicou o raster Silver `20.444 × 12.615` em EPSG:4326, com NoData `0`, LZW e seleção por centro do pixel. A origem da Silver está alinhada em deslocamento inteiro à grade Bronze (coluna 74.895, linha 116.435), preservando exatamente a resolução angular. O manifesto valida o raster, o limite IBGE e a legenda oficial estruturada de 33 classes; as 23 classes presentes em SC foram todas reconhecidas. O replay foi aprovado sem reescrita.

### Frente 3 — motor de áreas e AOIs

Implementar a geometria exclusiva de ZA/Buffer de Abrangência por `zona - UC`, comparar centro do pixel, cálculo elipsoidal e, se aprovado, cobertura fracionária. Validar em raster de teste, SC, UC, ZA e Buffer de Abrangência. Não carregar PostGIS antes da reconciliação oficial.

**Concluída em 30/08/2026 — somente até o Parquet Silver; a carga em `mapbiomas_clip` não foi feita nesta frente.** A DAG materializa o snapshot analítico exclusivamente a partir das fontes Bronze: UCs, ZAs oficiais associadas por interseção positiva ou adjacência de até 0,01 m, e Buffer de Abrangência de 3 km somente para UCs sem ZA. Todas as geometrias são limitadas ao limite IBGE 2025 de SC antes da estatística. O snapshot validado contém 22 AOIs (`11 UC`, `5 ZA`, `6 Buffers de Abrangência`) e conserva a exclusividade `UC`, `ZA − UC` e `Buffer de Abrangência − UC`.

O cálculo publicado em Silver usa máscara na grade nativa (`all_touched=False`) e área elipsoidal WGS84 por linha do raster com `pyproj.Geod`. O Parquet possui 214 agregados por `id_uc`, tipo de AOI e classe terminal, todos com área e contagem não negativas. O manifesto registra a política de borda, o método e quaisquer AOIs sem centro de pixel selecionado; na execução validada todos os 22 AOIs tiveram pixels válidos. Diferenças pequenas entre a área vetorial e a soma dos pixels são inerentes à política de centro do pixel e não constituem sobreposição.

Permanecem para a Frente 4A a publicação Gold, a reconciliação independente com a estatística oficial estadual da Coleção 11 e a definição formal de tolerância. A carga relacional de `mapbiomas_clip` e o seed de `mapbiomas_legend_class` permanecem para a Frente 4B. Cobertura fracionária continua uma evolução, não parte deste incremento.

### Frente 4A — Gold raster e estatísticas

Produzir COG, QML/legendas, Parquet/CSV estatístico, summaries DQ e replay de artefatos. A estatística oficial de SC é gate desta frente.

**Concluída em 30/08/2026 — somente artefatos de arquivo Gold; nada foi gravado no PostgreSQL/PostGIS.** O COG Gold foi publicado a partir da Silver e comparado pixel a pixel com a origem: preserva CRS, transform, dimensões, NoData e todos os valores categóricos. O artefato possui LZW, blocos de 512, pirâmide com `nearest`, legenda terminal e os estilos oficiais QGIS PT/EN. As estatísticas Gold foram publicadas em Parquet e CSV com 214 registros, checksum do COG, legenda, método e política de borda. As tabelas `mapbiomas_clip`, `mapbiomas_legend_class`, `mapbiomas_raster_asset` e `mapbiomas_raster_tile` continuam com 0 linhas.

A referência oficial `MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx` foi publicada em Bronze com checksum e usada na reconciliação estadual de 2025. A diferença total é `0,216859%` (20.666,34 ha), concentrada na classe 33, Rio/Lago/Oceano: 19.221,91 ha (`12,722642%` da classe), compatível com a diferença de recorte costeiro entre o limite IBGE 2025 adotado pelo projeto e o limite usado pelo MapBiomas. Excluída a classe 33, a diferença é 1.444,43 ha (`0,015401%`). O relatório não oculta a classe 33; ele publica simultaneamente os indicadores total e não aquático. A política aprovada aceita até `0,5%` no total estadual e `0,1%` fora da classe 33; ambos os critérios foram atendidos antes da publicação Gold.

### Frente 4B — carga PostGIS pós-Gold (relacional e raster) e experiência QGIS

Cobre as duas persistências MapBiomas que as Frentes 3 e 4A deixaram pendentes, além da experiência de consulta. Nada é considerado pronto até a publicação no banco ser validada.

**Fases A–C — fundação reproduzível (concluídas em 30/08/2026).**

- **A.** Bancos locais na imagem oficial `postgis/postgis:17-3.5`, sem customização — mesma versão de PostgreSQL/PostGIS do RDS de produção. A biblioteca GDAL usada por `ST_FromGDALRaster` já vem com o PostGIS; nenhuma ferramenta GDAL de linha de comando é necessária no servidor de banco. Imagem Airflow `protected-areas-sc-airflow:2.8.4-geo`, sem `_PIP_ADDITIONAL_REQUIREMENTS`.
- **B.** O driver GTiff exigido por `ST_FromGDALRaster`/`ST_AsGDALRaster` é configuração de servidor (`postgis.gdal_enabled_drivers` só pode ser alterado por superusuário): variável `POSTGIS_GDAL_ENABLED_DRIVERS=GTiff` nos containers locais e parâmetro `ENABLE_ALL` (padrão) no RDS. Raster out-db segue desabilitado (padrão do PostGIS 3). Round-trip `ST_AsGDALRaster('GTiff')`/`ST_FromGDALRaster` preserva valores, SRID e dimensões; `ST_Tile` e `AddRasterConstraints` → `raster_columns` funcionam. `AddRasterConstraints` não aceita `pg_temp` — usar `public`.
- **C.** `protected-areas-sc-db-main` recriado na imagem versionada com o volume nomeado preservado; `init_db.sql` não reexecutou. Integridade conferida (`uc=11`, `za=5`, `buffer=6`, `md5` de nomes idêntico), GUC persistido, Airflow sem erro de import.

**Fase D — carga pós-Gold, idempotente e validada no banco (concluída em 30/08/2026).**

Precedeu-a a parametrização multi-ano de `mapbiomas_pipeline.py`: `collection`/`version`/`year` vêm de `dag_run.conf` sobre os defaults de `PipelineConfig` (`MapbiomasDataset` + helpers de caminho); ~35 literais `2025`/`col11_2025`/`collection=11` removidos; os 7 estágios Silver/Gold de 2025 dão replay idêntico. O baseline `mapbiomas_clip` ganhou `collection_code`, `collection_version`, `reference_year`, `class_area_ha` e índice `idx_mapbiomas_clip_dataset`; a constraint aceita `coverage_ratio ≤ 1,05` (efeito de borda do centro de pixel).

Três novos estágios em `MapbiomasPipelineService` + tasks na DAG:

1. **`seed_legend_postgres` → `mapbiomas_legend_class`** — upsert de 41 classes (33 terminais + 8 agregados) do seed versionado `data/mapbiomas_legend_col11.json`; idempotente por `source_checksum`. Verificado: 41 inseridas, replay 41 unchanged.
2. **`load_raster_postgres` → `mapbiomas_raster_asset` + `mapbiomas_raster_tile`** — lê o COG Gold e grava janelas verbatim (`ST_FromGDALRaster`, sem reamostragem, sem reprojeção, sem padding; tiles de borda com tamanho natural), `AddRasterConstraints` (search_path fixo em `public`; sem `blocksize`), índice GiST. Transacional, idempotente por checksum do asset. Verificado: 1 asset PUBLISHED, 1000 tiles, `ST_Extent` = extensão do COG, `Σ` pixels dos tiles = `20444 × 12615` (sem gap/overlap), `ST_ValueCount` idêntico ao COG por classe, `ST_Value` num ponto do Parque da Serra do Tabuleiro retorna classe 3.

   *Correção 30/08/2026 (`instructions/AVALIE_RASTERS.md`):* a primeira versão criava overviews categóricos `o_{2,8,32}_mapbiomas_raster_tile` com padding de tile que inflava a extensão (aparência de deslocamento no QGIS) e os tornava confundíveis com o raster principal. Diagnóstico: o raster base sempre esteve a 30 m nativos e com classes intactas (o "pixel de ~1 km" era a tabela `o_32`, um overview de fator 32); a classe 91 é legítima do MapBiomas Coleção 11 em SC (562 pixels, presente na fonte e no COG). Overviews in-db foram removidos — não são necessários para a aplicação analítica e distorcem raster categórico.
3. **`load_area_statistics_postgres` → `mapbiomas_clip`** — 214 registros do Parquet Gold; resolve `uc_id` externo → `uc.id_uc`, `id_za_oficial`/`id_buffer_abrangencia` ativos, `id_legend_class`, `id_raster_asset` (pelo checksum do COG); `aoi_geometry_sha256` do snapshot; `ON CONFLICT DO NOTHING`. Verificado: 214 linhas (93 UC + 51 ZA + 70 Buffers de Abrangência), 0 FKs órfãs, `Σ class_area_ha = classified_area_ha` por AOI.

Testes: `airflow/tests/test_mapbiomas_pipeline.py` cobre a resolução de dataset por `conf`, o seed da legenda (hierarquia, pai de 48 = 36, cores), a contagem de tiles, o tiling como janela verbatim sem reamostragem/padding/novas classes e o índice de geometria de AOI (9 passam).

**Multi-ano validado (30/08/2026).** O raster in-db é **uma tabela física por ano**, `mapbiomas_raster_<ano>` — não há tabela unificada. `load_raster_postgres` grava os tiles direto em `mapbiomas_raster_<ano>` (drop+create a cada carga; PK, FK ao asset, índice GiST, `AddRasterConstraints`), catalogada em `mapbiomas_raster_asset`. Idempotência por `to_regclass` + contagem de tiles. Cross-ano é feito por `mapbiomas_clip` (`reference_year`), não por união de rasters. Um segundo ano (1989, GeoTIFF exportado do GEE, uma peça cobre SC, mesma grade WGS84) foi carregado via `dag_run.conf {"year": 1989}`: as 13 tasks com `state=success`; `mapbiomas_raster_asset` = 2; `mapbiomas_raster_1989`/`_2025` em `raster_columns` com `srid 4326`/`8BUI`/escala nativa (renderizam direto no QGIS); 1000 tiles/ano, `Σ` px = `20444×12615`, `ST_ValueCount` de cada ano idêntico ao seu COG; grades alinhadas; `mapbiomas_clip` = 409 (195 + 214); fingerprint de 2025 inalterado. A `mapbiomas_raster_tile` da versão anterior foi removida (redundância). Bugs de sombreamento `with rasterio.open(...) as dataset` em `build_silver`/`compute_area_statistics` (só disparavam em ano não-replay) corrigidos; `_publish_package` dá erro claro para Bronze sem manifesto; `_copy_and_verify` tem retry para `OSError` transitório.

**Fase E — benchmark (pendente):** tiles 128/256/512 comparados em carga, uso em disco, `ST_Value` por ponto, `ST_Intersects` com UC/ZA/Buffer de Abrangência, renderização QGIS, exportação GeoTIFF e backup/restore. Somente o desenho aprovado segue para a Frente 5.

**Fase F — experiência do pesquisador (pendente):** QML PT/EN aplicados às tabelas raster por ano, role read-only, views de catálogo/legenda/estatística, guia de conexão QGIS + exemplos SQL, dataset de equivalência COG↔tiles.

### Runbook — adicionar um ano MapBiomas

1. Colocar o GeoTIFF em `airflow/data/raw/mapbiomas/brazil_coverage-col11_<ANO>.tif` (nome exato). É a zona `raw` do projeto (`airflow/data/raw/`, no `.gitignore`; container `/opt/airflow/data/raw/mapbiomas/`, `MAPBIOMAS_SOURCE_DIR`). Os 9 arquivos companheiros da Coleção 11 e o XLSX oficial já estão nessa pasta e servem todos os anos; o limite IBGE fica em `airflow/data/raw/ibge/`. GeoTIFF do GEE vem em ~9 peças — renomear a peça sudeste (cobre SC inteira) para esse nome; a grade já está alinhada ao download oficial.
2. Disparar `DAG_MAPBIOMAS` sem filtro anual. Ela descobre o novo ano e processa todos os anos disponíveis; no fluxo API o disparo é automático.
3. Saída: asset/tabela para o ano novo e `mapbiomas_clip` por ano/AOI. Bronze/Silver/Gold permanecem particionadas por ano.
4. `reconcile_official_statistics` usa a coluna `y<ANO>` do XLSX oficial; falha se exceder a tolerância aprovada (`0,5%` total / `0,1%` fora da classe 33) — checagem legítima.

### Frente 5 — baseline SQL e PostGIS — concluída em 13/09/2026

Consolidar o modelo aprovado no `init_db.sql`, incluindo `postgis_raster`, catálogo, tiles e estatísticas. Recriar banco de desenvolvimento somente após aviso ao responsável, executar schema test em banco vazio e validar carga/idempotência. Nenhuma exclusão de banco será silenciosa.

### Frente 6 — DAG e integração operacional — concluída com E2E real

A DAG fina usa dynamic task mapping para todos os anos e preserva a identidade da importação. Os testes de grafo e service e os smokes ponta a ponta com ZIP e GeoJSON reais passaram.
