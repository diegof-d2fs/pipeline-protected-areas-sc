# Estratégia de testes

## Pirâmide e ambientes

1. Unitários puros para parsing, regras de confiança, chaves, janelas, máscaras e segurança de caminhos.
2. Contratos do cliente FIRMS e da ingestão local MapBiomas, sem rede real no CI.
3. Integração com filesystem temporário, raster de teste e PostGIS efêmero.
4. DAG tests para import, grafo, schedules, retries e template/config.
5. Um smoke test opt-in contra fontes oficiais, sem executar por padrão e sem registrar segredos.

## FIRMS

- fixtures CSV mínimas de NOAA‑20/21, inclusive vazia e schema alterado;
- 200, 401/403, 429, 5xx, timeout e conteúdo truncado;
- confiança `l/n/h`, timestamps, coordenadas, FRP e duplicidade;
- UC/ZA/Buffer de Abrangência, bordas e uma detecção relacionada a múltiplas UCs;
- replay da mesma janela e backfill fragmentado.
- comparação das amostras locais NOAA-20/21, SNPP e MODIS no período comum de SC;
- backfill 2015–presente com SP preferencial, fallback local e reconciliação NRT→SP;
- bbox de SC na requisição e exclusão exata dos pontos fora do polígono estadual;
- séries e contagens separadas por sensor, evitando inflação silenciosa quando novas plataformas entram em operação.

## MapBiomas

- pacote reduzido da Coleção 11 com manifest/checksums e publicação Bronze atômica;
- GeoTIFF EPSG:4326 pequeno com transform conhecido, valor 0 e classes 3/15/24/33;
- validação das 33 classes, classe desconhecida fatal e classe ausente localmente permitida;
- recorte SC preservando grade, dtype e valores;
- dimensões e área elipsoidal de células conhecidas em diferentes latitudes, AOI, borda, MultiPolygon e cobertura parcial;
- UC Point não recebe área; Buffer de Abrangência poligonal recebe;
- COG com overviews nearest-neighbour e QML sidecar;
- ano/coleção/versão/checksum diferentes não colidem; replay idêntico não duplica;
- descoberta reúne raw, Bronze e assets publicados, ignora `year` herdado e falha se não houver ano;
- dynamic task mapping aguarda todos os anos e preserva `import_id`;
- comparação por classe e total do resultado estadual com a estatística oficial da UF SC, com tolerância explícita e falha quando excedida;
- teste de referência com `ee.Image.pixelArea()` quando a reconciliação estadual indicar divergência material.
- equivalência de valores, alinhamento, NoData e extensão entre COG e tiles PostGIS;
- benchmark reproduzível de tiles 128, 256 e 512 para carga, espaço, ponto, AOI e renderização QGIS;
- validação de `raster_columns`, constraints, índice espacial e overviews categóricos;
- replay, falha no meio da carga, publicação atômica e remoção segura de lote incompleto;
- backup/restore do raster in-db e exportação para GeoTIFF com classes preservadas;
- conexão QGIS somente leitura, aplicação do QML e consulta de classe em pontos conhecidos.

## Partição UC/ZA/Buffer de Abrangência

- geometria somente na UC gera somente relação `UC`;
- geometria somente na ZA/Buffer de Abrangência exclusiva gera somente a relação de zona correspondente;
- geometria que cruza UC e ZA/Buffer de Abrangência gera um recorte para cada categoria atingida, com áreas calculadas separadamente;
- nenhum recorte `UC`, `ZA` e `BUFFER_ABRANGENCIA` da mesma UC pode ter sobreposição de área positiva;
- a soma dos recortes deve equivaler à interseção do polígono-fonte com a união das áreas analíticas da UC e da zona;
- ZA ativa tem precedência sobre Buffer de Abrangência; Buffer de Abrangência não é usado para a UC que possuir ZA ativa;
- UC pontual não gera área, mas não autoriza incluir área fictícia na zona;
- testes cobrem `ZaBufferPipelineService`, `ProdesPipelineService`, `MapbiomasAlertaPipelineService`, `MapbiomasPipelineService` e `FirmsPipelineService`.

## Cleanup

- nenhum teste exclui diretório real;
- dry-run produz as mesmas contagens sem mutação;
- paths resolvidos devem ficar sob raiz temporária;
- CLI oficial é mockado nos unitários e exercitado contra metadata efêmero em integração.

## Evidência executada

- API: em 13/09/2026, lint aprovado e suíte completa na imagem de teste com **37 passed**.
- Pipeline completo: em 13/09/2026, a suíte obteve **39 passed e 6 skipped** na imagem do scheduler; `pip check` não encontrou dependências quebradas e o DagBag não registrou erros.
- FIRMS E2E: janela SP de cinco dias retomada após falha sem readquirir Bronze, uma relação UC inserida, replay publicado sem nova task e duas fontes NRT diárias concluídas com resposta vazia válida.
- Schema: `init_db.sql` aplicado duas vezes em banco vazio, incluindo criação idempotente das tabelas raster 1989/2025.
- E2E: lotes reais ZIP e GeoJSON com três UCs concluíram API, cinco DAGs, Medallion e PostGIS; o ciclo GeoJSON limpo é o import `a3814c09-5580-417d-89ca-f69c810347d4`.

## Critérios de aceite

- testes determinísticos, sem depender do relógio atual;
- nenhum segredo ou URL com chave em snapshot/log;
- contagens DQ reconciliáveis com inputs e relações;
- bancos principal e de mutação limpos recebem schema completo e constraints apenas pelos respectivos scripts iniciais do primeiro build;
- idempotência verificada em duas execuções;
- falha entre Silver/Gold/PostGIS não publica estado final enganoso.
- imagem Airflow é reproduzível, passa em `pip check` e não instala dependências dinamicamente no startup;
- versões GDAL/PROJ/GEOS e bindings Python são registradas pelo teste de ambiente;
- se `exactextract` for adotado, seu resultado de cobertura fracionária é comparado com fixture analítica e com o método por centro do pixel.
