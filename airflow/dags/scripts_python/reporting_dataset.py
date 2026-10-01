"""Dataset emitido por toda tarefa que publica dados lidos pela camada `reporting`.

A `DAG_GEOSERVER_SYNC` é agendada por ele: qualquer carga vetorial ou raster concluída
dispara a sincronização do GeoServer, sem acoplar as DAGs de carga ao serviço OGC.
"""

from __future__ import annotations

from airflow.datasets import Dataset

REPORTING_PUBLISHED = Dataset("postgres://protected-areas-sc-db-main/reporting")
