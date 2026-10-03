FROM apache/airflow:2.8.4-python3.11

ARG AIRFLOW_VERSION=2.8.4
ARG PYTHON_VERSION=3.11

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends gdal-bin \
    && gdalinfo --version \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root airflow/requirements-airflow.txt /opt/airflow/requirements-airflow.txt

RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow==${AIRFLOW_VERSION}" \
    && pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /opt/airflow/requirements-airflow.txt \
    && pip check \
    && python -c "import fiona, geopandas, numpy, pandas, pyarrow, pyproj, rasterio, shapely; from pyproj import Geod; assert rasterio.__gdal_version__; assert pyproj.proj_version_str; assert Geod(ellps='WGS84').a > 0"

# Código do pipeline e do publicador do GeoServer embutidos: a imagem, identificada pelo commit,
# é a unidade de deploy. No desenvolvimento local, as pastas montadas sobrepõem este conteúdo.
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root geoserver /opt/airflow/geoserver
COPY --chown=airflow:root airflow/config /opt/airflow/config
