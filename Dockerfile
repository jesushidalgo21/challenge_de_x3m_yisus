FROM apache/airflow:2.8.4-python3.11

# --- venv aislado solo para dbt: evita conflictos de dependencias con Airflow ---
USER root
RUN python3.11 -m venv /opt/dbt_venv && chown -R airflow: /opt/dbt_venv

USER airflow

# Deps propias para las tasks de extracción (requests, psycopg2), instaladas
# respetando el constraints file de Airflow para no romper su resolución.
# Vendorizado en el repo (constraints-2.8.4-python3.11.txt) para que el build
# no dependa de bajar el archivo de raw.githubusercontent.com en cada build
# (host propenso a rate limiting / 503 bajo carga).
COPY constraints-2.8.4-python3.11.txt /tmp/constraints-2.8.4-python3.11.txt
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
    --constraint /tmp/constraints-2.8.4-python3.11.txt \
    -r /tmp/requirements-airflow.txt

# dbt vive en su propio venv, sin tocar el entorno de Airflow.
# PIP_USER=true viene seteado globalmente en la imagen base de Airflow (para
# instalar sin root en su propio venv) y choca con este venv aislado, que no
# tiene user-site habilitado. Se pisa puntualmente acá.
COPY dbt_project/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN PIP_USER=0 /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
