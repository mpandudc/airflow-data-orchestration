FROM apache/airflow:2.10.5

# The dbt project is pinned to an exact commit of mpandudc/dbt-clickhouse-marts,
# so a DAG run is reproducible and upgrades are an explicit ref bump.
ARG DBT_MARTS_REF=49d9f4977d3efb4827049cd70c175401ae2eccc2

USER root
ADD https://github.com/mpandudc/dbt-clickhouse-marts/archive/${DBT_MARTS_REF}.tar.gz /tmp/dbt-marts.tar.gz
# dbt lives in its own venv: its dependency pins must not fight Airflow's constraints.
RUN mkdir -p /opt/dbt-project \
    && tar -xzf /tmp/dbt-marts.tar.gz -C /opt/dbt-project --strip-components=1 \
    && rm /tmp/dbt-marts.tar.gz \
    && python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /opt/dbt-project/requirements.txt \
    && echo "${DBT_MARTS_REF}" > /opt/dbt-project/.ref
USER airflow
