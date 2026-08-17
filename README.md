# X3M · Pipeline de Revenue Diario por Producto

Pipeline batch que ingiere **Products** y **Carts** de [DummyJSON](https://dummyjson.com/),
los persiste en Postgres y produce `product_daily_revenue`: revenue vendido por producto
y por fecha. Orquestado con Apache Airflow, transformado con dbt, corre 100% local con
Docker Compose — sin cloud, sin credenciales externas.

> **Estado actual (en desarrollo):** el Setup (Docker Compose, imágenes, esqueleto de
> carpetas) está completo y verificado. La Implementación (DAG, extracción, modelos dbt,
> dashboard) está en curso. Los comandos y queries de este README reflejan el diseño
> final y se van validando a medida que el código se completa.

## Arquitectura (resumen)

- **Postgres** (una instancia, dos bases): `x3m_dw` para datos de negocio (raw → staging → mart, vía dbt) y `airflow_meta` para metadata de Airflow.
- **Airflow** (`LocalExecutor`): DAG `product_daily_revenue_dag` — `extract_products` y `extract_carts` en paralelo → `load_raw` (upsert idempotente en tablas raw JSONB) → `dbt_build`.
- **dbt** corre instalado en la misma imagen que Airflow, en un venv aislado, invocado desde el DAG. Modelos `staging` (parseo/tipado del JSONB) y `marts` (`product_daily_revenue`, incremental/merge).
- **Streamlit**: dashboard que lee directo de Postgres (`localhost:8501`).

El detalle y el razonamiento de cada decisión está en [DECISIONS.md](DECISIONS.md).

## Requisitos previos

- Docker Desktop instalado y corriendo (Docker Engine + Compose v2).

## Levantar todo (3 comandos)

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec airflow-scheduler airflow dags backfill product_daily_revenue_dag --start-date 2026-08-12 --end-date 2026-08-16 -y
```

1. `.env.example` trae credenciales locales de ejemplo, listas para usar (no hace falta editarlas para levantar el proyecto).
2. Buildea las imágenes y levanta Postgres, Airflow (init + webserver + scheduler) y Streamlit.
3. **Backfill explícito**: como la API es estática (ver nota abajo), no hay catchup automático — este comando carga 5 fechas pasadas para que haya histórico visible sin esperar corridas diarias reales. El DAG también corre solo, una vez por día, a partir de acá.

Esperá ~1-2 minutos después del `up` a que el healthcheck de Postgres y la migración de Airflow terminen antes de correr el backfill.

## Acceso a los servicios

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow UI | http://localhost:8080 | `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` en `.env` (default `admin` / `changeme`) |
| Streamlit | http://localhost:8501 | — |
| Postgres | `localhost:5432` | `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` en `.env` |

## Validación del resultado

```bash
docker compose exec postgres psql -U x3m_user -d x3m_dw -c "SELECT product_id, product_title, date, units_sold, revenue FROM product_daily_revenue ORDER BY date, revenue DESC LIMIT 20;"
```

(usá tus propios valores de `POSTGRES_USER`/`POSTGRES_DB` si cambiaste el `.env`)

## Nota importante sobre la fuente de datos

DummyJSON es una API **estática**: el contenido de Products y Carts no cambia entre
llamadas. Para simular cargas diarias, la columna `date` de `product_daily_revenue` **no
sale de los datos** — la genera el pipeline a partir de la fecha lógica de ejecución de
Airflow (`{{ ds }}` / data interval). Por eso es esperable ver el mismo `revenue` repetido
en distintas fechas: no es un bug, es el comportamiento documentado de la simulación.

## Versiones pinneadas

| Componente | Versión |
|---|---|
| Apache Airflow | 2.8.4 |
| Python | 3.11 |
| Postgres | 15.6-alpine |
| dbt-core / dbt-postgres | 1.7.13 |
| Streamlit | 1.32.0 |

Todas las dependencias (Python, dbt, Airflow) están fijadas en `requirements-airflow.txt`,
`dbt_project/requirements-dbt.txt`, `streamlit_app/requirements.txt` y el `Dockerfile`. Nada de `latest`.

## Estructura del repo

```
dags/                   DAG de Airflow y módulos de extracción/carga
dbt_project/            Modelos dbt (staging + marts)
streamlit_app/          Dashboard
tests/                  pytest (extracción, upsert)
init-db/                Bootstrap de la DB de metadata de Airflow
docker-compose.yml
Dockerfile              Airflow + dbt (venv aislado)
.env.example
DECISIONS.md
```
