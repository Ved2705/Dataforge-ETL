# DataForge ETL Platform

> Production-grade Data Engineering Platform built for portfolio demonstration.
> Integrates **PySpark**, **Great Expectations**, **dbt**, and **Apache Airflow** in a single Flask-based web application.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     DataForge ETL Platform                    │
├────────────┬──────────────────┬───────────────┬─────────────┤
│  Frontend  │   Flask Backend  │  ETL Engine   │  Database   │
│  (HTML/JS) │  REST API        │               │  PostgreSQL │
│  Chart.js  │  Flask-Login     │  PySpark      │             │
│            │  Flask-SQLAlchemy│  Great Expect.│  Tables:    │
│            │  Flask-Bcrypt    │  dbt          │  users      │
│            │                  │  Airflow API  │  datasets   │
│            │                  │  Pandas       │  pipeline_  │
│            │                  │  PyArrow      │    runs     │
│            │                  │               │  tasks      │
│            │                  │               │  quality_   │
│            │                  │               │    checks   │
│            │                  │               │  transforms │
└────────────┴──────────────────┴───────────────┴─────────────┘
```

## ETL Pipeline Flow

```
 Upload File
     │
     ▼
┌─────────┐    ┌─────────┐    ┌──────────────────┐    ┌───────────┐
│ EXTRACT │ ──▶│ PROFILE │ ──▶│ QUALITY          │ ──▶│ TRANSFORM │
│ Pandas  │    │ Pandas  │    │ Great Expectations│    │ Pandas /  │
│ PySpark │    │         │    │ 15+ expectations  │    │ PySpark   │
└─────────┘    └─────────┘    └──────────────────┘    └─────┬─────┘
                                                             │
     ┌───────────────────────────────────────────────────────┘
     ▼
┌─────────┐    ┌─────────┐    ┌─────────────────┐
│   dbt   │ ──▶│  LOAD   │ ──▶│  AIRFLOW DAG    │
│  Model  │    │  CSV    │    │  Trigger & Poll  │
│  SQL    │    │ Parquet │    │  REST API        │
└─────────┘    └─────────┘    └─────────────────┘
```

## Database Schema

```sql
users              -- registered accounts + auth
login_sessions     -- login/logout audit trail
datasets           -- file upload metadata + profile JSON
pipeline_runs      -- ETL execution records (per dataset)
pipeline_tasks     -- individual task results within a run
quality_checks     -- Great Expectations validation results
transformations    -- dbt / transform operation records
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB RAM minimum (for Spark)

### Run with Docker Compose

```bash
git clone <repo>
cd dataforge-etl

# Start everything
docker compose up -d

# Wait ~60s for Airflow to initialize, then:
open http://localhost:5000    # DataForge UI
open http://localhost:8080    # Airflow UI (airflow/airflow)
open http://localhost:8081    # Spark Master UI
```

### Environment Variables

Copy `.env.example` to `.env` and configure:
```
DATABASE_URL=postgresql://dataforge:dataforge@postgres:5432/dataforge
SECRET_KEY=your-secret-key
AIRFLOW_API_URL=http://airflow-webserver:8080/api/v1
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
SPARK_MASTER=local[*]
```

### Local Development (no Docker)

```bash
pip install -r requirements.txt

# Requires running PostgreSQL
export DATABASE_URL=postgresql://localhost/dataforge

python -m backend.app
```

---

## API Reference

### Auth
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/register` | Create account |
| POST | `/api/auth/login` | Login |
| POST | `/api/auth/logout` | Logout |
| GET  | `/api/auth/me` | Current user |
| GET  | `/api/auth/sessions` | Login audit log |

### Datasets
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/datasets/upload` | Upload file → profile → GE check |
| GET  | `/api/datasets/` | List all datasets |
| GET  | `/api/datasets/<ds_id>` | Dataset detail |
| DELETE | `/api/datasets/<ds_id>` | Delete dataset |

### Pipeline
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/pipeline/run` | Run full ETL pipeline |
| GET  | `/api/pipeline/download/<run_id>` | Download cleaned CSV |
| GET  | `/api/pipeline/download/<run_id>?format=parquet` | Download Parquet |
| GET  | `/api/pipeline/dag-status/<run_id>` | Airflow DAG status |
| GET  | `/api/pipeline/spark-status` | Spark availability |

### History
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/history/runs` | All pipeline runs |
| GET | `/api/history/runs/<run_id>` | Single run detail |
| GET | `/api/history/quality` | Quality check history |
| GET | `/api/history/transformations` | Transform history |
| GET | `/api/history/stats` | Dashboard statistics |

---

## ETL Tools Integration

### PySpark
- Used for large file extraction and `spark_repartition` transform
- Lazy singleton session (`local[*]` in dev, `spark://spark-master:7077` in Docker)
- Fallback to Pandas if Spark unavailable

### Great Expectations
- Auto-generates expectation suite per dataset
- Checks: column existence, null rates, numeric ranges, uniqueness, cardinality
- Falls back to built-in lite quality engine if GE not installed
- Results stored in `quality_checks` table

### dbt
- Generates `.sql` model + `schema.yml` from cleaned DataFrame
- Uses `{{ source() }}` macros for proper dbt lineage
- Staging models cast + rename columns
- Mart models join pipeline metadata for analytics
- Run via `dbt run --select <model>` or triggered through Airflow

### Apache Airflow
- DAG: `dataforge_etl_pipeline`
- Schedule: on-demand (triggered via REST API)
- Tasks: extract → quality_check → transform → dbt_run → dbt_test → load → notify
- Triggered by Flask via `/api/v1/dags/{dag_id}/dagRuns`
- Status polled via `/api/pipeline/dag-status/<run_id>`

---

## Transform Steps

| Step | Tool | Description |
|------|------|-------------|
| `snake_case_columns` | pandas | Rename all columns to snake_case |
| `drop_duplicates` | pandas | Remove exact duplicate rows |
| `drop_missing` | pandas | Drop columns with >80% null rate |
| `fill_missing` | pandas | Fill nulls (median/mean/mode/ffill/constant) |
| `remove_outliers` | pandas | IQR or Z-score outlier removal |
| `cast_types` | pandas | Auto-cast numeric/datetime columns |
| `normalize` | pandas | Min-max normalize to [0,1] |
| `standardize` | pandas | Z-score standardize |
| `filter_rows` | pandas | Filter rows by column condition |
| `aggregate` | pandas | Group-by + aggregation |
| `derive_column` | pandas | Add computed column via expression |
| `spark_repartition` | pyspark | Demonstrate Spark distributed partitioning |
