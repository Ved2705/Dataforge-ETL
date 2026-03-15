<div align="center">
  <img src="https://via.placeholder.com/150?text=DataForge+Logo" alt="DataForge Logo" width="150" />
  <h1>DataForge ETL Platform</h1>
  <p>
    <strong>A Production-Grade, Full-Stack Data Engineering & ETL Management Platform</strong>
  </p>
  
  [![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
  [![Flask](https://img.shields.io/badge/Flask-2.x-black.svg)](https://flask.palletsprojects.com/)
  [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791.svg)](https://www.postgresql.org/)
  [![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
  [![dbt](https://img.shields.io/badge/dbt-Core-FF694B.svg)](https://www.getdbt.com/)
</div>

---

## 📌 Overview

**DataForge ETL Platform** is a comprehensive, web-based tool designed to streamline the Extraction, Transformation, and Loading (ETL) process. Built as a showcase of modern data engineering practices, it integrates industry-standard tools like **Great Expectations** and **dbt** into a cohesive, user-friendly Flask application.

Whether you're processing small CSVs or large-scale datasets, DataForge provides an intuitive interface to profile data, build transformation pipelines, and enforce data quality rules.

## ✨ Key Features

- **📊 Automated Data Profiling:** Instantly generate column correlations, missing value analysis, and outlier detection upon dataset upload.
- **🛠️ No-Code/Low-Code Pipeline Builder:** Construct complex ETL pipelines using a drag-and-drop or checklist interface with pre-built transformation steps.
- **✅ Built-in Data Quality (Great Expectations):** Automatically validate data against predefined expectation suites to ensure pipeline integrity.
- **🔄 SQL Modeling (dbt):** Automatically generate dbt models (`.sql` and `schema.yml`) from cleaned DataFrames for seamless analytics engineering.
- **🔐 Secure Authentication:** Role-based access control with Flask-Login and Bcrypt password hashing.
- **📈 Interactive Dashboard:** Monitor pipeline runs, success rates, and dataset metadata with Chart.js visualizations.

---

## 🏗️ Architecture

DataForge employs a modular, containerized architecture:

```text
┌──────────────────────────────────────────────────────────────┐
│                     DataForge ETL Platform                   │
├────────────┬──────────────────┬───────────────┬─────────────┤
│  Frontend  │   Backend        │  ETL Engine   │  Storage    │
│  (HTML/JS) │  REST API        │               │  PostgreSQL │
│  Chart.js  │  Flask           │  Great Expect.│             │
│  CSS3      │  Flask-Login     │  dbt          │  Local FS / │
│            │  SQLAlchemy      │  Pandas       │  Volumes    │
└────────────┴──────────────────┴───────────────┴─────────────┘
```

---

## 💻 Technology Stack

**Backend:** Python, Flask, Flask-SQLAlchemy, Flask-Migrate, Flask-Login, Flask-CORS  
**Data Engineering:** Pandas, PyArrow, Great Expectations, dbt-core  
**Infrastructure:** Docker, Docker Compose  
**Databases:** PostgreSQL 15, Redis 7
**Frontend:** Vanilla JS, HTML5, CSS3, Chart.js  

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)
- Git

### Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/dataforge-etl.git
   cd dataforge-etl
   ```

2. **Configure Environment Variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your desired secrets and configurations
   ```

3. **Start the Infrastructure using Docker Compose:**
   ```bash
   docker compose up -d
   ```
   *Note: On the first run, it may take 1-2 minutes for the database to fully initialize.*

4. **Access the Services:**
   - **DataForge UI:** [http://localhost:5000](http://localhost:5000)

---

## 🧩 Pipeline Transformation Steps

DataForge supports a variety of modular transformation operators:

| Operator | Engine | Description |
|----------|--------|-------------|
| `snake_case_columns` | Pandas | Standardizes column headers to `snake_case` format. |
| `drop_duplicates` | Pandas | Identifies and removes exact duplicate records. |
| `drop_missing` | Pandas | Drops columns exceeding a configurable null threshold (e.g., >80% missing). |
| `fill_missing` | Pandas | Imputes missing values using strategies like median, mean, mode, or ffill. |
| `remove_outliers` | Pandas | Cleans statistical anomalies using IQR or Z-score methods. |
| `cast_types` | Pandas | Intelligently infers and casts column data types (numeric, datetime). |
| `normalize` | Pandas | Min-Max scaling of numeric variables to a `[0,1]` range. |
| `standardize` | Pandas | Z-score standardization (mean=0, std=1). |

---

## 📸 Screenshots

*(Replace these placeholder links with actual screenshots of your application)*

| Dashboard | Pipeline Builder |
|:---:|:---:|
| <img src="https://via.placeholder.com/400x250?text=Dashboard+Screenshot" alt="Dashboard" /> | <img src="https://via.placeholder.com/400x250?text=Pipeline+Builder+Screenshot" alt="Pipeline Builder" /> |
| **Data Profiling & Quality** | **Run History & Logs** |
| <img src="https://via.placeholder.com/400x250?text=Data+Profiling+Screenshot" alt="Data Profiling" /> | <img src="https://via.placeholder.com/400x250?text=Run+Logs+Screenshot" alt="Run History" /> |

---

## 🔮 Future Enhancements

- [ ] Connect directly to external data warehouses (Snowflake, BigQuery, Redshift).
- [ ] Add real-time streaming data ingestion support using Apache Kafka.
- [ ] Implement advanced custom dbt macro generation via the UI.
- [ ] Support cloud storage paths (AWS S3, Google Cloud Storage, Azure Blob).

---

## 👨‍💻 About the Author

This project was built to demonstrate proficiency in modern Data Engineering technologies, backend API design, and containerized deployments.

**[Your Name / Your Portfolio Link]**  
*Data Engineer / Backend Developer*

- **LinkedIn:** [linkedin.com/in/yourprofile](https://linkedin.com/in/yourprofile)
- **GitHub:** [github.com/yourusername](https://github.com/yourusername)

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
