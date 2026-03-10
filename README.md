# Olympics dbt Airflow Pipeline

A production-style end-to-end data engineering project built using the **Olympics Athletes Dataset (1896–2024)**. This project demonstrates a complete ETL pipeline using **Python**, **dbt**, **Apache Airflow**, **Snowflake**, and **Streamlit**.

---

## Project Overview

This project ingests historical Olympic athlete data, transforms it into a star schema data warehouse, and serves it through an interactive web application. The entire pipeline is orchestrated by Apache Airflow and runs automatically on a weekly schedule.

```
CSV Dataset
    ↓
Python Ingestion (load_raw.py)
    ↓
Snowflake RAW Schema
    ↓
dbt Staging (clean & standardize)
    ↓
dbt Marts (Star Schema — Dims & Facts)
    ↓
Streamlit Web App (4 interactive pages)
    ↑
Airflow DAG (orchestrates everything weekly)
    ↑
Monitoring (data quality checks & logging)
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + Snowflake Connector | Load CSV into Snowflake |
| Storage | Snowflake | Cloud data warehouse |
| Transformation | dbt Core | Staging + dimensional modelling |
| Orchestration | Apache Airflow | Schedule & automate pipeline |
| Containerisation | Docker | Run Airflow locally |
| Data Quality | Custom Python | 6 automated checks |
| Frontend | Streamlit | Interactive web app |

---

## Project Structure

```
olympics-dbt-airflow-pipeline/
│
├── data/                          # Olympics CSV dataset (not tracked in git)
│
├── ingestion/
│   └── load_raw.py                # Loads CSV into Snowflake RAW schema
│
├── olympic_dbt/                   # All dbt transformations
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_athlete_events.sql    # Cleans raw data
│   │   └── marts/
│   │       ├── dim_athlete.sql
│   │       ├── dim_country.sql
│   │       ├── dim_coach.sql
│   │       ├── dim_host.sql
│   │       ├── dim_sport.sql
│   │       ├── dim_medals.sql
│   │       ├── dim_event.sql
│   │       ├── fact_athlete_performance.sql
│   │       ├── fact_country_performance.sql
│   │       └── fact_event_performance.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── profiles.yml
│   └── dbt_project.yml
│
├── frontend/
│   ├── app.py                     # Streamlit home page
│   ├── snowflake_db.py            # Snowflake connection helper
│   └── pages/
│       ├── 01_overview.py         # Key stats & medal distribution
│       ├── 02_countries.py        # Medal table & country explorer
│       ├── 03_athletes.py         # Athlete search & career stats
│       └── 04_events.py           # Filter events by sport & year
│
├── monitoring/
│   └── monitor.py                 # Data quality checks & pipeline logging
│
├── orchestration/
│   ├── docker-compose.yml         # Airflow Docker setup
│   ├── .env                       # Snowflake credentials for Docker
│   └── dags/
│       └── olympic_pipeline.py    # Airflow DAG definition
│
├── config/
│   └── .env                       # Snowflake credentials (not tracked in git)
│
├── requirements.txt
└── README.md
```

---

## Data Model (Star Schema)

### Dimensions
| Table | Description |
|---|---|
| `DIM_ATHLETE` | Athlete profiles (name, gender, DOB, record holder status) |
| `DIM_COUNTRY` | Country info (code, name, first participation) |
| `DIM_COACH` | Coach names |
| `DIM_HOST` | Olympic Games host cities and years |
| `DIM_SPORT` | Sports and games type (Summer/Winter) |
| `DIM_MEDALS` | Medal types (Gold, Silver, Bronze, No Medal) |
| `DIM_EVENT` | Events and participation type (Team/Individual) |

### Facts
| Table | Description |
|---|---|
| `FACT_ATHLETE_PERFORMANCE` | Career stats per athlete (medals, Olympics attended, physical attributes) |
| `FACT_COUNTRY_PERFORMANCE` | Total medals and ranking per country |
| `FACT_EVENT_PERFORMANCE` | Individual event results linking all dimensions |

---

## Airflow Pipeline (DAG)

The `olympic_etl_pipeline` DAG runs **every week** and executes these steps in order:

```
ingest_raw_data → dbt_staging → dbt_dimensions → dbt_facts → run_monitoring
```

| Step | What it does |
|---|---|
| `ingest_raw_data` | Runs `load_raw.py` — loads CSV into Snowflake RAW |
| `dbt_staging` | Cleans and standardizes raw data |
| `dbt_dimensions` | Builds all 7 dimension tables |
| `dbt_facts` | Builds all 3 fact tables |
| `run_monitoring` | Runs 6 data quality checks and logs results |

---

## Monitoring

The monitoring script runs 6 automated data quality checks after every pipeline run:

- No null athlete IDs in `dim_athlete`
- No null country codes in `dim_country`
- Valid medal values only (Gold/Silver/Bronze/No Medal)
- Staging table has data
- No orphaned athletes in fact table
- Fact event performance has data

Results are logged to `OLYMPICS_DB.MONITORING.PIPELINE_RUN_LOG` in Snowflake.

---

## How to Run the Project

### Prerequisites
- Python 3.10+
- Docker Desktop
- Snowflake account (free trial at https://signup.snowflake.com)
- dbt CLI (`pip install dbt-core dbt-snowflake`)

---

### Step 1 — Clone the Repository
```bash
git clone https://github.com/mithali-m/olympics-dbt-airflow-pipeline.git
cd olympics-dbt-airflow-pipeline
```

---

### Step 2 — Install Python Dependencies
```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

### Step 3 — Download the Dataset
Download the Olympics Athletes Dataset (1896–2024) from Kaggle:
https://www.kaggle.com/datasets/ashyou09/olympics-athletes-dataset-18962024

Place the CSV file in the `data/` folder.

---

### Step 4 — Configure Snowflake Credentials
Create `config/.env` with your Snowflake credentials:
```
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=OLYMPICS_DB
SNOWFLAKE_SCHEMA=RAW
```

Also create `orchestration/.env` with the same credentials (used by Airflow in Docker).

---

### Step 5 — Configure dbt Profile
Update `olympic_dbt/profiles.yml` with your Snowflake credentials:
```yaml
olympic_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account_id
      user: your_username
      password: your_password
      role: ACCOUNTADMIN
      database: OLYMPICS_DB
      warehouse: COMPUTE_WH
      schema: ANALYTICS
      threads: 1
```

---

### Step 6 — Start Airflow with Docker
```bash
cd orchestration

# First time only — initialise Airflow database
docker-compose up airflow-init

# Start all Airflow services
docker-compose up -d
```

Wait 2-3 minutes for services to start, then open:
**http://localhost:8080** (login: `admin` / `admin`)

---

### Step 7 — Trigger the Pipeline
In the Airflow UI:
- Find `olympic_etl_pipeline`
- Click the **▶ (Play)** button → **Trigger DAG**
- Watch all 5 tasks turn **green** 

This runs the full pipeline:
```
ingest_raw_data → dbt_staging → dbt_dimensions → dbt_facts → run_monitoring
```

---

### Step 8 — Start the Frontend
Open a new terminal and run:
```bash
cd frontend
streamlit run app.py
```

Open your browser at **http://localhost:8501**

---

### Step 9 — Explore the App

| Page | What you can do |
|---|---|
| 📊 Overview | Total athletes, countries, sports, medal distribution charts |
| 🌍 Countries | Browse medal table, search a country, see its athletes |
| 🏃 Athletes | Search any athlete by name, view career stats and events |
| 🏆 Events | Filter events by sport and Olympic year |

---

### Step 10 — Stop Everything When Done
```bash
# Stop Streamlit
Ctrl + C

# Stop Airflow
cd orchestration
docker-compose down
```

---

## Subsequent Runs

Every time you want to use the project:
```bash
# 1. Start Docker Desktop
# 2. Start Airflow
cd orchestration && docker-compose up -d

# 3. Start Frontend
cd frontend && streamlit run app.py

# 4. Open http://localhost:8501
```

The Airflow pipeline runs **automatically every week** — no manual triggering needed after the first run!

---

## Dataset

**Olympics Athletes Dataset (1896–2024)**
- Source: Kaggle — https://www.kaggle.com/datasets/ashyou09/olympics-athletes-dataset-18962024
- 8,500 rows covering 128 years of Olympic history
- 30 columns including athlete demographics, event results, medal counts, and country statistics
