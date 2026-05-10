# Airflow ETL Pipeline with PostgreSQL and NASA API Integration 🚀

[![License](https://img.shields.io/github/license/yashjajoria/Airflow-ETL-Pipeline-with-Postgres-and-API-Integration.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg?logo=python)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-🛢️-blue?logo=postgresql)](https://www.postgresql.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.0-blue?logo=apache-airflow)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-%231572B6.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Last Commit](https://img.shields.io/github/last-commit/yashjajoria/Airflow-ETL-Pipeline-with-Postgres-and-API-Integration.svg)](../../commits/main)

---

## 📝 Project Overview

This repository demonstrates a full-featured **ETL data pipeline** built using [Apache Airflow](https://airflow.apache.org/), orchestrating ingestion of daily data from the [NASA Astronomy Picture of the Day (APOD) API](https://api.nasa.gov/), transforming the response, and loading it into a **PostgreSQL** database. All services (Airflow & Postgres) are containerized using **Docker** to ensure a consistent, reproducible development and deployment environment. 

This project is ideal for those looking to learn practical data engineering, workflow automation, and containerized development.

---

## ✨ Features

- **Automated ETL with Airflow DAGs**: Fully scheduled pipelines with clear data lineage.
- **API Extraction**: Uses Airflow’s `SimpleHttpOperator` for robust API calls.
- **TaskFlow for Transformation**: Leverages Airflow’s `@task` decorator for data transformation in Python.
- **PostgreSQL Integration**: Loads the clean data into a Postgres table, with schema auto-creation via Airflow operators.
- **Containerization**: Entire stack runs inside Docker containers for ease of deployment & isolation.
- **Postgres Hook and Operators**: Uses native Airflow hooks for secure and extensible database connections.
- **Daily Scheduling**: Runs automatically once a day.
- **Scalable and Modular**: Easily extendable for more APIs or complex workflows.
- **Production-Ready Structure**: Keeps code, configs, and secrets organized and industry-standard.

---

## 🛠️ Tech Stack

| Technology      | Purpose                                      |
|-----------------|----------------------------------------------|
| Python          | Main programming language for DAG & scripts  |
| Apache Airflow  | Orchestration & scheduling of ETL tasks      |
| PostgreSQL      | Relational database for persistent storage   |
| Docker          | Containerization of services                 |
| Docker Compose  | Orchestration of multi-container setup       |
| NASA APOD API   | Third-party data source                      |
| Airflow Hooks   | Secure connections to Postgres and HTTP APIs |

---

## 📊 Architecture Diagram

```mermaid
flowchart TD
    A[NASA APOD API] -- Extract --> B[Airflow SimpleHttpOperator]
    B -- Raw JSON --> C[Airflow TaskFlow Python Operator (Transform)]
    C -- Insert data --> D[Airflow PostgresHook]
    D -- Upsert data --> E[PostgreSQL Database]
    F[Airflow DAG Scheduler] -- Orchestrates --> B
    F -- Orchestrates --> C
    F -- Orchestrates --> D
    G[Docker Compose] -- Runs containers --> F
    G -- Runs containers --> E
```

---

## 🔄 Workflow Explanation

**1. Extract**  
Fetches astronomy data from the NASA APOD API using `SimpleHttpOperator`. The response contains details such as `title`, `explanation`, `url`, and `date`.

**2. Transform**  
Uses Airflow’s TaskFlow API (`@task` decorator) to process the JSON response, select required fields, and clean/validate the data before loading.

**3. Load**  
Loads the processed data into PostgreSQL using Airflow’s `PostgresHook`. The table is created automatically if it doesn’t exist (using `PostgresOperator`).

---

## 📁 Folder Structure

```
Airflow-ETL-Pipeline-with-Postgres-and-API-Integration/
├── dags/
│   └── nasa_apod_etl_dag.py         # Main Airflow DAG
├── docker/
│   ├── docker-compose.yaml          # Docker Compose orchestration
│   └── .env                         # Environment variables
├── requirements.txt                 # Python dependencies
├── airflow/
│   └── Dockerfile                   # Airflow Docker container
├── postgres/
│   └── Dockerfile                   # (If custom Postgres image)
├── README.md
```

---

## 🗓️ Airflow DAG Explanation

- **DAG** stands for *Directed Acyclic Graph*, representing the workflow.
- **Operators** are the building blocks: in this DAG, they connect to NASA API, transform data, and load into Postgres.
- **TaskFlow API** (Python @task): Makes task dependencies and data passing (XCom) simpler, readable, and more Pythonic.
- **Schedule**: Runs daily, ensuring up-to-date data.

**Sample DAG checkpoints:**
```python
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(schedule_interval='@daily', ...)
def nasa_apod_etl():
    extract = SimpleHttpOperator(...)
    @task()
    def transform(...): ...
    @task()
    def load(...): ...
    extract >> transform() >> load()
```

---

## 🗄️ Database Schema / Table Structure

**PostgreSQL Table: `apod_photos`**

| Column      | Data Type | Description                         |
|-------------|-----------|-------------------------------------|
| date        | date      | APOD date (Primary Key)             |
| title       | text      | Title of the image                  |
| explanation | text      | Detailed explanation                |
| url         | text      | Image URL                           |

**Sample SQL Table Creation:**
```sql
CREATE TABLE IF NOT EXISTS apod_photos (
    date DATE PRIMARY KEY,
    title TEXT,
    explanation TEXT,
    url TEXT
);
```

---

## 🐳 Docker Setup

Docker ensures parity across local and production environments by isolating all dependencies, configurations, and services.

**Benefits:**
- No "works on my machine" issues 🎉
- Simple onboarding for new contributors
- Environment consistency across deployments

**Sample Key Docker Compose Services:**
```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: airflow
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
  airflow:
    build: ./airflow
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      ...
    depends_on:
      - postgres
    ports:
      - 8080:8080
```

---

## 🚀 Installation & Setup Instructions

### 1. **Clone the Repository**
```bash
git clone https://github.com/yashjajoria/Airflow-ETL-Pipeline-with-Postgres-and-API-Integration.git
cd Airflow-ETL-Pipeline-with-Postgres-and-API-Integration
```

### 2. **Configure Environment Variables**
Copy `.env.example` to `.env` and update as needed.

### 3. **Start Services (Docker Compose)**
```bash
docker-compose -f docker/docker-compose.yaml up --build
```
This will build and start Airflow, PostgreSQL, and supporting services.

---

## ⚙️ Environment Variables (`.env` Example)

```env
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

AIRFLOW__CORE__FERNET_KEY=YOUR_FERNET_KEY
AIRFLOW__WEBSERVER__SECRET_KEY=YOUR_SECRET_KEY

NASA_API_KEY=DEMO_KEY
```

---

## ▶️ Running the Project

- With Docker running, visit [http://localhost:8080](http://localhost:8080) for Airflow UI.
- Enable and trigger the `nasa_apod_etl` DAG.
- The pipeline will extract data from NASA API daily and populate the Postgres database.

---

## 🌐 Airflow UI Access

- **URL:** [http://localhost:8080](http://localhost:8080)
- **Default Username:** `airflow`
- **Default Password:** `airflow`
- _You can change credentials in Docker Compose and Airflow environment settings._

---

## 🛢️ PostgreSQL Connection Details

| Parameter     | Value      |
|---------------|-----------|
| Host          | `postgres` (use `localhost` if connecting via DBeaver, etc.) |
| Port          | `5432`    |
| Database      | `airflow` |
| Username      | `airflow` |
| Password      | `airflow` |

- To connect externally: `psql -h localhost -p 5432 -U airflow airflow`

---

## 📦 Example API Response

```json
{
  "date": "2024-05-10",
  "explanation": "Astronomy is amazing. Here is why...",
  "title": "The Milky Way Over the Desert",
  "url": "https://apod.nasa.gov/apod/image/2405/MilkyWay_Dessert1024.jpg"
}
```

---

## 📸 Screenshots

> _Add your screenshots here_  
> ![Airflow UI Example](screenshots/airflow_ui.png)  
> ![DAG Status](screenshots/dag_status.png)  
> ![Postgres Data Table](screenshots/postgres_table.png)

---

## 🚧 Future Improvements

- Deploy on cloud (AWS ECS / GCP Composer)
- Implement CDC (Change Data Capture) on table
- Add data validation and alerting
- Parameterize DAG for different APIs or date ranges
- Integrate data lineage and visualization tools
- Scheduled email notifications on success/failure
- Use Docker Swarm or Kubernetes for orchestration

---

## 📚 Learning Outcomes

- Practical experience building containerized ETL pipelines
- Understanding of Airflow DAG design, execution, and modular Operator use
- Experience integrating REST APIs and relational databases
- Exposure to Docker-based workflows and distributed data pipelines

---

## 🧩 Challenges Faced

- Handling API rate limits and missing/invalid API responses
- Managing Airflow/DB service dependencies on startup in Docker
- Environment variable management for container secrets
- Ensuring idempotent data loading and handling duplicates
- Debugging DAG/task failures within an isolated environment

---

## 💡 Why Docker?

Docker containerizes all dependencies (Airflow, Postgres), providing:
- Easy onboarding for new developers
- Zero setup-clash between team members
- Identical local and prod environments
- Reproducibility and reliability

---

## 🗒️ Troubleshooting Tips

- **Airflow "Module not found":** Rebuild images: `docker-compose build --no-cache`
- **DB connection failed:** Ensure Postgres is healthy & available before Airflow starts.
- **Ports in use:** Stop other services running on 8080/5432.
- **API key errors:** Replace `NASA_API_KEY` in `.env` with a valid key from [api.nasa.gov](https://api.nasa.gov/).
- **Scheduler not running:** Check Airflow logs for errors and restart affected containers.
- **Accessing Airflow logs or UI:** Use `docker-compose logs airflow` and `docker-compose ps`.

---

## ☁️ Deployment & Scalability Ideas

- Deploy using managed Airflow (e.g., AWS MWAA, GCP Composer)
- Use cloud-hosted Postgres (RDS, CloudSQL)
- Integrate with cloud secrets managers (AWS Secrets Manager)
- Add continuous integration workflows for linting and test DAGs
- Parameterize for multi-API and multi-table ingestion

---

## 🏁 Conclusion

This project showcases a production-quality, fully automated data ingestion pipeline using industry-standard tools. It’s a great foundation for more complex data projects, teaching best practices in workflow management, orchestration, and cloud-native development.

---

## 👤 Author

**Yash Jajoria**  
[![GitHub](https://img.shields.io/badge/GitHub-@yashjajoria-181717?logo=github)](https://github.com/yashjajoria)

---

## 📝 License

This project is licensed under the [MIT License](LICENSE).

