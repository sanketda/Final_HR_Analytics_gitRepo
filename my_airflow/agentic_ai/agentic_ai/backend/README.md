

# Databricks Pipelines Monitoring System

Monitoring, automated diagnosis, and data quality validation for Databricks workspaces.

---





### Pipelines Monitoring

* Webhook listener captures **Databricks job failures**.
* Failures are **enqueued in Redis**, processed by **Celery workers**.
* Workers perform **LLM-driven root cause analysis** and apply **trivial remediations** when possible.
* **Notifications (email)** are sent to vendors.
* **PostgreSQL** persists job metadata and diagnosis history.
* **FastAPI endpoints** expose monitoring data for querying and visualization.

**Architecture:**
![Pipelines Monitoring](system_diagram.png)

---

### Data Quality Pipeline

* Periodically validates **newly ingested tables** in Databricks.
* Selects candidates based on **last modified timestamp vs. last validation timestamp**.
* Performs checks like:

  * Data type mismatches
  * Nullability violations
  * Column-level anomalies
* Results are **persisted in PostgreSQL**.
* Built on **LangGraph-based agentic workflows** for modular validation.

**Architecture:**
![Data Quality Pipeline](data_quality_diagram.png)



---

## Deployment (Docker)

```bash
docker login -u USERNAME -p PASSWORD
```

### Pipelines Monitoring

```bash
cd databricks-monitoring-deploy/
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

### Data Quality Pipeline

```bash
cd data-quality-deploy/
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```


---

## CI/CD Workflow

* Triggered on every **commit to `main`**.
* Pipeline stages:

  1. Run unit tests
  2. Build Docker images
  3. Push to container registry
  4. Inject env vars via **GitLab CI/CD variables**






