# 🏢 Enterprise Data Cleaning and ETL Orchestration Framework Using Python

> **Infosys Virtual Internship Project**  
> Enterprise-grade data cleaning and ETL orchestration framework built with Python & Apache Airflow.

---

## 📌 Project Overview

This framework provides a scalable, modular solution for **extracting, transforming, and loading (ETL)** enterprise data using Apache Airflow for orchestration, Pandas/PySpark for transformations, and Streamlit/Dash for real-time monitoring dashboards.

---

## 🏗️ Architecture

```
etl_framework/
├── dags/                        # Airflow DAG definitions
│   ├── clean_transform_load.py  # Main ETL DAG
│   ├── etl_pipeline_dag.py      # Pipeline orchestration
│   ├── extract_customers.py     # Customer data extraction
│   ├── extract_transactions.py  # Transaction data extraction
│   ├── master_etl_dag.py        # Master orchestration DAG
│   └── monitoring_dag.py        # Health monitoring DAG
├── cleaners/                    # Data cleaning modules
│   ├── rule_engine.py           # Cleaning rule engine
│   └── standard_rules.py       # Standard cleaning rules
├── transformers/                # Data transformation modules
│   ├── pandas_transformer.py    # Pandas-based transformations
│   └── spark_transformer.py     # PySpark transformations
├── extractors/                  # Data extraction modules
├── utils/                       # Utility modules
│   ├── pipeline_logger.py       # Centralized logging
│   └── alert_callbacks.py       # Success/failure alerts
├── data/                        # Data storage
├── tests/                       # Unit & integration tests
├── dashboard.html               # Monitoring dashboard
└── requirements.txt             # Dependencies
```

---

## 🗄️ Data Models

### Customers Table
| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT (PK) | Unique identifier |
| name | VARCHAR | Customer name |
| email | VARCHAR | Contact email |
| region | VARCHAR | Geographic region |
| created_at | TIMESTAMP | Record creation time |

### Transactions Table
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | INT (PK) | Unique identifier |
| customer_id | INT (FK) | References customers |
| amount | DECIMAL | Transaction amount |
| status | VARCHAR | Transaction status |
| timestamp | TIMESTAMP | Transaction time |

**Relationships:** `transactions.customer_id` → `customers.customer_id` (Many-to-One)

---

## 🚀 Milestones

### ✅ Milestone 1 — Weeks 1–2: Environment Setup & Pipeline Design
- Installed and configured Apache Airflow
- Designed data models and column relationships
- Set up extraction scripts for customers and transactions

### ✅ Milestone 2 — Weeks 3–4: Data Cleaning & Transformation
- Developed rule-based cleaning engine
- Implemented transformations using Pandas and PySpark
- Unit tested pipeline components

### ✅ Milestone 3 — Weeks 5–6: Orchestration & Monitoring
- Configured Airflow DAG scheduling
- Integrated centralized logging via `PipelineLogger`
- Added success/failure alert callbacks

### ✅ Milestone 4 — Weeks 7–8: Dashboards & Deployment
- Built monitoring dashboard (Streamlit/Dash)
- Tested on production-scale datasets
- Deployed and validated full ETL framework

---

## ⚙️ Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow | Pipeline orchestration & scheduling |
| Python 3.9 | Core language |
| Pandas | Data transformation |
| PySpark | Large-scale data processing |
| SQLAlchemy | Database ORM |
| Streamlit/Dash | Monitoring dashboards |
| Plotly | Data visualization |
| Pytest | Unit testing |

---

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.9+
- WSL2 / Linux environment
- Apache Airflow 2.x

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/Himaja-23110/enterprise-etl-framework-.git
cd enterprise-etl-framework-

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set Airflow home
export AIRFLOW_HOME=~/airflow

# 4. Initialize Airflow database
airflow db init

# 5. Start Airflow
airflow webserver --port 8080 &
airflow scheduler &
```

---

## 📊 Running the Pipeline

```bash
# Trigger ETL pipeline manually
airflow dags trigger etl_pipeline_dag

# Check pipeline health
airflow dags trigger monitoring_dag
```

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 📈 Monitoring

Access the Airflow UI at: `http://localhost:8080`  
Default credentials: `admin / admin`

---

## 👩‍💻 Author

**Himaja**  
Infosys Virtual Internship — Enterprise ETL Framework  
GitHub: [@Himaja-23110](https://github.com/Himaja-23110)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

