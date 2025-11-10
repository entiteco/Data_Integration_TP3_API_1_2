# Data Integration Pipeline: Kafka, ksqlDB, Data Lake & Data Warehouse

This project implements an end-to-end data integration pipeline based on a series of practical labs. It captures raw transaction events, processes them in real-time using  **ksqlDB** , and sinks the resulting streams and tables into both a **Data Lake** (partitioned file system) and a **Data Warehouse** (MySQL).

The project demonstrates two distinct architectures:

1. **Real-time Streaming:** (Phase 2) Consumers run continuously, processing messages one by one.
2. **Orchestrated Batch:** (Phase 3) An orchestrator runs jobs every 10 minutes to process data in batches using  **Apache Beam** .

## Architecture

The data flows through the system as follows:

`Python Producer` **→** `transaction_log` (Kafka Topic) **→** `ksqlDB Server` **→** `Processed Topics (Streams & Tables)` **→** `Python Consumers (Batch/Beam)` **→** **Data Lake (Files)** & **Data Warehouse (MySQL)**

## Features

* **Real-time Stream Processing:** Uses ksqlDB to filter, flatten, anonymize, and aggregate transaction data (e.g., `GROUP BY`, `WINDOW HOPPING`).
* **Data Lake Ingestion:** Sinks all ksqlDB output topics into a local, Hive-style partitioned file system (`/data_lake/topic_name/year=.../month=.../day=...`).
* **Data Warehouse Ingestion:** Sinks ksqlDB *Tables* into MySQL tables, using `UPSERT` logic (`INSERT ... ON DUPLICATE KEY UPDATE`) to keep aggregated data current.
* **Dual-Mode Consumers:** Provides both streaming consumers (Phase 2) and orchestrated batch consumers (Phase 3) using Apache Beam.
* **Batch Orchestration:** An orchestrator script (`orchestrator.py`) uses `schedule` to run the Beam pipelines as batch jobs every 10 minutes.

## Project Structure

```
.
├── application/
│   ├── consumer_datalake.py         # (Phase 2) Streaming consumer for Data Lake
│   ├── consumer_datawarehouse.py    # (Phase 2) Streaming consumer for Data Warehouse
│   ├── beam_consumer_datalake.py    # (Phase 3) Batch/Beam pipeline for Data Lake
│   ├── beam_consumer_datawarehouse.py # (Phase 3) Batch/Beam pipeline for Data Warehouse
│   ├── orchestrator.py                # (Phase 3) Main scheduler for Beam jobs
│   ├── kafka_producer_transaction.py  # Data generator
│   └── requirements.txt               # Python dependencies
├── cp-all-in-one/
│   └── docker-compose.yml           # Kafka, ksqlDB, Control Center stack
├── data_lake/
│   ├── streams/                     # Destination for stream data
│   └── tables/                      # Destination for table data
├── database/
│   └── schema.sql                   # MySQL DDL scripts (CREATE TABLE...)
└── README.md
```

## Setup & Installation

Follow these steps to set up the entire environment.

### 1. Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* [Python 3.10+](https://www.python.org/)
* A Git client
* A SQL GUI client (e.g., [DBeaver](https://dbeaver.io/), MySQL Workbench)

### 2. Start the Kafka Stack

This will launch Kafka, Zookeeper, ksqlDB-Server, and Control Center.

```
# 1. Clone the confluent-inc repository (if not already done)
git clone https://github.com/confluentinc/cp-all-in-one.git

# 2. Navigate into the directory
cd cp-all-in-one

# 3. Launch all services in detached mode
docker compose up -d

# 4. Verify in Docker Desktop that all containers are running
# 5. Access Control Center at http://localhost:9021
```

### 3. Start the MySQL Database

This launches a separate container for our Data Warehouse.

```bash
# In a new terminal, run:
docker run -d \
    --name mysql-tp3 \
    -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=admin123 \
    -e MYSQL_DATABASE=tp3 \
    mysql:8
```

### 4. Create the SQL Tables

You must create the table structures before the consumer can write to them.

1. Open your SQL client (DBeaver) and connect to the MySQL database:
   * **Host:** `localhost`
   * **Port:** `3306`
   * **Database:** `tp3`
   * **Username:** `root`
   * **Password:** `admin123`
2. Open the `database/schema.sql` file from this project.
3. Copy its contents and run the entire script in DBeaver to create the 4 required tables.

### 5. Install Python Dependencies

This installs Kafka, MySQL, Beam, and Schedule clients.

```bash
# Navigate to the application directory of *this* project
cd ../application

# (Optional but recommended) Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install all required packages
pip install -r requirements.txt
```

## How to Run

### 1. Run the ksqlDB Queries

Your consumers listen to topics that are *created by* ksqlDB. You must run your ksqlDB queries from TP 1 & 2 first.

1. Go to the Control Center: `http://localhost:9021`
2. Navigate to the **ksqlDB** cluster **→**  **Editor** .
3. Run all your `CREATE STREAM ... AS SELECT ...` and `CREATE TABLE ... AS SELECT ...` queries to start the flow of data.
4. Check the **Queries** tab to ensure they are all in a **"Running"** state.

### 2. Run the Phase 3 (Batch/Beam) Pipeline

This is the main pipeline for the TP3.

```
# 1. (Optional) Generate a backlog of data.
# Run this once to populate Kafka with 5000 messages.
python3 application/kafka_producer_transaction.py

# 2. Run the main orchestrator in a terminal.
# It will run the jobs once at startup, then every 10 minutes.
python3 application/orchestrator.py
```

The orchestrator will now run in your terminal, executing the Beam pipelines every 10 minutes to process new data.

---

### [Alternative] Running the Phase 2 (Streaming) Pipeline

To run the original real-time consumers:

**Terminal 1 (Data Lake):**

```bash
python3 application/consumer_datalake.py
```

**Terminal 2 (Data Warehouse):**

```bash
python3 application/consumer_datawarehouse.py
```

**Terminal 3 (Producer):**

```bash
python3 application/kafka_producer_transaction.py
```
