Data Platform Project (ETL & API)

This repository contains the complete source code for a data platform, separated into two main projects: an ingestion pipeline and a serving API.

🚀 Projects

This repository is a "mono-repo" containing two distinct projects. For detailed technical information, please see the README.md file inside each sub-directory.

1. ETL_Pipeline/ (Project 1: TP3)

This project is the Data Ingestion Pipeline.

    Technology: Kafka, ksqlDB, Apache Beam, Python, MySQL (Docker)

    Purpose: Consumes raw data from Kafka, processes it with ksqlDB, and uses a Beam pipeline to populate the Data Lake (file storage) and the Data Warehouse (MySQL).

    See Also: ETL_Pipeline/README.md for setup and technical details.

2. API_Service/ (Project 2: API 1 & 2)

This project is the Data Serving API.

    Technology: Django, Django Rest Framework, MySQL

    Purpose: Provides a secure, authenticated, and documented REST API that serves data from the Data Warehouse (MySQL) and the Data Lake (files) populated by the ETL pipeline.

    See Also: API_Service/README.md for setup and endpoint documentation.

🔗 How They Work Together

The ETL_Pipeline project must be running first. It populates the mysql-tp3 database and the data_lake/ directory.

The API_Service project then connects to that same mysql-tp3 database and data_lake/ directory to read and serve the data.

For full setup and run instructions, please see the README.md inside each project folder.



## 🚀 Installation and How to Run

This project consists of two separate but connected applications: the **ETL Pipeline** (which populates the database) and the **API Service** (which serves the data).

You must run them in the correct order.

### Part 1: Start Infrastructure & ETL Pipeline

1.  **Start Infrastructure (Kafka & MySQL):**
    Open a terminal, navigate to the `ETL_Pipeline/` folder and run:
    ```bash
    # (Assuming mysql-tp3 is part of your docker-compose.yml)
    cd ETL_Pipeline/cp-all-in-one/
    docker compose up -d
    ```

2.  **Run ksqlDB Queries:**
    Go to Control Center (`http://localhost:9021`) and run all the `CREATE STREAM AS...` and `CREATE TABLE AS...` queries from the TP2 script to start the data processing.

3.  **Generate Test Data:**
    Open a second terminal, navigate to `ETL_Pipeline/application/` and run the producer:
    ```bash
    # (Activate venv if you have one for this project)
    cd ETL_Pipeline/application/
    python3 kafka_producer_transaction.py
    ```

4.  **Run the ETL Pipeline:**
    Run the orchestrator to populate the Data Lake and Data Warehouse tables:
    ```bash
    python3 orchestrator.py
    ```
    *Wait for this job to complete. You can check DBeaver to confirm the tables are full.*

### Part 2: Start the API Service

1.  **Open a new terminal** for the API.
2.  **Install Dependencies:**
    ```bash
    cd API_Service/
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt 
    ```

3.  **Apply API Migrations:**
    This will connect to the MySQL database (started in Part 1) and create the Django-specific tables (users, tokens, audit logs).
    ```bash
    python3 manage.py migrate
    ```

4.  **Create Your Superuser:**
    You will be prompted to create a username (e.g., `admin`) and password.
    ```bash
    python3 manage.py createsuperuser
    ```

5.  **Run the API Server:**
    ```bash
    python3 manage.py runserver
    ```

6.  **Test the API:**
    Your API is now running on `http://127.0.0.1:8000/`.
    * View the documentation at: `http://127.0.0.1:8000/swagger/`
    * Follow the API documentation (README Part I) to get an auth token and test the endpoints.
