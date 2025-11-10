# End-to-End Data Platform: Kafka, Data Lake, & Django API

This project demonstrates the design and implementation of a complete, end-to-end data platform. It follows the entire lifecycle of data, starting from raw event generation, through real-time processing, ingestion into both a Data Lake and a Data Warehouse, and finally, exposure via a secure, authenticated, and documented REST API.

The architecture is built in distinct, decoupled layers:

1. **Event Streaming (Kafka & ksqlDB):** Raw transaction events are produced into Apache Kafka. **ksqlDB** is used for real-time stream processing to filter, flatten, anonymize, and aggregate this data into clean topics.
2. **Ingestion & Storage (Python, Beam, MySQL, Data Lake):** A Python-based orchestration layer (`schedule` + `Apache Beam`) consumes the processed topics. It populates a **Data Lake** (Hive-partitioned files for analytics) and a **Data Warehouse** (MySQL, for fast, aggregated lookups).
3. **Serving Layer (Django REST Framework):** A secure **Django API** acts as the single point of entry for end-users. It serves data from *both* the Data Lake and the Data Warehouse, enforces all security, and provides advanced capabilities like data replay and auditing.

---

## Core Technologies

* **Event Streaming:** Apache Kafka (Confluent Platform)
* **Stream Processing:** ksqlDB
* **Data Ingestion (ETL):** Python, `kafka-python-ng`, Apache Beam
* **Orchestration:** `schedule`
* **Data Lake:** Local File System (JSON files, Hive-partitioned by date)
* **Data Warehouse:** MySQL (running in Docker)
* **API & Serving Layer:** Django & Django Rest Framework (DRF)
* **Authentication:** DRF Token Authentication (Token-based)
* **API Documentation:** Swagger (drf-yasg)

---

## Project Architecture

The data flows through two primary pipelines: an **Ingestion Pipeline** (TPs 1-3) and a **Serving Pipeline** (TPs API 1-2).

### 1. Ingestion Pipeline (Data Flow)

`Producer (Python)` **→** `Kafka Topic (transaction_log)` **→** `ksqlDB (Real-time Processing)` **→** `Processed Kafka Topics (Streams & Tables)` **→** `Orchestrator (Apache Beam)` **→** `[Data Lake (Files)]` & `[Data Warehouse (MySQL)]`

### 2. Serving Pipeline (Query Flow)

`End User (Postman)` **↔** `Django REST API` **↔** `[Data Warehouse (MySQL)]` & `[Data Lake (Files)]`

---

## Key Features

* **Real-time Processing:** Data is anonymized and aggregated *before* it is ever stored.
* **Hybrid Storage:** Implements both a Data Lake (for raw history) and a Data Warehouse (for fast metrics).
* **Batch Orchestration:** Uses Apache Beam and `schedule` to run batch ingestion jobs every 10 minutes.
* **Secure API:** All endpoints are protected by token authentication (`api-token-auth`).
* **Custom Permissions:** A database-driven authorization model (`UserPermission` table) controls *what* data users can access.
* **Full Audit Trail:** A custom Django Middleware (`ApiAuditLog`) logs every single request made to the API for security and compliance.
* **Advanced Endpoints:** Includes capabilities for data replay (re-pushing data to Kafka) and RPC-style actions (triggering ML models).

---

## I. Authentication and Authorization

This section outlines the security, authentication, and auditing mechanisms implemented for the API. All API endpoints are protected and require authentication.


### 1. Authentication (Token-Based)

Authentication is handled using  **Django Rest Framework's built-in Token system** . All endpoints (except `/api-token-auth/`) require a valid token to be passed in the request header.

**Step 1: Get Your Authentication Token**

Make a `POST` request to this endpoint with your user credentials.

* **Endpoint:** `POST /api-token-auth/`
* **Body (JSON):**

```
{
    "username": "your_username",
    "password": "your_password"
}
```

* **Success Response (200 OK):**

```json
{
    "token": "9944b091f1a62f87e66164227b34b8c734e7f8f0"
}
```

**Step 2: Use Your Token**

For all other API requests, you must provide this token in an `Authorization` header.

* **Key:** `Authorization`
* **Value:** `Token 9944b091f1a62f87e66164227b34b8c734e7f8f0`


### 2. Authorization (Access Rights Management)

This API uses a custom, table-based permission system to control what authenticated users can do. Permissions are stored in the `UserPermission` table and are managed by administrators via a dedicated endpoint.

* **Model:** `UserPermission`
* **Available Permissions:** `view_products`, `add_products`, `edit_products`, `view_expensive_product`


**Grant a Permission**

An administrator can grant a permission to any user.

* **Endpoint:** `POST /admin/permissions/`
* **Auth:** Requires Admin Token (e.g., `kzm`'s token).
* **Body (JSON):**

```
{
    "username": "test_user",
    "permission": "view_products"
}
```

* **Success Response (201 Created):**

```json
{
    "status": "Permission 'view_products' added to test_user"
}
```


**Revoke a Permission**

An administrator can revoke a permission from any user.

* **Endpoint:** `DELETE /admin/permissions/`
* **Auth:** Requires Admin Token (e.g., `kzm`'s token).
* **Body (JSON):**

```json
{
    "username": "test_user",
    "permission": "view_products"
}
```

* **Success Response (204 No Content)**


### 3. Auditing

As required, **every single request** made to any endpoint starting with `/api/` is automatically logged in the database for auditing and security purposes.

* **Implementation:** This is achieved via a custom `AuditLogMiddleware` (`Api_1/middleware.py`).
* **Model:** `ApiAuditLog`
* **Logged Data:**
  * The user who made the request (if authenticated).
  * The time of the request.
  * The HTTP method (`GET`, `POST`, etc.).
  * The full path (`/api/products/`).
  * The request body (if any).
* **How to View Logs:** All audit logs are visible to superusers in the Django Admin interface at `http://127.0.0.1:8000/admin/Api_1/apiauditlog/`.

---


## II. Data Retrieval (Data Lake)

This part of the API provides endpoints to read data directly from the partitioned JSON files stored in the Data Lake. These endpoints are designed for retrieving raw, historical data and support advanced features like pagination, projection, and filtering.


### Endpoint: Get Data Lake Stream

This single, powerful endpoint handles all read operations for any stream in the Data Lake, fulfilling the requirements for questions 3, 4, and 5.

`GET /api/datalake/streams/<str:topic_name>/`

* **Description:** Retrieves a paginated list of all messages from a specific stream in the Data Lake (e.g., `TRANSACTIONS_PROCESSED`).
* **Authentication:** Requires Token Authentication (see Part I). The user must also have the `view_datalake` permission (granted via the `/api/admin/permissions/` endpoint).
* **Path Parameter:**
  * `<str:topic_name>`: The name of the ksqlDB topic to read (e.g., `TRANSACTIONS_PROCESSED`, `TRANSACTIONS_BLACKLISTED`, `TRANSACTIONS_FLAT`).

---

### Query Parameters

#### 1. Pagination (Question 3)

The endpoint automatically paginates results, returning a maximum of  **10 messages per page** .

* **Parameter:** `page`
* **Example:** `?page=2`
* **Description:** Specifies the page number to retrieve. Defaults to `1`.

#### 2. Projection (Question 4)

You can select a subset of fields to be returned using the `fields` parameter.

* **Parameter:** `fields`
* **Example:** `?fields=user_id,amount,status`
* **Description:** A comma-separated list of fields to return. The API automatically handles mapping `user_id` to `USER_ID` and `amount` to `AMOUNT_USD`.

#### 3. Filtering (Question 5)

The endpoint supports a wide range of filters on the dataset.

| Parameter        | Type   | Description                                         |
| ---------------- | ------ | --------------------------------------------------- |
| payement_method  | string | Exact match (e.g.,`apple_pay`)                    |
| country          | string | Exact match on `location.country` (e.g., `USA`) |
| product_category | string | Exact match (e.g.,`books`)                        |
| status           | string | Exact match (e.g.,`completed`)                    |
| amount_gt        | float  | Amount **greater than** this value           |
| amount_lt        | float  | Amount**less than** this value                |
| amount_eq        | float  | Amount**equal to** this value                 |
| rating_gt        | int    | Customer Rating**greater than** this value    |
| rating_lt        | int    | Customer Rating**less than** this value       |
| rating_eq        | int    | Customer Rating**equal to** this value        |


### Usage Examples

#### Example 1: (Q3) Pagination

Retrieve the second page of results from the `TRANSACTIONS_PROCESSED` stream.

* **Request:**
  `GET http://127.0.0.1:8000/api/datalake/streams/TRANSACTIONS_PROCESSED/?page=2`
* **Success Response (200 OK):**

```json
{
    "topic": "TRANSACTIONS_PROCESSED",
    "count": 394,
    "total_pages": 40,
    "current_page": 2,
    "results": [ ... 10 messages from page 2 ... ]
}
```


#### Example 2: (Q4) Projection

Retrieve only the `user_id`, `amount`, and `status` for all messages.

* **Request:**
  `GET http://127.0.0.1:8000/api/datalake/streams/TRANSACTIONS_PROCESSED/?fields=user_id,amount,status`
* **Success Response (200 OK):**

```json
{
    "topic": "TRANSACTIONS_PROCESSED",
    "count": 394,
    ...
    "results": [
        {
            "user_id": "XXXX-nnnn",
            "amount": 338.0375,
            "status": "cancelled"
        },
        {
            "user_id": "XXXX-nnnn",
            "amount": 345.42,
            "status": "pending"
        },
        ... 8 more ...
    ]
}
```


#### Example 3: (Q5) Filtering

Retrieve all transactions that are `completed`, in `Canada`, and have a rating greater than `4`.

* **Request:**
  `GET http://127.0.0.1:8000/api/datalake/streams/TRANSACTIONS_PROCESSED/?status=completed&country=Canada&rating_gt=4`
* **Success Response (200 OK):**

```json
{
    "topic": "TRANSACTIONS_PROCESSED",
    "count": 5,
    "total_pages": 1,
    "current_page": 1,
    "results": [ ... 5 matching messages ... ]
}
```

---



## III. Metrics (Data Warehouse)

This set of endpoints provides high-speed access to pre-calculated metrics. Unlike the Data Lake endpoints (Part II), these endpoints do **not** read from raw files. Instead, they query the **MySQL Data Warehouse** (`tp3` database), which is populated by the batch ETL pipeline (TP3).

This provides extremely fast responses for dashboards and analytics, as all the heavy computation (`GROUP BY`, `SUM`) has already been performed by ksqlDB and our Beam pipeline.

**Authentication:** All metric endpoints are protected by Token Authentication and require the user to have the `view_metrics` permission.

---

### Endpoint: Get Total Spent by User and Type

* **Question:** `Get total spent per user and transaction type`
* **Endpoint:** `GET /api/metrics/total-by-user/`
* **Description:** Retrieves the lifetime spending totals for all users, broken down by transaction type. This data is read directly from the `total_depense_par_user_type` aggregate table in the Data Warehouse.
* **Query Parameters:** `None`
* **Success Response (200 OK):**

```json
[
    {
        "user_id": "XXXX-nnnn",
        "transaction_type": "purchase",
        "total_depense_usd": "22588.24",
        "last_updated_at": "2025-11-09T21:13:46Z"
    },
    {
        "user_id": "XXXX-nnnn",
        "transaction_type": "refund",
        "total_depense_usd": "23990.86",
        "last_updated_at": "2025-11-09T21:13:45Z"
    },
    {
        "user_id": "YYYY-oooo",
        "transaction_type": "purchase",
        "total_depense_usd": "1500.20",
        "last_updated_at": "2025-11-09T21:10:11Z"
    }
]
```


### Endpoint: Get Top X Products

* **Question:** `Get the top x product bought`
* **Endpoint:** `GET /api/metrics/top-products/`
* **Description:** Returns a ranked list of the most purchased products. Data is read from the `product_purchase_counts` table.
* **Query Parameters:**
  * `x` (integer, optional): The number of top products to return. **Default:** 10.
* **Sample Request:**
  `GET http://127.0.0.1:8000/api/metrics/top-products/?x=3`
* **Success Response (200 OK):**
  *(Note: This list will be empty (`[]`) until your ETL pipeline (from TP3) runs successfully and populates the `product_purchase_counts` table.)*

```json
[
    {
        "product_id": "PROD-707",
        "purchase_count": 8,
        "last_updated_at": "2025-11-09T21:13:46Z"
    },
    {
        "product_id": "PROD-293",
        "purchase_count": 7,
        "last_updated_at": "2025-11-09T21:13:46Z"
    },
    {
        "product_id": "PROD-116",
        "purchase_count": 5,
        "last_updated_at": "2025-11-09T21:13:46Z"
    }
]
```


### Endpoint: Get Money Spent (Last 5 Minutes)

* **Question:** `Get money spent the last 5 minutes`
* **Endpoint:** `GET /api/metrics/last-5-min/`
* **Description:** Returns the total amount spent, grouped by transaction type, for the **most recent 5-minute sliding window** calculated by ksqlDB. Data is read from the `total_par_type_5min_sliding` table.
* **Query Parameters:** `None`
* **Success Response (200 OK):**
  *(Note: This list will also be empty (`[]`) until your ETL pipeline runs and populates the `total_par_type_5min_sliding` table.)*

```json
[
    {
        "transaction_type": "purchase",
        "total_amount_5min": "15234.50",
        "window_start_time": "2025-11-09T21:10:00Z"
    },
    {
        "transaction_type": "refund",
        "total_amount_5min": "890.22",
        "window_start_time": "2025-11-09T21:10:00Z"
    },
    {
        "transaction_type": "withdrawal",
        "total_amount_5min": "4350.00",
        "window_start_time": "2025-11-09T21:10:00Z"
    }
]
```

---



## IV. Data Lineage, Audit, and Logs

This section of the API provides endpoints for auditing, discovery, and data lineage. It allows administrators to see who accessed data and all users to discover available datasets.

### Endpoint: Get Audit Logs

* **Question:** `Get Who queried or accessed a specific data/table data.`
* **Endpoint:** `GET /api/audit-logs/`
* **Description:** Retrieves a paginated list of all API requests, captured by the `AuditLogMiddleware`. This endpoint is restricted to Admin users.
* **Authentication:** Requires **Admin** Token (`is_staff=True`).
* **Query Parameters (Optional):**
  * `page` (int): The page number to retrieve (e.g., `?page=2`).
  * `path` (string): Filters the logs for requests containing this path (e.g., `?path=/api/products/`).
  * `user_id` (int): Filters the logs for a specific user ID (e.g., `?user_id=2`).
* **Success Response (200 OK):**

```json
{
    "count": 150,
    "total_pages": 8,
    "current_page": 1,
    "results": [
        {
            "id": 150,
            "timestamp": "2025-11-09T22:30:01.123Z",
            "user": "kzm",
            "path": "/api/metrics/top-products/?x=3",
            "method": "GET",
            "request_body": ""
        },
        {
            "id": 149,
            "timestamp": "2025-11-09T22:29:50.456Z",
            "user": "test_user",
            "path": "/api/products/",
            "method": "GET",
            "request_body": ""
        },
        ... 18 more ...
    ]
}
```


### Endpoint: List Data Lake Resources

* **Question:** `Get the list of all the ressources available in my data lake`
* **Endpoint:** `GET /api/datalake/resources/`
* **Description:** Scans the `data_lake` directory on the server and returns a list of all available "stream" and "table" resources that can be queried.
* **Authentication:** Requires Token Authentication and the `view_datalake` permission.
* **Success Response (200 OK):**

```json
{
    "data_lake_root": "/path/to/your/Construction_data_lake_data_warehouse/data_lake",
    "available_resources": {
        "streams": [
            "TRANSACTIONS_BLACKLISTED",
            "TRANSACTIONS_FLAT",
            "TRANSACTIONS_PROCESSED",
            "TRANSACTIONS_COMPLETED",
            ...
        ],
        "tables": [
            "PRODUCT_PURCHASE_COUNTS",
            "TOTAL_DEPENSE_PAR_USER_TYPE",
            "TRANSACTION_LIFECYCLE",
            ...
        ]
    }
}
```


### Endpoint: Get Specific Data Version (Versioning)

* **Question:** `Get a specific version of the stored data...`
* **Endpoint:** `GET /api/datalake/streams/<str:topic_name>/`
* **Description:** This feature is implemented as part of the main Data Lake endpoint (from Part II). By providing `year`, `month`, and `day` query parameters, you are requesting a specific "version" of the data.
* **Authentication:** Requires Token Authentication and the `view_datalake` permission.
* **Query Parameters:**
  * `year` (int): The year of the data to retrieve (e.g., `2025`).
  * `month` (int): The month (e.g., `10`).
  * `day` (int): The day (e.g., `28`).
* **Sample Request:**
  `GET http://127.0.0.1:8000/api/datalake/streams/TRANSACTIONS_PROCESSED/?year=2025&month=10&day=28`
* **Success Response (200 OK):**

```json
{
    "topic": "TRANSACTIONS_PROCESSED",
    "count": 120,
    "total_pages": 12,
    "current_page": 1,
    "results": [ ... 10 messages only from Oct 28, 2025 ... ]
}
```

* **Error Response (404 Not Found):**
  If a version (date) that does not exist is requested:
  `GET http://127.0.0.1:8000/api/datalake/streams/TRANSACTIONS_PROCESSED/?year=1999&month=1&day=1`

```json
{
    "error": "Version (date) non trouvée : 1999-01-01"
}
```

---



## V. Advanced Capabilities

This section covers advanced endpoints that go beyond simple data retrieval. These include full-text search, RPC-style triggers, and data replay capabilities for pipeline management.

### Endpoint 1: Full-Text Search

* **Question:** `Full-text search across data... Our data lake is not designed for this, so proposed a technology...`
* **Endpoint:** `POST /api/search/`
* **Description:** Performs a brute-force, full-text search by iterating through all JSON files in the Data Lake that are newer than the `start_date`. This endpoint is **intentionally slow** to demonstrate the limitations of using a file-based Data Lake for search queries.
* **Authentication:** Requires Token Authentication and the `search_datalake` permission.
* **Body (JSON):**

```json
{
    "search_term": "TXN-9521c58f",
    "start_date": "2025-01-01"
}
```

* **Success Response (200 OK):**
  The response includes the search results and a recommendation for a more suitable technology.

```json
{
    "search_term": "TXN-9521c58f",
    "recommendation": "Attention: Cette recherche a été effectuée en scannant les fichiers JSON, ce qui est très lent et inefficace. Pour des performances de recherche en temps réel, il est recommandé d'indexer ces données dans un moteur de recherche dédié comme Elasticsearch ou OpenSearch.",
    "results_found": 2,
    "files": [
        {
            "resource": "/streams/TRANSACTIONS_FLAT/year=2025/month=10/day=28",
            "file": "data_offset_123.json"
        },
        {
            "resource": "/streams/TRANSACTIONS_PROCESSED/year=2025/month=10/day=28",
            "file": "data_offset_456.json"
        }
    ]
}
```


### Endpoint 2: RPC (Remote Procedure Call) - Trigger ML Training

* **Question:** `RPC (remote procedure call) - add an endpoint that will trigger the training of a machine learning...`
* **Endpoint:** `POST /api/rpc/train-model/`
* **Description:** Triggers a simulated, long-running background task (like training a machine learning model). This endpoint uses **asynchronous execution** (via `threading`) to immediately return a response to the user, while the task runs on the server.
* **Authentication:** Requires Token Authentication and the `trigger_ml_model` permission.
* **Body (JSON):**

```json
{
    "model_type": "fraud_detection_v3"
}
```

* **Success Response (202 Accepted):**
  The API immediately responds with `202 Accepted` to indicate the job was received and started.

```
{
    "status": "success",
    "message": "Entraînement du modèle fraud_detection_v3 démarré en arrière-plan."
}
```

* **Server Log (The real action):**
  The Django server console will show:
  `WARNING:--- DÉCLENCHEMENT RPC ---`
  `WARNING:Demande d'entraînement pour le modèle 'fraud_detection_v3' reçue de l'utilisateur kzm.`


### Endpoint 3: Re-push Single Transaction

* **Question:** `Re-push a transaction - Let’s suppose you have a corrupted transactions...`
* **Endpoint:** `POST /api/data/repush-transaction/`
* **Description:** Finds a single transaction by its ID from anywhere in the Data Lake, updates its timestamp to "now", and publishes it back to the `transaction_log` Kafka topic to be re-processed by the entire pipeline.
* **Authentication:** Requires Token Authentication and the `repush_transaction` permission.
* **Body (JSON):**

```json
{
    "transaction_id": "TXN-146bfc38"
}
```

* **Success Response (200 OK):**

```json
{
    "status": "success",
    "message": "Transaction TXN-146bfc38 re-poussée dans transaction_log.",
    "new_data": {
        "TRANSACTION_ID": "TXN-146bfc38",
        "TIMESTAMP": "2025-11-03T08:51:32.767200Z",
        "USER_ID": "XXXX-nnnn",
        "USER_NAME": "Xxxx",
        "PRODUCT_ID": "PROD-518",
        "AMOUNT_USD": 671.15,
        "CURRENCY": "USD",
        "TRANSACTION_TYPE": "purchase",
        "STATUS": "processing",
        "LOCATION_CITY": "Marseille",
        "LOCATION_COUNTRY": "France",
        "PAYMENT_METHOD": "apple_pay",
        "PRODUCT_CATEGORY": "electronics",
        "QUANTITY": 3,
        "SHIPPING_STREET": "nnn-Xxxx-Xx",
        "SHIPPING_ZIP": "nnnnn",
        "SHIPPING_CITY": "Marseille",
        "SHIPPING_COUNTRY": "France",
        "DEVICE_OS": "Android",
        "DEVICE_BROWSER": "Firefox",
        "DEVICE_IP": "nnn-nnn-nn-nn",
        "CUSTOMER_RATING": null,
        "DISCOUNT_CODE": "DISCOUNT-562",
        "timestamp": "2025-11-10T15:40:17.020653+00:00Z",
        "re-pushed_from_api": true
    }
}
```


### Endpoint 4: Re-push All Transactions

* **Question:** `Repush all- push all historical product to the beginning of the pipeline`
* **Endpoint:** `POST /api/data/repush-all/`
* **Description:** (DANGEROUS) Triggers an asynchronous background task that scans the *entire* Data Lake (specifically the `TRANSACTIONS_PROCESSED` stream), reads every JSON file, and re-publishes it to the `transaction_log` Kafka topic. This is used to re-populate or re-process the entire system.
* **Authentication:** Requires Token Authentication and the `repush_all_transactions` permission (recommended for Superusers only).
* **Body (JSON):** (empty)
* **Success Response (202 Accepted):**

```json
{
    "status": "Job démarré",
    "message": "Le re-push de tout l'historique a démarré en arrière-plan."
}
```

* **Server Log:**
  `WARNING:DÉBUT du Re-push total demandé par kzm`
  `WARNING:FIN du Re-push total. 394 messages ont été re-poussés.`

---



## VI. API Documentation (Swagger/ReDoc)

As required by the assignment, a dynamic API documentation has been implemented using `drf-yasg`. This tool automatically generates an interactive **Swagger (OpenAPI)** specification by inspecting the API's code.

This documentation includes:

* Every endpoint URL.
* The required HTTP method (GET/POST/DELETE).
* Input parameters (Body fields, Query parameters like `?page=`).
* Output (response) descriptions.
* An interactive UI to test all endpoints directly from the browser (authentication is required).

### Accessing the Documentation

With the server running (`python manage.py runserver`), the auto-generated documentation is available at the following URLs:

* **Swagger UI:**
  `http://127.0.0.1:8000/swagger/`
* **ReDoc UI (Alternative):**
  `http://127.0.0.1:8000/redoc/`

---



## License

This project is licensed under the  **ISC License** .

Project by Kévin HEUGAS.
