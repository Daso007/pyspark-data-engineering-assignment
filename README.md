# PySpark Data Engineering Assignment - Transaction Pattern Detection

## Dibyajyoti Datta
**Submission for PySpark Data Engineer Role**

## Table of Contents
1.  [Project Overview]
2.  [Architecture]
    *   [Mechanism X (Data Ingestion)]
    *   [Mechanism Y (Pattern Detection & State Management)]
    *   [Data Flow Diagram]
3.  [Tech Stack]
4.  [Setup Instructions]
    *   [AWS Setup]
        *   [S3 Buckets]
        *   [IAM User & Policy]
        *   [RDS PostgreSQL Instance]
    *   [Local Environment (for Mechanism X)]
    *   [Databricks Setup (for Mechanism Y)]
    *   [Data Files]
5.  [How to Run]
    *   [1. Start Mechanism X]
    *   [2. Start Mechanism Y]
6.  [Pattern Definitions]
7.  [Key Assumptions & Design Choices]
8.  [Limitations & Potential Future Improvements]
9.  [Repository Structure]

## 1. Project Overview

This project implements a data pipeline to simulate real-time processing of financial transactions. It consists of two main components:

*   **Mechanism X:** A Python script that reads a large transaction dataset (`transactions.csv`), creates chunks of 10,000 entries, and ingests these chunks into an AWS S3 bucket every second.
*   **Mechanism Y:** A Databricks PySpark streaming application that ingests these transaction chunks from S3 as they become available. It enriches the data, detects predefined fraudulent or noteworthy patterns using state maintained in AWS RDS PostgreSQL, and outputs these detections to another S3 bucket in batches of 50.

The primary goal is to demonstrate a scalable data engineering pipeline using PySpark, S3, and PostgreSQL for near real-time pattern detection.

## 2. Architecture

### Mechanism X (Data Ingestion)
*   A standalone Python script (`mechanism_x/mechanism_x.py`).
*   Reads `transactions.csv` locally (simulating a data source like Google Drive).
*   Uses `pandas` to read the CSV in chunks of 10,000 rows.
*   Uses `boto3` (AWS SDK for Python) to upload each chunk as a new `.csv` file to a designated S3 input bucket (`s3a://<your-name>-transaction-chunks/`).
*   A `time.sleep(1)` is implemented between uploads to simulate a 1-second invocation frequency.

### Mechanism Y (Pattern Detection & State Management)
*   A PySpark Structured Streaming application running on Databricks (`mechanism_y/mechanism_y_final_spark_code.py`).
*   **Input:** Reads new chunk files from the S3 input bucket as a stream.
*   **State Management (Temporary Information):** AWS RDS for PostgreSQL (Version 14.x used for this implementation) stores aggregated summaries required for pattern detection. These include:
    *   Total transaction counts per merchant.
    *   Transaction counts and total transaction values per customer-merchant pair.
    *   Male/Female transaction counts per merchant.
    *   In each micro-batch, Mechanism Y calculates batch-level aggregates, writes them to temporary tables in PostgreSQL, and then uses `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) statements via raw JDBC calls to update the main summary tables.
*   **Enrichment:** Joins streaming transaction data with static `CustomerImportance.csv` data (loaded from DBFS) to get customer weights for certain patterns. Pre-calculates percentile weights from this file.
*   **Pattern Detection:**
    *   Reads the updated full state from PostgreSQL summary tables in each micro-batch.
    *   Applies logic to detect three specific patterns (PatId1, PatId2, PatId3) based on this state and enriched batch data.
*   **Output:**
    *   Detected patterns are collected.
    *   Results are written to a designated S3 output bucket (`s3a://<your-name>-detection-outputs/`) in unique `.csv` files, with each file containing up to 50 detections.
    *   Output format: `YStartTime(IST), detectionTime(IST), patternId, ActionType, customerName, MerchantId`.





## 3. Tech Stack
*   **Programming Language:** Python 3.x, PySpark
*   **Cloud Platform:** AWS
    *   **Storage:** S3 (for raw data chunks, archived data, detection outputs, checkpoints)
    *   **Database:** RDS for PostgreSQL (Version 14.10 for state management)
*   **Data Processing:** Apache Spark (via Databricks)
    *   Spark Structured Streaming
    *   Spark SQL & DataFrame API
*   **Development Environment:**
    *   Local machine for Mechanism X (Python, pandas, boto3)
    *   Databricks for Mechanism Y (Notebooks, Cluster with PySpark)
*   **Database Client:** DBeaver (for interacting with PostgreSQL)

## 4. Setup Instructions

### Prerequisites
*   AWS Account (Free Tier eligible recommended for testing).
*   Python 3.x installed locally.
*   Databricks Account (Community Edition or standard workspace).
*   Git installed locally (for cloning repository).
*   Docker installed locally (Alternative for running PostgreSQL if not using RDS, but RDS was used for this project).

### AWS Setup

#### S3 Buckets
Create the following S3 buckets in your preferred AWS region (e.g., `ap-south-1`):
1.  `your-name-de-assignment-transaction-chunks`: For Mechanism X to upload transaction chunks.
    *   Inside this bucket, manually create a folder named `archive/` if `cleanSource` is to be used by Spark (see [Assumptions](#key-assumptions--design-choices)).
2.  `your-name-de-assignment-detection-outputs`: For Mechanism Y to store detected patterns.
3.  `your-name-de-assignment-auxiliary-data`: For storing Spark checkpoints and potentially `CustomerImportance.csv`.

*(Replace `your-name` with a unique identifier)*

#### IAM User & Policy
1.  Create an IAM User with programmatic access (Access Key ID and Secret Access Key).
2.  Attach a policy to this user (or to an IAM Role if using Instance Profiles with Databricks) granting necessary S3 permissions. Example policy JSON:
    ```json
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListBucketsInConsole",
            "Effect": "Allow",
            "Action": [
                "s3:ListAllMyBuckets",
                "s3:GetBucketLocation"
            ],
            "Resource": "*"
        },
        {
            "Sid": "AllowReadWriteToAppBuckets",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::daso-de-assignment-transaction-chunks",
                "arn:aws:s3:::daso-de-assignment-transaction-chunks/*",
                "arn:aws:s3:::daso-de-assignment-detection-outputs",
                "arn:aws:s3:::daso-de-assignment-detection-outputs/*",
                "arn:aws:s3:::daso-de-assignment-auxiliary-data",
                "arn:aws:s3:::daso-de-assignment-auxiliary-data/*"
            ]
        }
    ]

 '''   

#### RDS PostgreSQL Instance
1.  Launch an AWS RDS for PostgreSQL instance.
    *   **Engine version:** PostgreSQL 14.x (e.g., 14.10, as `MERGE` is not used; `INSERT ON CONFLICT` is used).
    *   **Template:** "Free tier" for testing.
    *   **DB instance identifier:** e.g., `de-assignment-pg-db`
    *   **Master username:** e.g., `postgres`
    *   **Master password:** Choose a strong password.
    *   **Public access:** Set to "Yes" for easier connection from Databricks/local DBeaver during development.
    *   **Initial database name:** `transactions_db`
2.  **Security Group:** Modify the inbound rules of the RDS instance's security group:
    *   Allow TCP traffic on port `5432` from your local machine's IP (for DBeaver access).
    *   Allow TCP traffic on port `5432` from Databricks (can use `0.0.0.0/0` for simplicity in a dev environment, but restrict in production).
3.  **Create Tables:** Connect to the RDS instance using DBeaver (or psql) and run the DDL statements from `sql_schema/postgres_tables.sql` to create the three summary tables:
    *   `merchant_transaction_summary`
    *   `customer_merchant_summary`
    *   `merchant_gender_summary`

### Local Environment (for Mechanism X)
1.  Ensure Python 3.x is installed.
2.  Install required libraries: `pip install pandas boto3`
3.  Configure AWS credentials for boto3, preferably using `aws configure`.
4.  Place `transactions.csv` in the `mechanism_x/` directory or update its path in `mechanism_x.py`.

### Databricks Setup (for Mechanism Y)
1.  **Cluster Configuration:**
    *   Choose a Databricks Runtime (e.g., 13.3 LTS with Spark 3.4.1, Scala 2.12).
    *   **S3 Access:** Configure S3 access keys in the cluster's Spark configuration if using Databricks Community Edition or not using instance profiles:
        ```ini
        spark.hadoop.fs.s3a.access.key YOUR_AWS_ACCESS_KEY_ID
        spark.hadoop.fs.s3a.secret.key YOUR_AWS_SECRET_ACCESS_KEY
        spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
        # Optional: If S3AFileSystem class not found, add Hadoop AWS JARs
        # spark.jars.packages com.amazonaws:aws-java-sdk-bundle:1.12.x,org.apache.hadoop:hadoop-aws:3.3.x
        ```
    *   **Libraries:** Install the PostgreSQL JDBC driver via Maven coordinates on the cluster:
        *   Coordinates: `org.postgresql:postgresql:42.5.0` (or a newer compatible version).

### Data Files
*   `transactions.csv`: Provided dataset, used by Mechanism X.
*   `CustomerImportance.csv`: Provided dataset. Upload this to Databricks DBFS (e.g., `/FileStore/CustomerImportance.csv`) or to an S3 path accessible by Mechanism Y. Update `CUSTOMER_IMPORTANCE_PATH` in the Spark script accordingly.

## 5. How to Run

### 1. Start Mechanism X
1.  Navigate to the `mechanism_x/` directory in your local terminal.
2.  (If using a virtual environment, activate it).
3.  Run the script: `python mechanism_x.py`
4.  Monitor the terminal output and the S3 input bucket (`your-name-de-assignment-transaction-chunks`) to see chunks being uploaded.

### 2. Start Mechanism Y
1.  Ensure your Databricks cluster is running and configured as per setup.
2.  Ensure PostgreSQL summary tables are created and empty (`TRUNCATE` them).
3.  Ensure `Mechanism_X` is running or has populated the S3 input bucket with fresh data.
4.  Open the `mechanism_y/mechanism_y_final_spark_code.py` notebook in Databricks.
5.  Verify all configuration paths (S3, PG credentials, checkpoint path - **use a new checkpoint path for each major test run**) are correct.
6.  Run the notebook cells. The streaming query will start.
7.  Monitor:
    *   Notebook output for batch processing logs, UPSERT confirmations, and detection counts.
    *   Spark UI (Streaming tab).
    *   PostgreSQL tables in DBeaver for updated state.
    *   S3 output bucket (`your-name-de-assignment-detection-outputs`) for detection files.
    *   S3 archive folder (if `cleanSource` is enabled) for processed input files.

## 6. Pattern Definitions

*(Briefly restate the logic for PatId1, PatId2, PatId3 as implemented)*

*   **PatId1 - UPGRADE:**
    *   Conditions: Merchant's total transactions > `PATID1_MERCHANT_TX_THRESHOLD`. Customer's transactions with merchant > `PATID1_CUSTOMER_TX_THRESHOLD_PER_MERCHANT`. Customer's transaction `Weight` (from CustomerImportance) is below the 1st percentile of weights for that merchant and transaction category (or a fixed low value if percentile calculation fails).
    *   Action: `UPGRADE`
*   **PatId2 - CHILD:**
    *   Conditions: Customer's average transaction value with a merchant < `PATID2_AVG_AMOUNT_THRESHOLD` AND customer has made at least `PATID2_MIN_TRANSACTIONS` with that merchant.
    *   Action: `CHILD`
*   **PatId3 - DEI-NEEDED:**
    *   Conditions: For a merchant, total Female customer transactions < total Male customer transactions AND total Female customer transactions > `PATID3_MIN_FEMALE_TRANSACTIONS`.
    *   Action: `DEI-NEEDED`

## 7. Key Assumptions & Design Choices

*   **Source Data Location:** `transactions.csv` is read locally by Mechanism X; `CustomerImportance.csv` is loaded from DBFS by Mechanism Y. This simulates the provided GDrive links.
*   **`cleanSource` (S3 File Archiving):** *(State whether you got this working or had to disable it due to environment issues like `IndexOutOfBoundsException` from `FileStreamSource$SourceFileArchiver` or "different file system" errors, despite trying standard configurations.)* If disabled, input files in S3 are not moved after processing but are managed by the streaming checkpoint.
*   **Pattern Thresholds:** Reduced thresholds were used for easier triggering during development and demo (e.g., `PATID1_MERCHANT_TX_THRESHOLD = 5` instead of 50,000). These are configurable in the script.
*   **State Management in PostgreSQL:**
    *   AWS RDS PostgreSQL 14.x is used.
    *   UPSERTs are handled using `INSERT ... ON CONFLICT DO UPDATE` executed via raw JDBC calls, after writing batch aggregates to temporary PG tables. This ensures atomic updates to the state tables.
*   **`CustomerImportance` Percentiles:** The 1st percentile of `Weight` is pre-calculated from the static `CustomerImportance.csv` file.
*   **Error Handling:** Basic `try-except` blocks are used for critical operations like DB interactions and file loading. Fallbacks (like using empty DataFrames or fixed values) are implemented where appropriate.

## 8. Limitations & Potential Future Improvements

*   **`cleanSource` Issue (If applicable):** If `cleanSource` could not be reliably enabled, implement a custom post-batch file move strategy using `boto3` or `dbutils.fs.mv`.
*   **Advanced Percentiles:** For PatId1, the "top 1 percentile of customer transactions" is simplified. A more robust solution would involve storing more detailed data or using approximate percentile algorithms directly on streaming data or larger state tables.
*   **Scalability of Raw JDBC Calls:** For very high-throughput scenarios, managing many individual JDBC connections/statements from the Spark driver might become a bottleneck. Consider connection pooling or batching JDBC statements more effectively if this were a production system at massive scale.
*   **Error Recovery for JDBC UPSERTs:** While errors are caught, more sophisticated retry mechanisms or dead-letter queues for failed DB updates could be implemented.
*   **Schema Evolution for Input/State:** Not explicitly handled. In production, schema changes would need a management strategy.
*   **Security:** Passwords and keys are currently in the script for simplicity; in production, use Databricks secrets, environment variables, or IAM roles/Instance Profiles.

## 9. Repository Structure

.
├── mechanism_x/
│ └── mechanism_x.py
├── mechanism_y/
│ └── mechanism_y_final_spark_code.py
├── sql_schema/
│ └── postgres_tables.sql
├── README.md
└── .gitignore
