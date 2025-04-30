# MultiAvro Stream Consumer

## Overview
This application is a Spring Boot service that consumes Avro-encoded messages from multiple Kafka topics, processes and filters them, and ingests the results into a relational database. It supports dynamic schema handling, automated table creation, time-based filtering, and secure integration with Azure Key Vault and Data Lake.

---

## Features
- **Multi-topic Kafka Avro consumer**
- **Dynamic schema support**: Automatically creates/updates target tables to match Avro schema
- **Time-based filtering**: Consume messages within a specified time window
- **Configurable via database**: All consumer settings are managed in a DB table
- **Secure secret management**: Integrates with Azure Key Vault
- **Parallel processing**: Consumes multiple topics/configs in parallel

---

## Prerequisites
- Java 8+
- Maven
- PostgreSQL (or compatible RDBMS)
- Kafka cluster with Schema Registry
- Azure Key Vault (if using secret management)

---

## Database Setup

### 1. Create Configuration Table
This table holds all consumer configurations. The application expects the table to be named `onemed_partialload_configuration`.

```sql
CREATE TABLE onemed_partialload_configuration (
    id SERIAL PRIMARY KEY,
    bootstrap_servers VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    group_name VARCHAR(255) NOT NULL,
    max_records INTEGER NOT NULL,
    session_timeout_ms INTEGER NOT NULL,
    oauthbearer_url VARCHAR(255) NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    client_secrets VARCHAR(255) NOT NULL,
    source_type VARCHAR(50),
    row_table VARCHAR(255),
    extensionlogical_cluster VARCHAR(255),
    extension_identity_pool_id VARCHAR(255),
    active CHAR(1) NOT NULL DEFAULT 'N',
    schema_registry VARCHAR(255) NOT NULL,
    offset_reset_config VARCHAR(50),
    target_table VARCHAR(255),
    column_filters VARCHAR(255),
    row_filters VARCHAR(255),
    start_timestamp BIGINT,   -- optional, for time-based filtering
    end_timestamp BIGINT      -- optional, for time-based filtering
);
```

### 2. Insert Configuration Example
Insert a row for each topic you want to consume. Example:

```sql
INSERT INTO onemed_partialload_configuration (
    bootstrap_servers, topic, group_name, max_records, session_timeout_ms, oauthbearer_url, client_id, client_secrets,
    source_type, row_table, extensionlogical_cluster, extension_identity_pool_id, active, schema_registry, offset_reset_config,
    target_table, column_filters, row_filters, start_timestamp, end_timestamp
) VALUES (
    'localhost:9092',
    'my_topic',
    'my_group',
    1000,
    30000,
    'https://my-oauth-url',
    'my-client-id',
    'my-client-secret',
    'kafka',
    NULL,
    NULL,
    NULL,
    'Y',
    'http://localhost:8081',
    'earliest',
    NULL,
    '',
    '',
    NULL, -- start_timestamp (set by app if provided at runtime)
    NULL  -- end_timestamp (set by app if provided at runtime)
);
```

- `topic` and `target_table` should match (the app will use topic name as table name for ingestion).
- `start_timestamp` and `end_timestamp` are optional. If provided, only messages within this window are consumed.

### 3. Target Table Creation
**No manual creation needed!**
The application will automatically create or update the target table (named after the topic) to match the Avro schema at runtime.

---

## Time Window Automation

If you run the application with `--startTimestamp` and/or `--endTimestamp` command-line arguments, the application will automatically update the `start_timestamp` and `end_timestamp` columns in the `onemed_partialload_configuration` table for all active topics at startup. This ensures the database always reflects the time window used for each run, and you do not need to manually update these columns.

**Example:**
```bash
java -jar target/multiavro-1.0-SNAPSHOT.jar --startTimestamp=1680307200000 --endTimestamp=1680393600000
```

---

## Running the Application

### 1. Build
```bash
mvn clean package
```

### 2. Run
You can run the application with or without time-based filtering:

**Without time filter:**
```bash
java -jar target/multiavro-1.0-SNAPSHOT.jar
```

**With time filter (epoch millis):**
```bash
java -jar target/multiavro-1.0-SNAPSHOT.jar --startTimestamp=1680307200000 --endTimestamp=1680393600000
```
- Command-line arguments override DB values for time window and update the DB automatically.

---

## Notes
- The application will fetch the Avro schema for each topic from the schema registry and ensure the target table matches the schema.
- All secrets (e.g., client_id, client_secrets) can be securely managed via Azure Key Vault.
- For each topic, a new table will be created/updated in the database with the same name as the topic.
- Error and batch status tables are managed internally; see code for details if you wish to customize.

---

## Troubleshooting
- Ensure the database user has permission to create/alter tables.
- Ensure the Kafka and schema registry endpoints are reachable from the application host.
- Check logs for detailed error messages if ingestion or table creation fails.

---

## Contact
For questions or support, contact the project maintainer or open an issue in your repository.
