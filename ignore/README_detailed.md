# MultiAvro Stream Consumer - Detailed Project Guide

## Overview
This Spring Boot application consumes Avro-encoded messages from multiple Kafka topics, processes and filters them, and ingests the results into a relational database. It is designed for dynamic schema handling, automated table creation, time-based filtering, and secure integration with Azure Key Vault and Data Lake.

---

## Project Structure & Main Logic

### 1. `MultiAvroStreamConsumerApplication.java`
- **Purpose:** Main entry point for the application.
- **Main Method:** `public static void main(String[] args)`
  - Parses command-line arguments for time-based filtering.
  - Sets system properties for time window.
  - Updates the configuration table with user-provided time window.
  - Starts the Spring Boot context and application.

### 2. `ConfigService.java`
- **Purpose:** Loads all consumer configurations from the database and builds `AvroConsumerConfig` objects.
- **Main Method:** `public List<AvroConsumerConfig> getAllConfigurations()`
  - Fetches configuration rows from the `onemed_partialload_configuration` table.
  - Applies runtime overrides (start/end timestamp from user input).
  - Returns a list of configuration objects for use by the rest of the application.

### 3. `DatabaseService.java`
- **Purpose:** Handles all database operations.
- **Main Methods:**
  - `public void ensureTargetTable(Schema avroSchema, String tableName)`
    - Checks if the target table exists and matches the Avro schema.
    - Creates or alters the table as needed.
  - `public void insertRecords(List<GenericRecord> avroRecords, String tableName)`
    - Inserts processed records into the target table.
  - `public void updateTimeWindowForTopic(String topic, Long startTimestamp, Long endTimestamp)`
    - Updates the time window columns in the configuration table based on user input.

### 4. `KafkaStreamsService.java`
- **Purpose:** Core logic for consuming, filtering, and processing Kafka messages.
- **Main Method:** `public void processKafkaStreamsColumnFilter(AvroConsumerConfig config)`
  - Fetches the Avro schema for the topic.
  - Ensures the target table is up to date.
  - Consumes messages, applies filters, and inserts data.
  - Handles time-based filtering and parallel processing.

### 5. `AvroConsumerConfig.java`
- **Purpose:** POJO that holds all configuration for a single Kafka consumer instance.
- **Main Methods:** Getters and setters for all configuration fields (topic, target table, schema registry, filters, time window, etc.).
- **Usage:** Used throughout the application to pass configuration details to services.

### 6. `KeyVaultService.java`
- **Purpose:** Handles secure retrieval of secrets (like credentials) from Azure Key Vault.
- **Main Method:** `public String getSecret(String secretName)`
  - Fetches secrets for secure configuration and authentication.

### 7. `ErrorRecord.java`
- **Purpose:** Simple model class to represent error records (failed message and error message).
- **Main Methods:** Getters for record and error message.
- **Usage:** Used for error tracking and reporting.

### 8. `configservice.java`
- **Purpose:** (Legacy or duplicate file; not used in main logic. Use `ConfigService.java`.)

---

## Database Setup

### 1. Create Configuration Table
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
    start_timestamp BIGINT,
    end_timestamp BIGINT
);
```

### 2. Insert Configuration Example
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
- `start_timestamp` and `end_timestamp` are optional and will be set by the application if provided at runtime.

### 3. Target Table Creation
**No manual creation needed!**
The application will automatically create or update the target table (named after the topic) to match the Avro schema at runtime.

---

## Time Window Automation
If you run the application with `--startTimestamp` and/or `--endTimestamp` command-line arguments, the application will automatically update the `start_timestamp` and `end_timestamp` columns in the `onemed_partialload_configuration` table for all active topics at startup. This ensures the database always reflects the time window used for each run.

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
