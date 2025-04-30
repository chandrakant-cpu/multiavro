package com.multiavrostreamconsumer;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.multiavrostreamconsumer.ErrorRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DatabaseService {

    private static final Logger logger = LogManager.getLogger(DatabaseService.class);

    private final JdbcTemplate jdbcTemplate;

    private static final int BATCH_SIZE = 100000; // Adjust based on performance testing
    private static final int THREAD_POOL_SIZE = 100; // Set based on your system's available cores

    public DatabaseService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertToTarget(List<GenericRecord> records, String tableName, String batchId) {
        if (records.isEmpty()) {
            return;
        }
        // Executor service for parallel processing
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        int totalRecords = records.size();
        for (int i = 0; i < totalRecords; i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, totalRecords);
            List<GenericRecord> batch = records.subList(i, end);
            // Asynchronously process each batch
            executorService.submit(() -> {
                insertRecords(batch, tableName);
            });
        }

        // Shutdown the executor once all tasks are completed
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    // Ensures the target table exists and has all columns from the Avro schema
    public void ensureTargetTable(Schema avroSchema, String tableName) {
        Set<String> avroFields = new HashSet<>();
        Map<String, String> avroFieldTypes = new HashMap<>();
        for (Schema.Field field : avroSchema.getFields()) {
            avroFields.add(field.name());
            avroFieldTypes.put(field.name(), avroTypeToSqlType(field.schema()));
        }
        avroFields.add("pg_load_dt");
        avroFields.add("pg_modify_dt");
        avroFieldTypes.put("pg_load_dt", "TIMESTAMP");
        avroFieldTypes.put("pg_modify_dt", "TIMESTAMP");

        // Check if table exists
        String checkTableSql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + tableName + "'";
        Integer tableExists = jdbcTemplate.queryForObject(checkTableSql, Integer.class);
        if (tableExists == null || tableExists == 0) {
            // Create table
            StringBuilder createSql = new StringBuilder("CREATE TABLE " + tableName + " (");
            int i = 0;
            for (String col : avroFields) {
                if (i++ > 0) createSql.append(", ");
                createSql.append(col).append(" ").append(avroFieldTypes.get(col));
            }
            createSql.append(")");
            jdbcTemplate.execute(createSql.toString());
            logger.info("Created table {} with columns {}", tableName, avroFields);
        } else {
            // Check for missing columns
            String colSql = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + tableName + "'";
            List<String> dbCols = jdbcTemplate.queryForList(colSql, String.class);
            Set<String> dbColSet = new HashSet<>(dbCols);
            for (String col : avroFields) {
                if (!dbColSet.contains(col)) {
                    String alterSql = "ALTER TABLE " + tableName + " ADD COLUMN " + col + " " + avroFieldTypes.get(col);
                    jdbcTemplate.execute(alterSql);
                    logger.info("Added column {} to table {}", col, tableName);
                }
            }
        }
    }

    // Helper to map Avro types to SQL types
    private String avroTypeToSqlType(Schema schema) {
        Schema.Type type = schema.getType();
        switch (type) {
            case STRING: return "VARCHAR(255)";
            case INT: return "INT";
            case LONG: return "BIGINT";
            case FLOAT: return "FLOAT";
            case DOUBLE: return "DOUBLE PRECISION";
            case BOOLEAN: return "BOOLEAN";
            case BYTES: return "BYTEA";
            case RECORD: return "JSONB";
            case ARRAY: return "JSONB";
            case MAP: return "JSONB";
            case UNION:
                // Use the first non-null type
                for (Schema s : schema.getTypes()) {
                    if (s.getType() != Schema.Type.NULL) {
                        return avroTypeToSqlType(s);
                    }
                }
                return "VARCHAR(255)";
            default: return "VARCHAR(255)";
        }
    }

    public void insertRecords(List<GenericRecord> avroRecords, String tableName) {
        if (avroRecords.isEmpty()) {
            return;
        }

        // Fetch column names from the first record
        GenericRecord firstRecord = avroRecords.get(0);
        List<String> columnNames = firstRecord.getSchema().getFields().stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());

        // Add pg_load_dt and pg_modify_dt to the column list
        columnNames.add("pg_load_dt");
        columnNames.add("pg_modify_dt");

        // Build the insert query
        String insertQuery = buildInsertQuery(tableName, columnNames);

        // Use batchUpdate to process records in bulk
        logger.info("Number of records to be inserted: {}", avroRecords.size());
        try {
            jdbcTemplate.batchUpdate(insertQuery, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {
                    GenericRecord record = avroRecords.get(i);
                    int index = 1;

                    // Set values for all columns in the record
                    for (String column : columnNames) {
                        if (column.equals("pg_load_dt") || column.equals("pg_modify_dt")) {
                            // Set current timestamp for pg_load_dt and pg_modify_dt
                            ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
                        } else {
                            Object value = record.get(column);
                            if (value instanceof GenericRecord) {
                                // Serialize nested GenericRecord to JSON string
                                ps.setObject(index++, serializeToJson((GenericRecord) value), Types.OTHER);
                            } else if (value instanceof Utf8) {
                                // Convert Utf8 to String explicitly
                                ps.setString(index++, value.toString());
                            } else if (value == null) {
                                // Handle null values
                                ps.setNull(index++, Types.NULL);
                            } else {
                                // Use setObject for other types
                                ps.setObject(index++, value);
                            }
                        }
                    }
                }
                @Override
                public int getBatchSize() {
                    return avroRecords.size();
                }
            });
        } catch (Exception e) {
            logger.error("Error inserting records into table {}: {}", tableName, e.getMessage());
            e.printStackTrace();
        }
    }

    // Helper method to serialize a GenericRecord to JSON
    private String serializeToJson(GenericRecord record) {
        return record.toString(); // Avro's toString() method produces a JSON representation of the record
    }

    private String buildInsertQuery(String tableName, List<String> columnNames) {
        String columns = String.join(", ", columnNames);
        String placeholders = columnNames.stream().map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
    }

    public void insertToErrorRecords(String batchId, int configId, List<ErrorRecord> errorRecords) {
        try {
            jdbcTemplate.batchUpdate("INSERT INTO error_records (batch_id, error_message, record) VALUES (?, ?, ?)", new BatchPreparedStatementSetter() {
                public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {
                    ps.setString(1, batchId);
                    ps.setObject(2, errorRecords.get(i).getErrorMessage());
                    ps.setObject(3, errorRecords.get(i).getRecord());
                }

                public int getBatchSize() {
                    return errorRecords.size();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertToArchive(String archiveTable, String batchId, List<String> records) {
        try {
            jdbcTemplate.batchUpdate("INSERT INTO " + archiveTable + " (batch_id, record) VALUES (?, ?)", new BatchPreparedStatementSetter() {
                public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {
                    ps.setString(1, batchId);
                    ps.setObject(2, records.get(i));
                }

                public int getBatchSize() {
                    return records.size();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertToBatchStatus(String batchId, Date date, Integer configId, long executionTime, long fetchedRecords, long targetRecords,
                                    long deleteRecords, long errorRecords, long archiveRecords) {
        try {
            jdbcTemplate.update("INSERT INTO batch_status (batch_id, date, config_id, execution_time, fetched_records, target_records, delete_records, error_records, archive_records) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", batchId, date, configId, executionTime, fetchedRecords, targetRecords,
                    deleteRecords, errorRecords, archiveRecords);
            System.out.println("Records inserted to Target Table" + fetchedRecords);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Map<String, Object>> getConfigurations() {
        try {
            String sql = "SELECT * FROM configurations";
            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    // Fetch configurations from a specific table name
    public List<Map<String, Object>> getConfigurationsFromTable(String tableName) {
        try {
            String sql = "SELECT * FROM " + tableName;
            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    // Update start_timestamp and end_timestamp for a topic based on user input
    public void updateTimeWindowForTopic(String topic, Long startTimestamp, Long endTimestamp) {
        if (startTimestamp != null) {
            jdbcTemplate.update("UPDATE onemed_partialload_configuration SET start_timestamp = ? WHERE topic = ?", startTimestamp, topic);
        }
        if (endTimestamp != null) {
            jdbcTemplate.update("UPDATE onemed_partialload_configuration SET end_timestamp = ? WHERE topic = ?", endTimestamp, topic);
        }
    }
}
