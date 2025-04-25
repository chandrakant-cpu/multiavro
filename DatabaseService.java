package com.multiavrostreamconsumer.service;

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

import com.multiavrostreamconsumer.model.ErrorRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.multiavrostreamconsumer.utility.AvroConstants;

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

//    public void insertToTarget(List<GenericRecord> records, String tableName, String batchId) {
//        if (records.isEmpty()) {
//            return;
//        }
//
//        // Executor service for parallel processing
//        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
//
//        int totalRecords = records.size();
//        for (int i = 0; i < totalRecords; i += BATCH_SIZE) {
//            int end = Math.min(i + BATCH_SIZE, totalRecords);
//            List<GenericRecord> batch = records.subList(i, end);
//
//            // Asynchronously process each batch
//            executorService.submit(() -> {
//                insertRecords(batch, tableName);
//            });
//        }
//
//        // Shutdown the executor once all tasks are completed
//        executorService.shutdown();
//        try {
//            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
//                executorService.shutdownNow();
//            }
//        } catch (InterruptedException e) {
//            executorService.shutdownNow();
//        }
//    }
//
//
//    public void insertRecords(List<GenericRecord> avroRecords, String tableName) {
//        if (avroRecords.isEmpty()) {
//            return;
//        }
//
//        // Fetch column names from the first record
//        GenericRecord firstRecord = avroRecords.get(0);
//        List<String> columnNames = firstRecord.getSchema().getFields().stream()
//                .map(field -> {
//                    // Check if the schema has ambiguous fields and prefix field names if necessary
//                    Schema schema = firstRecord.getSchema();
//                    boolean hasAmbiguousFields = hasAmbiguousFields(schema);
//                    return hasAmbiguousFields ? schema.getName() + "_" + field.name() : field.name();
//                })
//                .collect(Collectors.toList());
//
//        // Add pg_load_dt and pg_modify_dt to the column list
//        columnNames.add("pg_load_dt");
//        columnNames.add("pg_modify_dt");
//
//        // Build the insert query
//        String insertQuery = buildInsertQuery(tableName, columnNames);
//
//        // Use batchUpdate to process records in bulk
//        logger.info("Number of records to be inserted: {}", avroRecords.size());
//        try {
//            jdbcTemplate.batchUpdate(insertQuery, new BatchPreparedStatementSetter() {
//                @Override
//                public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {
//                    GenericRecord record = avroRecords.get(i);
//                    int index = 1;
//
//                    // Set values for all columns in the record
//                    for (String column : columnNames) {
//                        if (column.equals("pg_load_dt") || column.equals("pg_modify_dt")) {
//                            // Set current timestamp for pg_load_dt and pg_modify_dt
//                            ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
//                        } else {
//                            Object value = record.get(column.replaceFirst(".*_", "")); // Remove prefix for actual field lookup
//                            if (value instanceof Utf8) {
//                                // Convert Utf8 to String explicitly
//                                ps.setString(index++, value.toString());
//                            } else {
//                                // Use setObject for other types
//                                ps.setObject(index++, value);
//                            }
//                        }
//                    }
//                }
//
//                @Override
//                public int getBatchSize() {
//                    return avroRecords.size();
//                }
//            });
//        } catch (Exception e) {
//            logger.error("Error inserting records into table {}: {}", tableName, e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//
//    private boolean hasAmbiguousFields(Schema schema) {
//        Map<String, Integer> fieldNameCounts = new HashMap<>();
//
//        // Count occurrences of each field name across all parent schemas
//        schema.getFields().forEach(field -> {
//            fieldNameCounts.put(field.name(), fieldNameCounts.getOrDefault(field.name(), 0) + 1);
//        });
//
//        // Check if any field name appears more than once
//        return fieldNameCounts.values().stream().anyMatch(count -> count > 1);
//    }

//    public void insertRecords(List<GenericRecord> avroRecords, String tableName) {
//        if (avroRecords.isEmpty()) {
//            return;
//        }
//
//        // Fetch column names from the first record
//        GenericRecord firstRecord = avroRecords.get(0);
//        List<String> columnNames = firstRecord.getSchema().getFields().stream()
//                .map(Schema.Field::name).collect(Collectors.toList());
//
//        // Add pg_load_dt and pg_modify_dt to the column list
//        columnNames.add("pg_load_dt");
//        columnNames.add("pg_modify_dt");
//
//        // Build the insert query
//        String insertQuery = buildInsertQuery(tableName, columnNames);
//
//        // Use batchUpdate to process records in bulk
//        logger.info("Number of records to be inserted: {}", avroRecords.size());
//        try {
//            jdbcTemplate.batchUpdate(insertQuery, new BatchPreparedStatementSetter() {
//                @Override
//                public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {
//                    GenericRecord record = avroRecords.get(i);
//                    int index = 1;
//
//                    // Set values for all columns in the record
//                    for (String column : columnNames) {
//                        if (column.equals("pg_load_dt") || column.equals("pg_modify_dt")) {
//                            // Set current timestamp for pg_load_dt and pg_modify_dt
//                            ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
//                        } else {
//                            Object value = record.get(column);
//                            if (value instanceof Utf8) {
//                                // Convert Utf8 to String explicitly
//                                ps.setString(index++, value.toString());
//                            } else {
//                                // Use setObject for other types
//                                ps.setObject(index++, value);
//                            }
//                        }
//                    }
//                }
//
//                @Override
//                public int getBatchSize() {
//                    return avroRecords.size();
//                }
//            });
//        } catch (Exception e) {
//            logger.error("Error inserting records into table {}: {}", tableName, e.getMessage());
//            e.printStackTrace();
//        }
//    }

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
        return AvroConstants.INSERT_INTO + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
    }

    public void insertToErrorRecords(String batchId, int configId, List<ErrorRecord> errorRecords) {
        try {
            jdbcTemplate.batchUpdate(AvroConstants.INSERT_ERROR, new BatchPreparedStatementSetter() {
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
            jdbcTemplate.batchUpdate(AvroConstants.INSERT_INTO + archiveTable + AvroConstants.INSERT_ARCHIVE, new BatchPreparedStatementSetter() {
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
            jdbcTemplate.update(AvroConstants.INSERT_STATUS, batchId, date, configId, executionTime, fetchedRecords, targetRecords,
                    deleteRecords, errorRecords, archiveRecords);
            System.out.println("Records inserted to Target Table" + fetchedRecords);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Map<String, Object>> getConfigurations() {
        try {
            String sql = AvroConstants.GET_CONFIGURATION;
            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }


}
