package com.multiavrostreamconsumer.service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.multiavrostreamconsumer.model.ErrorRecord;
import com.multiavrostreamconsumer.utility.AvroConstants;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.multiavrostreamconsumer.model.AvroConsumerConfig;
import java.io.File;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

//iceberg packages

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.kafka.common.serialization.Serdes.Bytes;

@Service
public class KafkaStreamsService {

    private final ThreadPoolTaskExecutor taskExecutor;
    private final DatabaseService databaseService;
    private final KeyVaultService keyVaultService;
 //   private final DatabricksService databricksService;
    private final ConfigService configService;
    private static final Logger logger = LogManager.getLogger(KafkaStreamsService.class);
    private final AtomicInteger counter = new AtomicInteger(0);
   // private final AtomicLong startTime = new AtomicLong(0);

    private final CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger totalRecordsProcessed = new AtomicInteger(0);

    private static final int BATCH_SIZE = 100;
    private static final int THREAD_POOL_SIZE = 10;
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private final List<GenericRecord> recordBuffer = Collections.synchronizedList(new ArrayList<>());

    public KafkaStreamsService(ThreadPoolTaskExecutor taskExecutor, DatabaseService databaseService, KeyVaultService keyVaultService, ConfigService configService) throws InterruptedException {
        this.taskExecutor = taskExecutor;
        this.databaseService = databaseService;
        this.keyVaultService = keyVaultService;
        this.configService = configService;
        initializeStreams();
    }

    private void initializeStreams() throws InterruptedException {
        List<AvroConsumerConfig> configurations = this.configService.getAllConfigurations();
        logger.info("Total active avro configurations: {}", configurations.size());
        List<GenericRecord> avroGenericRecords;
        CountDownLatch latch = new CountDownLatch(configurations.size());

        // Execute each configuration in its own thread.
        for (AvroConsumerConfig config : configurations) {
            taskExecutor.execute(() -> {
                try {
                    logger.info("Starting consumer for config: {}", config);


                    processKafkaStreamsColumnFilter(config);
                

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        logger.info("All Kafka Streams processed successfully.");
    }

    public void processKafkaStreamsColumnFilter(AvroConsumerConfig config) {
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        StreamsBuilder builder = new StreamsBuilder();

        Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<ErrorRecord> errorRecordsQueue = new ConcurrentLinkedQueue<>();

        String sourceTopic = config.getTopic();

        KStream<String, byte[]> stream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.ByteArray()));

        List<GenericRecord> recordBufferOther = new ArrayList<>();

        stream.foreach((key, value) -> {
            try {
                if (value != null) {
                    // Fetch or compute the writer schema
                    Schema writerSchema = schemaCache.computeIfAbsent(config.getTopic(), (key1) -> {
                        try {
                            SchemaFetcher schemaFetcher = new SchemaFetcher(config.getSchemaRegistry());
                            int schemaId = extractSchemaId(value);
                            return schemaFetcher.fetchSchema(schemaId);
                        } catch (IllegalArgumentException e) {
                            throw new RuntimeException(AvroConstants.UNKNOWN_SERIALIZATION_FORMAT);
                        } catch (Exception e) {
                            throw new RuntimeException(e.getMessage());
                        }
                    });

                    GenericRecord record;

                    // Check if column filters are present
                    if (config.getColumnFilters() != null && !config.getColumnFilters().isEmpty() && config.getColumnFilters().stream().anyMatch(field -> writerSchema.getField(field) != null)) {
                        Schema readerSchema = schemaCache.computeIfAbsent(config.getTopic() + ".readerSchema", (key2) -> {
                            try {
                                List<Schema.Field> filteredFields = new ArrayList<>();
                                writerSchema.getFields().forEach(f -> {
                                    if (config.getColumnFilters().contains(f.name())) {
                                        filteredFields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()));
                                    }
                                });

                                Schema modifiedSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(), writerSchema.isError());
                                modifiedSchema.setFields(filteredFields);

                                return modifiedSchema;

                            } catch (IllegalArgumentException e) {
                                throw new RuntimeException(AvroConstants.UNKNOWN_SERIALIZATION_FORMAT);
                            } catch (Exception e) {
                                throw new RuntimeException(e.getMessage());
                            }
                        });

                        // Deserialize using writerSchema and readerSchema
                        record = deserializeAvro(value, writerSchema, readerSchema);
                    } else {
                        // Deserialize using only writerSchema
                        record = deserializeAvro(value, writerSchema);
                    }

                    // Check if row filters are present
                    boolean shouldAddRecord = true;
                    if (config.getRowFilters() != null && !config.getRowFilters().isEmpty()) {
                        shouldAddRecord = applyRowFilters(record, config.getRowFilters());
                    }

                    if (shouldAddRecord) {
                        recordBufferOther.add(record);

                        // Process the batch if buffer size reaches the threshold
                        if (recordBufferOther.size() >= config.getMaxRecords().intValue()) {
                            String batchId = getBatchId(); // Create batchId for the batch
                            logger.info("Kafka Streams Avro OMD:: The total record size in buffer " + recordBufferOther.size());
                            long endTime = System.currentTimeMillis();
                            long executionTime = (endTime - startTime.get()) / 1000;

                            try {
                                databaseService.insertToTarget(recordBufferOther, config.getTargetTable(), batchId);
                                databaseService.insertToBatchStatus(batchId, new Date(), config.getConfigId(), executionTime, recordBufferOther.size(), 0, 0, errorRecordsQueue.size(), 0);
                            } catch (Exception e) {
                                logger.error("Error inserting batch to target table. Adding failed records to error table.", e);
                                for (GenericRecord failedRecord : recordBufferOther) {
                                    errorRecordsQueue.add(new ErrorRecord(failedRecord.toString(), e.getMessage()));
                                    databaseService.insertToErrorRecords(batchId, config.getConfigId(), new ArrayList<>(errorRecordsQueue));
                                }
                            }

                            recordBufferOther.clear();
                            startTime.set(System.currentTimeMillis());
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing record. Adding to error queue.", e);

                // Create an error record
                ErrorRecord errorRecord = new ErrorRecord(value != null ? new String(value) : null, e.getMessage());

                // Add the error record to the queue
                errorRecordsQueue.add(errorRecord);
            }
        });

        // Periodic processing of remaining records and error records
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            synchronized (recordBufferOther) {
                if (!recordBufferOther.isEmpty()) {
                    String batchId = getBatchId(); // Create batchId for the remaining batch
                    List<GenericRecord> batch = new ArrayList<>(recordBufferOther);
                    recordBufferOther.clear();
                    logger.info("Kafka Streams :: Processing remaining records periodically. Total record size: " + batch.size());
                    try {
                        databaseService.insertToTarget(batch, config.getTargetTable(), batchId);
                    } catch (Exception e) {
                        logger.error("Error inserting remaining batch to target table. Adding failed records to error table.", e);
                        for (GenericRecord failedRecord : batch) {
                            errorRecordsQueue.add(new ErrorRecord(failedRecord.toString(), e.getMessage()));

                        }
                    }
                }
            }

            synchronized (errorRecordsQueue) {
                if (!errorRecordsQueue.isEmpty()) {
                    String batchId = getBatchId(); // Create batchId for error records
                    List<ErrorRecord> errorBatch = new ArrayList<>(errorRecordsQueue);
                    errorRecordsQueue.clear();
                    logger.info("Kafka Streams :: Processing error records periodically. Total error record size: " + errorBatch.size());
                    databaseService.insertToErrorRecords(batchId, config.getConfigId(), errorBatch);
                }
            }
        }, 1, 5, TimeUnit.MINUTES);

        // Start Kafka Streams
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config.getKafkaConsumerProps());
        streams.start();

        // Shutdown hook for fail-safe processing
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown initiated. Processing remaining records...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }

            synchronized (recordBufferOther) {
                if (!recordBufferOther.isEmpty()) {
                    String batchId = getBatchId(); // Create batchId for the remaining batch
                    List<GenericRecord> batch = new ArrayList<>(recordBufferOther);
                    recordBufferOther.clear();
                    logger.info("Processing remaining records before shutdown. Total record size: " + batch.size());
                    try {
                        databaseService.insertToTarget(batch, config.getTargetTable(), batchId);
                    } catch (Exception e) {
                        logger.error("Error inserting remaining batch to target table during shutdown. Adding failed records to error table.", e);
                        for (GenericRecord failedRecord : batch) {
                            errorRecordsQueue.add(new ErrorRecord(failedRecord.toString(), e.getMessage()));
                        }
                    }
                }
            }

            synchronized (errorRecordsQueue) {
                if (!errorRecordsQueue.isEmpty()) {
                    String batchId = getBatchId(); // Create batchId for error records
                    List<ErrorRecord> errorBatch = new ArrayList<>(errorRecordsQueue);
                    errorRecordsQueue.clear();
                    logger.info("Processing remaining error records before shutdown. Total error record size: " + errorBatch.size());
                    databaseService.insertToErrorRecords(batchId, config.getConfigId(), errorBatch);
                }
            }

            logger.info("Closing Kafka Streams...");
            streams.close();
            executorService.shutdown();
        }));
    }



    public List<Map<String, Object>> getConfigurations() {
        try {
            String sql = AvroConstants.GET_CONFIGURATION;
            //   return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }
        return null;
    }

    private int extractSchemaId(byte[] messageBytes) throws IllegalArgumentException {
        ByteBuffer buffer = ByteBuffer.wrap(messageBytes);
        if (buffer.get() != 0x0) {
            throw new IllegalArgumentException();
        }
        return buffer.getInt();
    }

    private GenericRecord deserializeAvro(byte[] messageBytes, Schema writerSchema, Schema readerSchema) throws RuntimeException {
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(messageBytes, 5, messageBytes.length - 5, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(AvroConstants.MESSAGE_DESERIALIZATION_FAILED);
        }
    }

    private static GenericRecord deserializeAvro(byte[] messageBytes, Schema writerSchema) throws RuntimeException {
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(messageBytes, 5, messageBytes.length - 5, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(AvroConstants.MESSAGE_DESERIALIZATION_FAILED);
        }
    }

    private boolean applyRowFilters(GenericRecord genericRecord, Map<String, List<String>> rowFilters) {
        if (genericRecord != null) {
            if (rowFilters.isEmpty()) {
                return true;
            }
            final boolean[] filterResult = {true};
            rowFilters.forEach((filterKey, filterValues) -> {
                if (genericRecord.hasField(filterKey) && !filterValues.contains(genericRecord.get(filterKey).toString())) {
                    filterResult[0] = false;
                }
            });
            return filterResult[0];
        }
        return false;
    }

    private String getBatchId() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }


    private Schema processSchemaWithAmbiguity(Schema writerSchema) {
        try {
            // Check for ambiguous fields in the schema
            boolean hasAmbiguousFields = hasAmbiguousFields(writerSchema);

            // Process fields with or without prefixing based on ambiguity
            List<Schema.Field> processedFields = new ArrayList<>();
            writerSchema.getFields().forEach(f -> {
                String fieldName = hasAmbiguousFields
                        ? writerSchema.getName() + "_" + f.name() // Prefix field name if ambiguous
                        : f.name(); // Use field name as is if no ambiguity
                processedFields.add(new Schema.Field(fieldName, f.schema(), f.doc(), f.defaultVal(), f.order()));
            });

            Schema modifiedSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(), writerSchema.isError());
            modifiedSchema.setFields(processedFields);

            return modifiedSchema;

        } catch (Exception e) {
            throw new RuntimeException("Error processing schema with ambiguity: " + e.getMessage());
        }
    }

    private boolean hasAmbiguousFields(Schema schema) {
        Map<String, Integer> fieldNameCounts = new HashMap<>();

        // Count occurrences of each field name across all parent schemas
        schema.getFields().forEach(field -> {
            fieldNameCounts.put(field.name(), fieldNameCounts.getOrDefault(field.name(), 0) + 1);
        });

        // Check if any field name appears more than once
        return fieldNameCounts.values().stream().anyMatch(count -> count > 1);
    }

    private void processRemainingRecords(AvroConsumerConfig config) {
        synchronized (recordBuffer) {
            if (!recordBuffer.isEmpty()) {
                List<GenericRecord> batch = new ArrayList<>(recordBuffer);
            //    System.out.println("Processing remaining records, count: " + batch.size());
                recordBuffer.clear();
                executorService.submit(() -> databaseService.insertToTarget(batch, config.getTargetTable(), "batch_id"));
            }
        }
    }

    private void processBuffer(List<GenericRecord> buffer, String targetTable, String batchId) {
        synchronized (buffer) {
            if (!buffer.isEmpty()) {
                List<GenericRecord> batch = new ArrayList<>(buffer);
                buffer.clear();
                executorService.submit(() -> {
               //     databaseService.insertToTarget(batch, targetTable, batchId);
            //        System.out.println("Processed batch of size: " + batch.size() + " to table: " + targetTable);
                });
            }
        }
    }


}