package com.multiavrostreamconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.multiavrostreamconsumer.AvroConsumerConfig;
import com.multiavrostreamconsumer.ErrorRecord;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

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

import static org.apache.kafka.common.serialization.Serdes.Bytes;

@Service
public class KafkaStreamsService {
    private final ThreadPoolTaskExecutor taskExecutor;
    private final DatabaseService databaseService;
    private final KeyVaultService keyVaultService;
    private final ConfigService configService;
    private static final Logger logger = LogManager.getLogger(KafkaStreamsService.class);
    private final AtomicInteger counter = new AtomicInteger(0);

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
        CountDownLatch latch = new CountDownLatch(configurations.size());

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
        // Fetch Avro schema from schema registry
        Schema avroSchema = fetchAvroSchema(config.getSchemaRegistry(), config.getTopic());
        // Ensure target table exists and is up to date
        databaseService.ensureTargetTable(avroSchema, config.getTargetTable());

        // Example: Add time-based filtering using KafkaConsumer
        Properties props = config.getKafkaConsumerProps();
        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> consumer =
                new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));

        Long startTimestamp = config.getStartTimestamp();
        Long endTimestamp = config.getEndTimestamp();

        // Seek to offsets for startTimestamp if provided
        if (startTimestamp != null) {
            consumer.poll(java.time.Duration.ofMillis(100)); // Ensure assignment
            Set<org.apache.kafka.common.TopicPartition> partitions = consumer.assignment();
            Map<org.apache.kafka.common.TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (org.apache.kafka.common.TopicPartition partition : partitions) {
                timestampsToSearch.put(partition, startTimestamp);
            }
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsets =
                    consumer.offsetsForTimes(timestampsToSearch);
            for (org.apache.kafka.common.TopicPartition partition : partitions) {
                org.apache.kafka.clients.consumer.OffsetAndTimestamp offsetAndTimestamp = offsets.get(partition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(partition, offsetAndTimestamp.offset());
                }
            }
        }

        boolean running = true;
        while (running) {
            org.apache.kafka.clients.consumer.ConsumerRecords<String, byte[]> records =
                    consumer.poll(java.time.Duration.ofMillis(1000));
            for (org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> record : records) {
                // If endTimestamp is set, skip records after it
                if (endTimestamp != null && record.timestamp() > endTimestamp) {
                    running = false;
                    break;
                }
                // ...existing message processing logic...
            }
            // Optionally break if no more records or other stopping condition
        }
        consumer.close();
    }

    // Fetch Avro schema from schema registry (basic implementation for Confluent-compatible registry)
    private Schema fetchAvroSchema(String schemaRegistryUrl, String topic) {
        try {
            String url = schemaRegistryUrl + "/subjects/" + topic + "-value/versions/latest";
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed to fetch schema: HTTP error code : " + conn.getResponseCode());
            }
            java.io.InputStream is = conn.getInputStream();
            java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
            String result = s.hasNext() ? s.next() : "";
            com.fasterxml.jackson.databind.JsonNode node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(result);
            String schemaStr = node.get("schema").asText();
            return new org.apache.avro.Schema.Parser().parse(schemaStr);
        } catch (Exception e) {
            throw new RuntimeException("Error fetching Avro schema: " + e.getMessage(), e);
        }
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
            throw new RuntimeException("Message deserialization failed");
        }
    }

    private static GenericRecord deserializeAvro(byte[] messageBytes, Schema writerSchema) throws RuntimeException {
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(messageBytes, 5, messageBytes.length - 5, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException("Message deserialization failed");
        }
    }

    private String getBatchId() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    // Additional methods for row filtering, schema handling, etc.
}

