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
        // Implementation code for processing Kafka messages
        // (See the original file for the detailed implementation)
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

    private String getBatchId() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    // Additional methods for row filtering, schema handling, etc.
}
