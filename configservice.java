package com.multiavrostreamconsumer.service;
import com.att.eda.edac.event.payload.Avro;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.multiavrostreamconsumer.model.AvroConsumerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ConfigService {
    @Value("${sasl-login-callback-handler-class}")
    String saslLoginCallbackHandlerClass;
    @Value("${sasl-mechanism}")
    String saslMechanism;
    @Value("${security-protocol}")
    String securityProtocol;
    @Value("${key-deserializer-class}")
    String keyDeserializerClass;
    @Value("${value-deserializer-class}")
    String valueDeserializerClass;
    @Value("${allow-auto-create-topics}")
    String allowAutoCreateTopics;
    @Value("${enable-auto-commit}")
    String enableAutoCommit;
    @Value("${fetch-max-bytes}")
    String fetchMaxBytes;
    @Value("${max-partition-fetch-bytes}")
    String maxPartitionFetchBytes;
    @Value("${heartbeat-interval-ms}")
    String heartbeatIntervalMs;
    @Value("${max-poll-interval-ms}")
    String maxPollIntervalMs;

    private final DatabaseService databaseService;
    private final KeyVaultService keyVaultService;

    public ConfigService(DatabaseService databaseService, KeyVaultService keyVaultService) {
        this.databaseService = databaseService;
        this.keyVaultService = keyVaultService;
    }

    public List<AvroConsumerConfig> getAllConfigurations() {
        return this.databaseService.getConfigurations().stream()
                .map(config -> {
                    AvroConsumerConfig consumerConfig = new AvroConsumerConfig();

                    consumerConfig.setKafkaConsumerProps(buildConsumerProperties(config));

                    consumerConfig.setConfigId((Integer) config.get("id"));
                    consumerConfig.setArchiveTable(config.get("row_table").toString());
                    consumerConfig.setTargetTable(config.get("target_table").toString());
                    consumerConfig.setTopic(config.get("topic").toString());
                    consumerConfig.setSchemaRegistry(config.get("schema_registry").toString());
                    consumerConfig.setMaxRecords((BigDecimal) config.get("max_records"));

                    // Column filters
                    consumerConfig.setColumnFilters(Arrays.asList(config.get("column_filters").toString().split(",")));

                    // Row filters
                    Map<String, List<String>> rowFilters = new HashMap<>();
                    Arrays.stream(config.get("row_filters").toString().split(",")).forEach(filter -> {
                        String[] filters = filter.split(":");
                        List<String> filterValues = new ArrayList<>(Arrays.asList(filters).subList(1, filters.length));
                        rowFilters.put(filters[0], filterValues);
                    });
                    consumerConfig.setRowFilters(rowFilters);

                    return consumerConfig;
                }).collect(Collectors.toList());
    }

    private BlobContainerClient getBlobContainerClient(AvroConsumerConfig consumerConfig) {
        String CLIENT_ID = keyVaultService.getSecret((String) consumerConfig.getKafkaConsumerProps().get("client_id"));
        String CLIENT_SECRET = "<your-client-secret>";
        String TENANT_ID = "<your-tenant-id>";
        String STORAGE_ACCOUNT_URL = "url";
        String CONTAINER_NAME = "container";

        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(CLIENT_SECRET)
                .tenantId(TENANT_ID)
                .build();

        return new BlobContainerClientBuilder()
                .endpoint(STORAGE_ACCOUNT_URL)
                .credential(clientSecretCredential)
                .containerName(CONTAINER_NAME)
                .buildClient();
    }

    public void uploadData(String blobName, String data, AvroConsumerConfig config) {
        BlobContainerClient containerClient = getBlobContainerClient(config);
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        try (InputStream dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
            blobClient.upload(dataStream, data.length(), true);
            System.out.println("Data uploaded to ADLS: " + blobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Properties buildConsumerProperties(Map<String, Object> config) {
        Properties consumerProps = new Properties();
        String clientId = keyVaultService.getSecret((String) config.get("client_id"));
        String clientSecrets = keyVaultService.getSecret((String) config.get("client_secrets"));

        consumerProps.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, config.get("oauthbearer_url"));
        consumerProps.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, saslLoginCallbackHandlerClass);
        consumerProps.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                + "clientId='" + clientId + "' "
                + "clientSecret='" + clientSecrets + "' "
                + "scope='" + clientId + "/.default' "
                + "extension_logicalCluster='" + config.get("extensionlogical_cluster") + "' "
                + "extension_identityPoolId='" + config.get("extension_identity_pool_id") + "';");

        consumerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap_servers"));
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        consumerProps.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2");
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, config.get("group_name"));

        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ((BigDecimal) config.get("max_records")).intValueExact());
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ((BigDecimal) config.get("session_timeout_ms")).intValueExact());
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        consumerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return consumerProps;
    }
}
