package com.multiavrostreamconsumer.model;

import scala.math.BigInt;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AvroConsumerConfig {
    Properties kafkaConsumerProps;
    Integer configId;
    String topic;
    String archiveTable;
    String targetTable;
    String schemaRegistry;
    BigDecimal maxRecords;
    List<String> columnFilters;
    Map<String, List<String>> rowFilters;

    public Properties getKafkaConsumerProps() {
        return kafkaConsumerProps;
    }

    public void setKafkaConsumerProps(Properties kafkaConsumerProps) {
        this.kafkaConsumerProps = kafkaConsumerProps;
    }

    public Integer getConfigId() {
        return configId;
    }

    public void setConfigId(Integer configId) {
        this.configId = configId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getArchiveTable() {
        return archiveTable;
    }

    public void setArchiveTable(String archiveTable) {
        this.archiveTable = archiveTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getSchemaRegistry() {
        return schemaRegistry;
    }

    public void setSchemaRegistry(String schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public BigDecimal getMaxRecords() {
        return maxRecords;
    }

    public void setMaxRecords(BigDecimal maxRecords) {
        this.maxRecords = maxRecords;
    }

    public List<String> getColumnFilters() {
        return columnFilters;
    }

    public void setColumnFilters(List<String> columnFilters) {
        this.columnFilters = columnFilters;
    }

    public Map<String, List<String>> getRowFilters() {
        return rowFilters;
    }

    public void setRowFilters(Map<String, List<String>> rowFilters) {
        this.rowFilters = rowFilters;
    }

    @Override
    public String toString() {
        return "ConsumerConfig{" +
                "kafkaConsumerProps=" + kafkaConsumerProps +
                ", configId=" + configId +
                ", topic='" + topic + '\'' +
                ", archiveTable='" + archiveTable + '\'' +
                ", targetTable='" + targetTable + '\'' +
                ", schemaRegistry='" + schemaRegistry + '\'' +
                ", columnFilters=" + columnFilters +
                ", rowFilters=" + rowFilters +
                ", maxRecords=" + maxRecords +
                '}';
    }
}
