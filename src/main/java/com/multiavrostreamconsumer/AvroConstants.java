package com.multiavrostreamconsumer;

// Add a stub for AvroConstants to resolve references in DatabaseService and KafkaStreamsService
public class AvroConstants {
    public static final String INSERT_INTO = "INSERT INTO ";
    public static final String INSERT_ERROR = "INSERT INTO error_records VALUES (?, ?, ?)";
    public static final String INSERT_ARCHIVE = " VALUES (?, ?)";
    public static final String INSERT_STATUS = "INSERT INTO batch_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String GET_CONFIGURATION = "SELECT * FROM onemed_partialload_configuration WHERE active = 'Y'";
    public static final String MESSAGE_DESERIALIZATION_FAILED = "Message deserialization failed";
}