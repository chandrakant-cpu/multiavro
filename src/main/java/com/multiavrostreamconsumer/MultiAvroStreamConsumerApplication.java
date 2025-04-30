package com.multiavrostreamconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MultiAvroStreamConsumerApplication {
    public static void main(String[] args) {
        // Parse command-line arguments for startTimestamp and endTimestamp
        Long startTimestamp = null;
        Long endTimestamp = null;
        for (String arg : args) {
            if (arg.startsWith("--startTimestamp=")) {
                startTimestamp = Long.parseLong(arg.replace("--startTimestamp=", ""));
            } else if (arg.startsWith("--endTimestamp=")) {
                endTimestamp = Long.parseLong(arg.replace("--endTimestamp=", ""));
            }
        }
        if (startTimestamp != null) {
            System.setProperty("multiavro.startTimestamp", startTimestamp.toString());
        }
        if (endTimestamp != null) {
            System.setProperty("multiavro.endTimestamp", endTimestamp.toString());
        }

        // Update DB with user-provided time window for all active topics
        org.springframework.context.ConfigurableApplicationContext ctx = SpringApplication.run(MultiAvroStreamConsumerApplication.class, args);
        if (startTimestamp != null || endTimestamp != null) {
            DatabaseService dbService = ctx.getBean(DatabaseService.class);
            ConfigService configService = ctx.getBean(ConfigService.class);
            // Get all active topics from config table
            java.util.List<AvroConsumerConfig> configs = configService.getAllConfigurations();
            for (AvroConsumerConfig cfg : configs) {
                dbService.updateTimeWindowForTopic(cfg.getTopic(), startTimestamp, endTimestamp);
            }
        }
    }
}
