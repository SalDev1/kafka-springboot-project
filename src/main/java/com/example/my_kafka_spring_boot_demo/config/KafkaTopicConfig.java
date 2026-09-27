package com.example.my_kafka_spring_boot_demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    @Bean

    // This helps us understand topic, partition, replication & retention.
    public NewTopic experimentTopic() {
        // RETENTION : Keep records for 1 hour (3600000 ms).
        return TopicBuilder
                .name("orders-topic")
                .partitions(2) // Total number of partitions for assigned topic
                .replicas(1)  // Each individual partition has 2 total copies, distributed across any 2 active Kafka brokers.
                .configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, "3600000"
                 ))
                .build();
    }
}
