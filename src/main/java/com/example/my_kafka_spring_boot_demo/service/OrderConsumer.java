package com.example.my_kafka_spring_boot_demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    // Consumer 1 in Group "order-processing-group"
    @KafkaListener(topics= "orders-topic", groupId = "order-processing-group", clientIdPrefix = "consumer-1")
    public void listenConsumer1(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset,
                                @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("[Consumer 1] Received key: {}, message: {} from Partition: {} at Offset: {}",
                key, message, partition, offset);
    }

    @KafkaListener(topics= "orders-topic", groupId = "order-processing-group", clientIdPrefix = "consumer-2")
    public void listenConsumer2(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset,
                                @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("[Consumer 2] Received key: {}, message: {} from Partition: {} at Offset: {}",
                key, message, partition, offset);
    }

    @KafkaListener(topics= "orders-topic", groupId = "order-processing-group", clientIdPrefix = "consumer-3")
    public void listenConsumer3(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset,
                                @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("[Consumer 3] Received key: {}, message: {} from Partition: {} at Offset: {}",
                key, message, partition, offset);
    }
}
