package com.example.my_kafka_spring_boot_demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private final KafkaTemplate<String,String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String,String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(String key, String value) {
        // PRODUCER sending message with KEY to TOPIC
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send("orders-topic",key,value);

        future.whenComplete((result,ex) -> {
            if(ex == null) {
                // ACKNOWLEDGMENT response contains metadata : Partition and Offset
                log.info("Sent key = [{}] value=[{}] to Partition=[{}] with Offset=[{}]",
                          key,
                          value,
                          result.getRecordMetadata().partition(),
                          result.getRecordMetadata().offset()
                        );
            } else {
                log.error("Unable to send message due to: {}", ex.getMessage());
            }
        });
    }

}
