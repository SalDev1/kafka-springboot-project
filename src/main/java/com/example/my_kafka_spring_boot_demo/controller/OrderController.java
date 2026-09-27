package com.example.my_kafka_spring_boot_demo.controller;
import com.example.my_kafka_spring_boot_demo.service.OrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    // Inject your OrderProducer bean
    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    // Example URL: http://localhost:8080/api/v1/orders?key=USER_101&message=Order_Placed
    @GetMapping
    public ResponseEntity<String> sendOrder(@RequestParam(name = "key") String key, @RequestParam(name = "message") String message) {
        // Calling the producer method
        orderProducer.sendOrder(key, message);
        return ResponseEntity.ok("Message sent to Kafka! Key: " + key + ", Message: " + message);
    }
}
