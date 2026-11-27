package com.example.inventory_service.kafka;


import com.example.inventory_service.avro.OrderCreated;
import com.example.inventory_service.service.InventoryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.nio.charset.StandardCharsets;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"order-events"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderCreatedListenerRetryIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoBean
    private InventoryService inventoryService;

    @Test
    @DisplayName("Verify Spring Kafka retries when listener processing fails")
    void testRetryBehaviorOnException() {

        doThrow(new RuntimeException("Simulated failure"))
                .when(inventoryService).processOrder(any());

        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                1001L,
                "Test Product",
                1,
                LocalDateTime.now()
        );

        ProducerRecord<String, Object> record = new ProducerRecord<>("order-events", "1001", event);
        record.headers().add("__TypeId__", "ORDER_CREATED".getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(inventoryService, atLeast(2)).processOrder(any()));
    }
}