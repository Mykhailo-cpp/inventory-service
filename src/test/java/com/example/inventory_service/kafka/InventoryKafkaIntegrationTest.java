package com.example.inventory_service.kafka;

import com.example.inventory_service.repository.ProcessedMessagesRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class InventoryKafkaIntegrationTest {

    private static final String ORDER_EVENTS = "order-events";
    private static final String INVENTORY_RESULT = "inventory-result";
    private static final String DLT = "order-events.DLT";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private ProcessedMessagesRepository processedMessagesRepository;

    @Test
    void smallOrder_isProcessedAndReportedAvailable() {
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, 3);

        assertThat(awaitResult(INVENTORY_RESULT, orderId.toString(), Duration.ofSeconds(20)))
                .as("inventory-result for %s", orderId)
                .contains(orderId.toString())
                .contains("\"status\":\"AVAILABLE\"");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(processedMessagesRepository.existsById(orderId.toString())).isTrue());
    }

    @Test
    void largeOrder_isReportedOutOfStock() {
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, 10);

        assertThat(awaitResult(INVENTORY_RESULT, orderId.toString(), Duration.ofSeconds(20)))
                .contains(orderId.toString())
                .contains("\"status\":\"OUT_OF_STOCK\"");
    }

    @Test
    void processedOrder_doesNotEndUpInDeadLetterTopic() {
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, 2);
        assertThat(awaitResult(INVENTORY_RESULT, orderId.toString(), Duration.ofSeconds(20))).isNotNull();

        assertThat(awaitResult(DLT, orderId.toString(), Duration.ofSeconds(3)))
                .as("order %s must not be dead-lettered", orderId)
                .isNull();
    }

    private void sendOrderCreated(UUID orderId, int quantity) {
        String json = """
                {"orderId":"%s","customerId":1,"product":"laptop","quantity":%d,"createdTime":"2026-01-01T12:00:00.000"}
                """.formatted(orderId, quantity).trim();

        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(ORDER_EVENTS, "1", json);
            record.headers().add("__TypeId__", "ORDER_CREATED".getBytes(StandardCharsets.UTF_8));
            producer.send(record);
            producer.flush();
        }
    }

    private String awaitResult(String topic, String orderId, Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(300))) {
                    if (r.value() != null && r.value().contains(orderId)) {
                        return r.value();
                    }
                }
            }
        }
        return null;
    }
}