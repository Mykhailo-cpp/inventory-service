package com.example.inventory_service.kafka;

import com.example.inventory_service.avro.OrderCreated;
import com.example.inventory_service.kafka.avro.OrderEventAvroSerializer;
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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class InventoryKafkaIntegrationTest {

    private static final String ORDER_EVENTS = "order-events";
    private static final String INVENTORY_RESULT = "inventory-result";
    private static final String DLT = "order-events.DLT";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${local.server.port}")
    private int port;

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

    @Test
    void correlationId_fromOrderEventIsCarriedToInventoryResult() {
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, 2, "corr-from-order-service");

        ConsumerRecord<String, String> result =
                awaitRecord(INVENTORY_RESULT, orderId.toString(), Duration.ofSeconds(20));
        assertThat(result).isNotNull();
        var header = result.headers().lastHeader("X-Correlation-Id");
        assertThat(header).as("correlation header on inventory-result").isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("corr-from-order-service");
    }

    @Test
    void healthEndpoint_reportsUpWithKafkaAndDatabase() throws Exception {
        HttpResponse<String> response = get("/actuator/health");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .contains("\"status\":\"UP\"")
                .contains("\"kafka\"")
                .contains("\"db\"");
    }

    @Test
    void prometheusEndpoint_exposesMetricsTaggedWithApplicationName() throws Exception {
        HttpResponse<String> response = get("/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .contains("jvm_memory_used_bytes")
                .contains("application=\"inventory-service\"");
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void sendOrderCreated(UUID orderId, int quantity) {
        sendOrderCreated(orderId, quantity, null);
    }

    private void sendOrderCreated(UUID orderId, int quantity, String correlationId) {
        OrderCreated event = new OrderCreated(
                orderId.toString(), 1L, "laptop", quantity, LocalDateTime.of(2026, 1, 1, 12, 0));

        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderEventAvroSerializer.class);
        try (KafkaProducer<String, OrderCreated> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, OrderCreated> record = new ProducerRecord<>(ORDER_EVENTS, "1", event);
            record.headers().add("__TypeId__", "ORDER_CREATED".getBytes(StandardCharsets.UTF_8));
            if (correlationId != null) {
                record.headers().add("X-Correlation-Id", correlationId.getBytes(StandardCharsets.UTF_8));
            }
            producer.send(record);
            producer.flush();
        }
    }

    private String awaitResult(String topic, String orderId, Duration timeout) {
        ConsumerRecord<String, String> record = awaitRecord(topic, orderId, timeout);
        return record == null ? null : record.value();
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String orderId, Duration timeout) {
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
                        return r;
                    }
                }
            }
        }
        return null;
    }
}