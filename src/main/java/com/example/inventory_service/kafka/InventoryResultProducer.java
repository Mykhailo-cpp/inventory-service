package com.example.inventory_service.kafka;

import com.example.inventory_service.config.KafkaTopicConfig;
import com.example.inventory_service.dto.InventoryResultEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class InventoryResultProducer {

    private final KafkaTemplate<String, InventoryResultEvent> kafkaTemplate;

    public void publish(InventoryResultEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.INVENTORY_RESULT_TOPIC, event.orderId().toString(), event)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish inventory result for order: {}", event.orderId(), ex);
                    } else {
                        log.info("Published inventory result {} for order {}", event.status(), event.orderId());
                    }
                });
    }
}