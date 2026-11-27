package com.example.inventory_service.kafka;


import com.example.inventory_service.avro.OrderCreated;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Log4j2
@KafkaListener(topics = "order-events", groupId = "audit-group")
public class OrderAuditListener {

    @KafkaHandler
    public void onCreated(OrderCreated e,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int p,
                          @Header(KafkaHeaders.OFFSET) long o) {
        log.info("AUDIT >>> order {} partition {} offset {}", e.getOrderId(), p, o);
    }

    @KafkaHandler(isDefault = true)
    public void other(Object o) { }
}