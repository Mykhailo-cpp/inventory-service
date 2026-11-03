package com.example.inventory_service.kafka;

import com.example.inventory_service.dto.OrderCancelledEvent;
import com.example.inventory_service.dto.OrderCreatedEvent;
import com.example.inventory_service.dto.OrderUpdatedEvent;
import com.example.inventory_service.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Log4j2
public class OrderEventListener {

    private final InventoryService inventoryService;

    @KafkaListener(topics = "order-events", groupId = "inventory-service-v3")
    public void handleOrderCreated(
            OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset){
        log.info("ORDER CREATED >>> Key(customerId): {} | Partition: {} | Offset: {}", key, partition, offset);


        log.info("ORDER CREATED >>> OrderId: {}, Product: {}, x{}", event.orderId(), event.product(), event.quantity());
        try{
            inventoryService.processOrder(event);
        }catch(Exception e){
            log.error("Error processing ORDER_CREATED for orderId: {}", event.orderId(), e);
            //throw e;
        }
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-service")
    public void handleOrderUpdated(
            OrderUpdatedEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset){
        log.info("ORDER UPDATED >>> Key(customerId): {} | Partition: {} | Offset: {}", key, partition, offset);


        System.out.println("ORDER UPDATED >>> OrderId: " + event.orderId() + " Product: " + event.product() + " New Quantity" + event.newQuantity());
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-service")
    public void handleOrderCancelled(
            OrderCancelledEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset){
        log.info("ORDER CANCELLED >>> Key(customerId): {} | Partition: {} | Offset: {}", key, partition, offset);

        System.out.println("ORDER CANCELLED >>> OrderId: " + event.orderId() + " Reason: " + event.reason());
    }
}
