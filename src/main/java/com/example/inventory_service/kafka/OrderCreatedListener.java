package com.example.inventory_service.kafka;

import com.example.inventory_service.dto.OrderCreatedEvent;
import com.example.inventory_service.service.InventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
@RequiredArgsConstructor
public class OrderCreatedListener {

    private final InventoryService inventoryService;
    private final Random random = new Random();

    @KafkaListener(topics = "order-created", groupId = "inventory-service")
    public void handleOrderCreated(OrderCreatedEvent event){
        System.out.println("Processing order:");
        System.out.println("OrderId: " + event.orderId());
        System.out.println("Product: " + event.product());
        System.out.println("Quantity: " + event.quantity());

        if(random.nextBoolean()){
            throw new RuntimeException("Simulated failure during inventory processing");
        }

        inventoryService.processOrder(event);
    }
}
