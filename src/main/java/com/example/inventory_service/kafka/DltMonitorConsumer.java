package com.example.inventory_service.kafka;

import com.example.inventory_service.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DltMonitorConsumer {

    @KafkaListener(topics = "order-creadet.DLT", groupId = "dlt-monitor-group")
    public void listenDlt(OrderCreatedEvent event){
        System.out.println("FAILED MESSAGE RECEIVED IN DLT: ");
        System.out.println("OrderId: " + event.orderId());
        System.out.println("Product: " + event.product());
        System.out.println("Quantity: " + event.quantity());
    }
}
