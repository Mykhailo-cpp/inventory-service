package com.example.inventory_service.service.impl;

import com.example.inventory_service.dto.InventoryResultEvent;
import com.example.inventory_service.dto.InventoryStatus;
import com.example.inventory_service.dto.OrderCreatedEvent;
import com.example.inventory_service.kafka.InventoryResultProducer;
import com.example.inventory_service.model.ProcessedMessage;
import com.example.inventory_service.repository.ProcessedMessagesRepository;
import com.example.inventory_service.service.InventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class InventoryServiceImpl implements InventoryService {
    private static final int MAX_AVAILABLE_QUANTITY = 5;

    private final InventoryResultProducer producer;
    private final ProcessedMessagesRepository processedMessagesRepository;

    @Override
    @Transactional
    public InventoryResultEvent processOrder(OrderCreatedEvent order) {

        String messageId = String.valueOf(order.orderId());

        if(processedMessagesRepository.existsByMessageId(messageId)){
            System.out.println("Duplicate event detected. Ignored for orderId: " + messageId);

            return null;
        }

        InventoryStatus status;

        if(order.quantity() <= MAX_AVAILABLE_QUANTITY){
            status = InventoryStatus.AVAILABLE;
        }
        else{
            status = InventoryStatus.OUT_OF_STOCK;
        }

        InventoryResultEvent result = new InventoryResultEvent(order.orderId(), status);

        producer.publish(result);

        processedMessagesRepository.save(
                ProcessedMessage.builder()
                        .messageId(messageId)
                        .processedAt(LocalDateTime.now())
                        .build()
        );

        return result;
    }
}
