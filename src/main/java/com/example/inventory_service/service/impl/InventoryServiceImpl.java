package com.example.inventory_service.service.impl;

import com.example.inventory_service.dto.InventoryResultEvent;
import com.example.inventory_service.dto.InventoryStatus;
import com.example.inventory_service.dto.OrderCreatedEvent;
import com.example.inventory_service.kafka.InventoryResultProducer;
import com.example.inventory_service.model.ProcessedMessage;
import com.example.inventory_service.repository.ProcessedMessagesRepository;
import com.example.inventory_service.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Random;

@Service
@RequiredArgsConstructor
@Log4j2
public class InventoryServiceImpl implements InventoryService {
    private static final int MAX_AVAILABLE_QUANTITY = 5;

    private final InventoryResultProducer producer;
    private final ProcessedMessagesRepository processedMessagesRepository;
    private final Random random = new Random();

    @Override
    @Transactional
    public InventoryResultEvent processOrder(OrderCreatedEvent order) {
        /*if(random.nextBoolean()){
            throw new RuntimeException("Simulated failure during inventory processing");
        }*/


        String messageId = String.valueOf(order.orderId());

        if(processedMessagesRepository.existsById(messageId)){
            log.warn("Duplicate event detected. Skipping orderId: {}", messageId);
            return null;
        }

        processedMessagesRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));

        InventoryStatus status;

        if(order.quantity() <= MAX_AVAILABLE_QUANTITY){
            status = InventoryStatus.AVAILABLE;
        }
        else{
            status = InventoryStatus.OUT_OF_STOCK;
        }

        InventoryResultEvent result = new InventoryResultEvent(order.orderId(), status);

        producer.publish(result);

        return result;
    }
}
