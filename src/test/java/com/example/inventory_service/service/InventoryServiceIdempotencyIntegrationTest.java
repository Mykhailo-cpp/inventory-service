package com.example.inventory_service.service;

import com.example.inventory_service.dto.InventoryResultEvent;
import com.example.inventory_service.dto.OrderCreatedEvent;
import com.example.inventory_service.repository.ProcessedMessagesRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import com.example.inventory_service.kafka.InventoryResultProducer;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class InventoryServiceIdempotencyIntegrationTest {

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ProcessedMessagesRepository processedMessageRepository;

    @MockitoBean
    private InventoryResultProducer inventoryResultProducer;

    @BeforeEach
    void setUp() {
        processedMessageRepository.deleteAll();
    }

    @Test
    @DisplayName("Should process order on first attempt and ignore duplicate event on second attempt")
    void shouldPreventDuplicateProcessingOfSameOrder() {
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                1001L,
                "Laptop",
                2,
                LocalDateTime.now()
        );

        InventoryResultEvent firstResult = inventoryService.processOrder(event);

        assertNotNull(firstResult, "First call should process successfully");
        assertEquals(orderId, firstResult.orderId());
        assertTrue(processedMessageRepository.existsByMessageId(orderId.toString()),
                "Message ID should be recorded in processed_messages table");

        InventoryResultEvent secondResult = inventoryService.processOrder(event);

        assertNull(secondResult, "Second call should return null due to duplicate detection");
        assertEquals(1, processedMessageRepository.count(),
                "Database should still contain only 1 processed record");
    }
}