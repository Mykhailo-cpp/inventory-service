package com.example.inventory_service.service;

import com.example.inventory_service.avro.OrderCreated;
import com.example.inventory_service.dto.InventoryResultEvent;

public interface InventoryService {
    InventoryResultEvent processOrder(OrderCreated event);
}
