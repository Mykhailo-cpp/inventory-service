package com.example.inventory_service.kafka.avro;

import com.example.inventory_service.avro.OrderCancelled;
import com.example.inventory_service.avro.OrderCreated;
import com.example.inventory_service.avro.OrderUpdated;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

public final class OrderEventTypes {
    public static final String TYPE_HEADER = "__TypeId__";
    private static final Map<String, Class<? extends SpecificRecord>> BY_NAME = Map.of(
            "ORDER_CREATED", OrderCreated.class,
            "ORDER_UPDATED", OrderUpdated.class,
            "ORDER_CANCELLED", OrderCancelled.class);
    private OrderEventTypes() {}

    public static String nameOf(Class<?> type) {
        return BY_NAME.entrySet().stream().filter(e -> e.getValue().equals(type))
                .map(Map.Entry::getKey).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No event type for " + type.getName()));
    }
    public static Class<? extends SpecificRecord> classOf(String name) {
        Class<? extends SpecificRecord> t = BY_NAME.get(name);
        if (t == null) throw new IllegalArgumentException("Unknown event type '" + name + "'");
        return t;
    }
}