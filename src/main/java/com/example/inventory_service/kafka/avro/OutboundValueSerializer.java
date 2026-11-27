package com.example.inventory_service.kafka.avro;


import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

public class OutboundValueSerializer implements Serializer<Object> {
    private final OrderEventAvroSerializer avro = new OrderEventAvroSerializer();
    private final JsonSerializer<Object> json = new JsonSerializer<>();

    @Override public void configure(Map<String, ?> configs, boolean isKey) {
        avro.configure(configs, isKey);
        json.configure(configs, isKey);
    }
    @Override public byte[] serialize(String topic, Object data) {
        return json.serialize(topic, data);
    }
    @Override public byte[] serialize(String topic, Headers headers, Object data) {
        if (data instanceof SpecificRecord r) return avro.serialize(topic, headers, r);
        return json.serialize(topic, headers, data);
    }
    @Override public void close() { avro.close(); json.close(); }
}
