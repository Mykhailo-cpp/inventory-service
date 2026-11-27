package com.example.inventory_service.kafka;

import com.example.inventory_service.kafka.avro.OrderEventAvroDeserializer;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class DltMonitorConsumer {

    private final OrderEventAvroDeserializer deserializer = new OrderEventAvroDeserializer();

    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "dlt-monitor-group",
            properties = {
                    "value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
            }
    )
    public void listenDlt(ConsumerRecord<String, byte[]> record){
       log.warn("RECEIVED DLT EVENT >>> Key: {}, Value: {}, Partition: {}, Offset: {}",
               record.key(), describe(record), record.partition(), record.offset());
    }

    private String describe(ConsumerRecord<String, byte[]> record) {
        try {
            return String.valueOf(deserializer.deserialize(record.topic(), record.headers(), record.value()));
        } catch (RuntimeException e) {
            int size = record.value() == null ? 0 : record.value().length;
            return "<undecodable, " + size + " bytes: " + e.getMessage() + ">";
        }
    }
}
