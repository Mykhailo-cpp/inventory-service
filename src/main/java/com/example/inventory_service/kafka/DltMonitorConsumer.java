package com.example.inventory_service.kafka;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class DltMonitorConsumer {

    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "dlt-monitor-group",
            properties = {
                    "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
            }
    )
    public void listenDlt(ConsumerRecord<String, String> record){
       log.warn("RECEIVED DLT EVENT >>> Key: {}, Value: {}, Partition: {}, Offset: {}",
               record.key(), record.value(), record.partition(), record.offset());
    }
}
