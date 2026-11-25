package com.example.inventory_service.config;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Log4j2
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate){

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    if(record.topic().endsWith(".DLT")){
                        log.error("Failed to process message on DLT topic {}. Halting recovery", record.topic());
                        return null;
                    }
                    String destinationTopic = record.topic() + ".DLT";
                    log.warn("Routing record from topic {} to DLT: {}", record.topic(), destinationTopic);
                    return new TopicPartition(destinationTopic, -1);
                }
        );

        FixedBackOff backOff = new FixedBackOff(2000L, 3L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            if(deliveryAttempt > 1){
                log.info("Retry attempt #{} for record key={}: {}", deliveryAttempt-1, record.key(), record.value());
            }
        });

        return errorHandler;
    }


}
