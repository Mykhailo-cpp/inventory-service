package com.example.inventory_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(){

        FixedBackOff backOff = new FixedBackOff(2000L, 3L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);

        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) ->
                System.out.println("Retry attempt #" + deliveryAttempt + " for record: " + record.value())));

        return errorHandler;
    }


}
