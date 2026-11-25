package com.example.inventory_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String INVENTORY_RESULT_TOPIC = "inventory-result";
    public static final String ORDER_EVENTS_DLT_TOPIC = "order-events.DLT";

    @Bean
    public NewTopic inventoryResultTopic(){
        return TopicBuilder.name(INVENTORY_RESULT_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderEventsDltTopic(){
        return TopicBuilder.name(ORDER_EVENTS_DLT_TOPIC).partitions(3).replicas(1).build();
    }
}
