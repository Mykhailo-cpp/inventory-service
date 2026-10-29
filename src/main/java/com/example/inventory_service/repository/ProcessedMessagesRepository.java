package com.example.inventory_service.repository;

import com.example.inventory_service.model.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedMessagesRepository extends JpaRepository<ProcessedMessage, String> {

    boolean existsByMessageId(String messageId);
}
