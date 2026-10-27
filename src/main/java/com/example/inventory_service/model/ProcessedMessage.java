package com.example.inventory_service.model;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name ="processed_messages")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProcessedMessage {

    @Id
    private String messageId;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;
}
