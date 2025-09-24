package com.logistics.dispatch_orchestrator_consumer.domain.model;

import lombok.*;
import java.time.Instant;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentEvent {
    private String eventId;
    private String shipmentId;
    private String orderId;
    private String customerId;
    private String address;
    private String city;
    private String postalCode;
    private String serviceLevel;
    private Instant requestedAt;   // ← mejor que long, más semántico
    private Integer attemptNumber; // ← 1 = primera vez, 2 = reintento
    private String correlationId;
    private String status;
}
