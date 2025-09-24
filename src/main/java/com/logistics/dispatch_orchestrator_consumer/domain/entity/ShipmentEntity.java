package com.logistics.dispatch_orchestrator_consumer.domain.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("shipments")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ShipmentEntity {

    @Id
    private String shipmentId;   // identificador único del envío (equivalente al requestId del profe)

    private String orderId;      // id de la orden relacionada
    private String customerId;   // cliente que recibe
    private String origin;       // dirección/almacén de salida
    private String destination;  // dirección de entrega
    private String status;       // estado del envío (ej: PENDING, DISPATCHED, DELIVERED, FAILED)

    private Long requestedAt;    // cuándo se creó la solicitud
    private Long dispatchedAt;   // cuándo se despachó
    private Long deliveredAt;    // cuándo se entregó

    private String correlationId; // trazabilidad (útil si pasa por varios servicios)
    private Integer attemptNumber; // nro de intentos de entrega

    // metadatos adicionales
    private String rawPayload;   // payload original recibido del evento
    private String errorMessage; // si hubo algún error en el proceso
}
