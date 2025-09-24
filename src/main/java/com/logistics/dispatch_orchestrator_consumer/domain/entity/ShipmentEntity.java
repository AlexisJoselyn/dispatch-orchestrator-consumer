package com.logistics.dispatch_orchestrator_consumer.domain.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Representa un envío dentro del sistema de logística.
 * Se persiste en MongoDB en la colección "shipments".
 */
@Document("shipments")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ShipmentEntity {

    /** Identificador único del envío (equivalente al requestId del productor). */
    @Id
    private String shipmentId;

    /** Identificador de la orden asociada al envío. */
    private String orderId;

    /** Identificador del cliente receptor. */
    private String customerId;

    /** Dirección de entrega. */
    private String address;

    /** Ciudad de entrega. */
    private String city;

    /** Código postal de la dirección de entrega. */
    private String postalCode;

    /** Nivel de servicio (ej: estándar, express, etc.). */
    private String serviceLevel;

    /** Punto de origen (almacén o dirección de salida). */
    private String origin;

    /** Punto de destino (dirección de entrega final). */
    private String destination;

    /** Estado actual del envío (ej: PENDING, DISPATCHED, DELIVERED, FAILED). */
    private String status;

    /** Marca de tiempo en la que se creó la solicitud de envío. */
    private Long requestedAt;

    /** Marca de tiempo en la que el envío fue despachado. */
    private Long dispatchedAt;

    /** Marca de tiempo en la que el envío fue entregado. */
    private Long deliveredAt;

    /** Marca de tiempo en la que el envío fue procesado por este servicio. */
    private Long processedAt;

    /** Identificador para trazabilidad en flujos distribuidos. */
    private String correlationId;

    /** Número de intentos realizados para despachar o entregar el envío. */
    private Integer attemptNumber;

    /** Payload original recibido del evento Kafka. */
    private String rawPayload;

    /** Mensaje de error en caso de que ocurra un fallo en el proceso. */
    private String errorMessage;
}
