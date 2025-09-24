package com.logistics.dispatch_orchestrator_consumer.domain.mapper;

import com.logistics.dispatch_orchestrator_consumer.domain.entity.ShipmentEntity;
import com.logistics.dispatch_orchestrator_consumer.domain.model.ShipmentEvent;
import org.apache.avro.generic.GenericRecord;
import org.mapstruct.Mapper;

import java.time.Instant;

@Mapper(componentModel = "spring")
public interface EntityMapper {

    // Avro GenericRecord -> Modelo de dominio
    default ShipmentEvent toEvent(GenericRecord gr) {
        if (gr == null) return null;
        ShipmentEvent e = new ShipmentEvent();
        e.setEventId((String) gr.get("eventId"));
        e.setShipmentId((String) gr.get("shipmentId"));
        e.setOrderId((String) gr.get("orderId"));
        e.setCustomerId((String) gr.get("customerId"));
        e.setAddress((String) gr.get("address"));
        e.setCity((String) gr.get("city"));
        e.setPostalCode((String) gr.get("postalCode"));
        e.setServiceLevel((String) gr.get("serviceLevel"));

        Object ts = gr.get("requestedAt");
        if (ts instanceof Long l) {
            e.setRequestedAt(Instant.ofEpochMilli(l));
        }

        Object at = gr.get("attemptNumber");
        if (at instanceof Integer i) {
            e.setAttemptNumber(i);
        }

        e.setCorrelationId((String) gr.get("correlationId"));
        e.setStatus((String) gr.get("status"));
        return e;
    }

    // Dominio -> Entidad Mongo
    default ShipmentEntity toEntity(ShipmentEvent ev) {
        if (ev == null) return null;
        ShipmentEntity en = new ShipmentEntity();
        en.setShipmentId(ev.getShipmentId());
        en.setOrderId(ev.getOrderId());
        en.setCustomerId(ev.getCustomerId());
        en.setAddress(ev.getAddress());
        en.setCity(ev.getCity());
        en.setPostalCode(ev.getPostalCode());
        en.setServiceLevel(ev.getServiceLevel());
        en.setRequestedAt(ev.getRequestedAt() != null ? ev.getRequestedAt().toEpochMilli() : 0L);
        en.setAttemptNumber(ev.getAttemptNumber() == null ? 1 : ev.getAttemptNumber());
        en.setCorrelationId(ev.getCorrelationId());
        en.setStatus(ev.getStatus());
        return en;
    }
}
