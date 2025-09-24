package com.logistics.dispatch_orchestrator_consumer.infrastructure.kafka;

import com.logistics.dispatch_orchestrator_consumer.config.KafkaTopicsProperties;
import com.logistics.dispatch_orchestrator_consumer.domain.mapper.EntityMapper;
import com.logistics.dispatch_orchestrator_consumer.domain.model.ShipmentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaRxConsumer {

    private final ReceiverOptions<String, Object> baseOptions;
    private final KafkaTopicsProperties topics;
    private final EntityMapper mapper;

    public io.reactivex.rxjava3.core.Flowable<EventMessage<ShipmentEvent>> stream() {
        var options = baseOptions.subscription(Collections.singleton(topics.getMain()));
        return RxJava3Adapter.fluxToFlowable(
                KafkaReceiver.create(options)
                        .receive()
                        .map(this::toMessage)
        );
    }

    private EventMessage<ShipmentEvent> toMessage(ReceiverRecord<String, Object> rec) {
        Object value = rec.value();
        ShipmentEvent ev = null;
        try {
            if (value instanceof GenericRecord gr) {
                ev = mapper.toEvent(gr); // Avro -> dominio
            } else {
                log.warn("Valor no Avro ({}), se ignora", value == null ? "null" : value.getClass());
            }
        } catch (Exception e) {
            log.error("Error mapeando Avro -> ShipmentEvent", e);
        }
        return new EventMessage<>(ev, rec.receiverOffset(), value);
    }
}
