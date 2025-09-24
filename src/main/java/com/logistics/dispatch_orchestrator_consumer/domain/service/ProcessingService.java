package com.logistics.dispatch_orchestrator_consumer.domain.service;

import com.logistics.dispatch_orchestrator_consumer.domain.entity.ShipmentEntity;
import com.logistics.dispatch_orchestrator_consumer.domain.mapper.EntityMapper;
import com.logistics.dispatch_orchestrator_consumer.domain.model.ShipmentEvent;
import com.logistics.dispatch_orchestrator_consumer.domain.repo.ShipmentRepository;
import com.logistics.dispatch_orchestrator_consumer.infrastructure.kafka.DltProducer;
import com.logistics.dispatch_orchestrator_consumer.infrastructure.kafka.EventMessage;
import com.logistics.dispatch_orchestrator_consumer.infrastructure.kafka.KafkaRxConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

    private final KafkaRxConsumer consumer;
    private final ShipmentRepository mongoRepo;
    private final DltProducer dlt;
    private final EntityMapper mapper;
    private final MeterRegistry meter;
    private final ObjectMapper json = new ObjectMapper();

    private Disposable subscription;

    // ====== Métricas ======
    private Counter consumed;
    private Counter duplicateAck;
    private Counter persistedFirst;
    private Counter upsertSecond;
    private Counter sentToDlt;
    private Counter processingErrors;
    private Timer   processTimer;

    @PostConstruct
    public void start() {
        // Inicializa métricas
        consumed        = Counter.builder("shipment_events_consumed_total")
                .description("Eventos consumidos desde Kafka").register(meter);
        duplicateAck    = Counter.builder("shipment_duplicate_ack_total")
                .description("Primer intento duplicado, solo ACK").register(meter);
        persistedFirst  = Counter.builder("shipment_persist_first_total")
                .description("Insert en Mongo en primer intento").register(meter);
        upsertSecond    = Counter.builder("shipment_upsert_second_total")
                .description("Upsert en Mongo en segundo intento").register(meter);
        sentToDlt       = Counter.builder("shipment_dlt_total")
                .description("Eventos enviados a DLT").register(meter);
        processingErrors= Counter.builder("shipment_processing_errors_total")
                .description("Errores durante el procesamiento").register(meter);
        processTimer    = Timer.builder("shipment_process_timer")
                .description("Duración del procesamiento por evento")
                .publishPercentileHistogram()
                .register(meter);

        subscription = consumer.stream()
                .observeOn(Schedulers.io())
                .flatMapCompletable(this::routeAndProcess, false, 4)
                .subscribe(
                        () -> log.info("Stream completed"),
                        err -> log.error("Stream error", err)
                );
    }

    private Completable routeAndProcess(EventMessage<ShipmentEvent> msg) {
        return Completable.defer(() -> {
            long start = System.nanoTime();
            try {
                consumed.increment();

                ShipmentEvent ev = msg.getPayload();
                if (ev == null || ev.getShipmentId() == null) {
                    sentToDlt.increment();
                    return sendToDltAndAck("unknown", safeJson(msg.getRawKafkaValue()), msg)
                            .doOnTerminate(() ->
                                    processTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS));
                }

                int attempt = ev.getAttemptNumber() == null ? 1 : ev.getAttemptNumber();
                return (attempt <= 1 ? handleFirstAttempt(ev, msg) : handleSecondAttempt(ev, msg))
                        .doOnTerminate(() ->
                                processTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS))
                        .onErrorResumeNext(err -> {
                            processingErrors.increment();
                            log.error("Error processing shipmentId={}", ev.getShipmentId(), err);
                            sentToDlt.increment();
                            return sendToDltAndAck(ev.getShipmentId(), safeJson(ev), msg);
                        });

            } catch (Throwable t) {
                processingErrors.increment();
                log.error("Fatal error before routing", t);
                return Completable.fromAction(msg::ack)
                        .doOnTerminate(() ->
                                processTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS));
            }
        });
    }

    /** Primer intento → persistir en Mongo si no existe */
    private Completable handleFirstAttempt(ShipmentEvent ev, EventMessage<ShipmentEvent> msg) {
        return mongoRepo.existsByShipmentId(ev.getShipmentId())
                .flatMapCompletable(exists -> exists
                        ? ackDuplicate(ev, msg)
                        : persistAndDispatch(ev, "DISPATCHED", msg));
    }

    /** Segundo intento → upsert en Mongo */
    private Completable handleSecondAttempt(ShipmentEvent ev, EventMessage<ShipmentEvent> msg) {
        return persistAndDispatch(ev, "DISPATCHED_RETRY", msg);
    }

    private Completable ackDuplicate(ShipmentEvent ev, EventMessage<ShipmentEvent> msg) {
        return Completable.fromAction(() -> {
            duplicateAck.increment();
            log.warn("Duplicate (1st attempt) shipmentId={}, ack only", ev.getShipmentId());
            msg.ack();
        });
    }

    private Completable persistAndDispatch(ShipmentEvent ev, String status, EventMessage<ShipmentEvent> msg) {
        ShipmentEntity entity = mapper.toEntity(ev);
        entity.setStatus(status);
        entity.setProcessedAt(Instant.now().toEpochMilli());

        return mongoRepo.save(entity)
                .doOnSuccess(saved -> {
                    if ("DISPATCHED".equals(status)) {
                        persistedFirst.increment();
                    } else {
                        upsertSecond.increment();
                    }
                })
                .flatMapCompletable(saved -> simulateDispatch(ev))
                .andThen(Completable.fromAction(msg::ack));
    }

    /** Simula el despacho externo */
    private Completable simulateDispatch(ShipmentEvent ev) {
        return Completable.timer(150, TimeUnit.MILLISECONDS)
                .doOnComplete(() -> log.info("Dispatched attempt={} shipmentId={} customer={}",
                        ev.getAttemptNumber(), ev.getShipmentId(), ev.getCustomerId()));
    }

    private Completable sendToDltAndAck(String key, String jsonPayload, EventMessage<?> msg) {
        return Completable.fromPublisher(dlt.send(key, jsonPayload))
                .andThen(Completable.fromAction(msg::ack));
    }

    private String safeJson(Object o) {
        try { return json.writeValueAsString(o); }
        catch (Exception e) { return "{}"; }
    }
}
