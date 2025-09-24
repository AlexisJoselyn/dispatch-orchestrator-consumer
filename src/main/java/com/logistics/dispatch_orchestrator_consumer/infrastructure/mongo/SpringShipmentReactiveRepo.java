package com.logistics.dispatch_orchestrator_consumer.infrastructure.mongo;

import com.logistics.dispatch_orchestrator_consumer.domain.entity.ShipmentEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface SpringShipmentReactiveRepo
        extends ReactiveMongoRepository<ShipmentEntity, String> {

    /**
     * Verifica si existe un envío por su shipmentId.
     * @param shipmentId identificador del envío
     * @return Mono<Boolean> indicando existencia
     */
    Mono<Boolean> existsByShipmentId(String shipmentId);

    /**
     * Busca un envío por su shipmentId.
     * @param shipmentId identificador del envío
     * @return Mono<ShipmentEntity> con el envío encontrado o vacío
     */
    Mono<ShipmentEntity> findByShipmentId(String shipmentId);
}
