package com.logistics.dispatch_orchestrator_consumer.domain.repo;

import com.logistics.dispatch_orchestrator_consumer.domain.entity.ShipmentEntity;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

public interface ShipmentRepository {

    /**
     * Busca un envío por shipmentId.
     */
    Maybe<ShipmentEntity> findByShipmentId(String shipmentId);

    /**
     * Verifica si existe un envío con ese shipmentId.
     */
    Single<Boolean> existsByShipmentId(String shipmentId);

    /**
     * Inserta o actualiza un envío en la colección.
     */
    Single<ShipmentEntity> save(ShipmentEntity entity);
}
