package com.logistics.dispatch_orchestrator_consumer.infrastructure.mongo;

import com.logistics.dispatch_orchestrator_consumer.domain.entity.ShipmentEntity;
import com.logistics.dispatch_orchestrator_consumer.domain.repo.ShipmentRepository;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;

@Component
@RequiredArgsConstructor
public class ShipmentRepositoryMongoAdapter implements ShipmentRepository {

    private final SpringShipmentReactiveRepo reactiveRepo;

    @Override
    public Maybe<ShipmentEntity> findByShipmentId(String shipmentId) {
        return RxJava3Adapter.monoToMaybe(reactiveRepo.findByShipmentId(shipmentId));
    }

    @Override
    public Single<Boolean> existsByShipmentId(String shipmentId) {
        return RxJava3Adapter.monoToSingle(reactiveRepo.existsByShipmentId(shipmentId));
    }

    @Override
    public Single<ShipmentEntity> save(ShipmentEntity entity) {
        return RxJava3Adapter.monoToSingle(reactiveRepo.save(entity));
    }
}
