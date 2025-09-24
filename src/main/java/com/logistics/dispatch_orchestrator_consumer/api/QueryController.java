package com.logistics.dispatch_orchestrator_consumer.api;


import com.logistics.dispatch_orchestrator_consumer.domain.entity.ShipmentEntity;
import com.logistics.dispatch_orchestrator_consumer.domain.repo.ShipmentRepository;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class QueryController {

    private final ShipmentRepository repo;

    /**
     * GET /api/shipments/{shipmentId} -> 200 con el documento o 404 si no existe
     */
    @GetMapping("/shipments/{shipmentId}")
    public Single<ResponseEntity<ShipmentEntity>> byPath(@PathVariable String shipmentId) {
        return repo.findByShipmentId(shipmentId)       // Maybe<ShipmentEntity>
                .map(ResponseEntity::ok)                // 200 OK
                .switchIfEmpty(Maybe.just(ResponseEntity.notFound().build()))
                .toSingle();
    }

    /**
     * GET /api/shipments?shipmentId=... -> igual que arriba (fallback)
     */
    @GetMapping("/shipments")
    public Single<ResponseEntity<ShipmentEntity>> byQuery(@RequestParam String shipmentId) {
        return repo.findByShipmentId(shipmentId)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Maybe.just(ResponseEntity.notFound().build()))
                .toSingle();
    }

    /**
     * GET /api/health -> simple health
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
}

