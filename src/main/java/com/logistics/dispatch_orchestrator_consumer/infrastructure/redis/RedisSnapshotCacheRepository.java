package com.logistics.dispatch_orchestrator_consumer.infrastructure.redis;

import com.logistics.dispatch_orchestrator_consumer.domain.repo.SnapshotCacheRepository;
import io.reactivex.rxjava3.core.Maybe;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;

/**
 * Implementación de {@link SnapshotCacheRepository} que utiliza Redis
 * como almacenamiento de snapshots.
 *
 * Los snapshots se guardan como cadenas JSON en Redis,
 * accediendo mediante claves construidas a partir del requestId.
 */
@Component
@RequiredArgsConstructor
public class RedisSnapshotCacheRepository implements SnapshotCacheRepository {

    private final ReactiveStringRedisTemplate redis;

    /**
     * Obtiene el snapshot en formato JSON desde Redis para un requestId dado.
     *
     * @param requestId identificador único del request
     * @return un Maybe que contiene el snapshot JSON si existe,
     *         o vacío si no está en Redis
     */
    @Override
    public Maybe<String> getSnapshotJson(String requestId) {
        return RxJava3Adapter.monoToMaybe(redis.opsForValue().get(key(requestId)));
    }

    /**
     * Construye la clave que se usará en Redis para almacenar el snapshot.
     *
     * @param id identificador del request
     * @return clave con prefijo "card:event:"
     */
    private String key(String id) {
        return "card:event:" + id;
    }
}
