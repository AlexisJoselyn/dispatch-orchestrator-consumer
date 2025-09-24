package com.logistics.dispatch_orchestrator_consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * Configuración de Redis para el uso de ReactiveStringRedisTemplate.
 *
 * Permite trabajar con Redis de forma reactiva, usando cadenas
 * como clave/valor en las operaciones.
 */
@Configuration
public class RedisConfig {

    /**
     * Expone un bean de ReactiveStringRedisTemplate, que facilita las
     * operaciones con Redis en modo reactivo.
     *
     * @param cf la conexión reactiva a Redis
     * @return instancia de ReactiveStringRedisTemplate
     */
    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory cf) {
        return new ReactiveStringRedisTemplate(cf);
    }
}
