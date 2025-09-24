package com.logistics.dispatch_orchestrator_consumer.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
@RequiredArgsConstructor
public class DltProducer {
    private final KafkaSender<String, String> sender;
    private final KafkaTopicsProperties topics;

    public Mono<Void> send(String key, String value) {
        return sender.send(
                Mono.just(SenderRecord.create(
                        new ProducerRecord<>(topics.getDlt(), key, value), null))
        ).then();
    }
}
