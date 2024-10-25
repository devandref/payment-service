package io.github.devandref.payment_service.core.consumer;

import io.github.devandref.payment_service.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class InventoryServiceConsumer {

    private final JsonUtil jsonUtil;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.orchestrator}"
    )
    public void consumerOrchestratorEvent(String payload) {
        log.info("Receiving event {} from orchestrator topic", payload);
        var event = jsonUtil.toEvent(payload);
        log.info("Event object {}", event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-success}"
    )
    public void consumerPaymentSuccessEvent(String payload) {
        log.info("Receiving event {} from payment-success topic", payload);
        var event = jsonUtil.toEvent(payload);
        log.info("Event object {}", event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-fail}"
    )
    public void consumerPaymentFailEvent(String payload) {
        log.info("Receiving event {} from payment-fail topic", payload);
        var event = jsonUtil.toEvent(payload);
        log.info("Event object {}", event);
    }


}
