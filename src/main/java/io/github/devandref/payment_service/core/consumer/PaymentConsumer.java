package io.github.devandref.payment_service.core.consumer;

import io.github.devandref.payment_service.core.service.PaymentService;
import io.github.devandref.payment_service.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class PaymentConsumer {

    private final JsonUtil jsonUtil;
    private final PaymentService paymentService;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-success}"
    )
    public void consumerSuccessEvent(String payload) {
        log.info("Receiving event {} from payment-success topic", payload);
        var event = jsonUtil.toEvent(payload);
        paymentService.realizePayment(event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-fail}"
    )
    public void consumeFailEvent(String payload) {
        log.info("Receiving event {} from payment-fail topic", payload);
        var event = jsonUtil.toEvent(payload);
        paymentService.realizeRefund(event);
    }


}
