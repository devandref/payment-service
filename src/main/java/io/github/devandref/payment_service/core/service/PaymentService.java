package io.github.devandref.payment_service.core.service;

import io.github.devandref.payment_service.config.KafkaConfig;
import io.github.devandref.payment_service.config.exeception.ValidationException;
import io.github.devandref.payment_service.core.dto.Event;
import io.github.devandref.payment_service.core.dto.History;
import io.github.devandref.payment_service.core.dto.OrderProducts;
import io.github.devandref.payment_service.core.enums.EPaymentStatus;
import io.github.devandref.payment_service.core.enums.ESagaStatus;
import io.github.devandref.payment_service.core.model.Payment;
import io.github.devandref.payment_service.core.repository.PaymentRepository;
import io.github.devandref.payment_service.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@AllArgsConstructor
public class PaymentService {

    private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";
    private static final Double REDUCE_SUM_VALUE = 0.0;
    private static final Double MIN_AMOUNT_VALUE = 0.1;

    private final JsonUtil jsonUtil;
    private final KafkaConfig kafkaConfig;
    private final PaymentRepository paymentRepository;

    public void realizePayment(Event event) {
        try {
            checkCurrentValidation(event);
            createPendingPayment(event);
            var payment = findByOrderIdAndTransactionId(event);
            validateAmount(payment.getTotalAmount());
            changePaymentSuccess(payment);
            handleSuccess(event);
        } catch (Exception ex) {
            log.error("Error trying to make payment: ", ex);
        }
    }

    private void createPendingPayment(Event event) {
        Double totalAmount = calculateAmount(event);
        Integer totalItem = calculateTotalItems(event);
        var payment = Payment
         .builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionalId())
                .totalAmount(totalAmount)
                .totalItems(totalItem)
         .build();
        save(payment);
        setEventAmountItems(event, payment);
    }

    private Double calculateAmount(Event event) {
        return event
                .getPayload()
                .getProducts()
                .stream()
                .map(product -> product.getQuantity() * product.getProduct().getUnitValue())
                .reduce(REDUCE_SUM_VALUE, Double::sum);
    }

    private Integer calculateTotalItems(Event event) {
        return event
                .getPayload()
                .getProducts()
                .stream()
                .map(OrderProducts::getQuantity)
                .reduce(REDUCE_SUM_VALUE.intValue(), Integer::sum);
    }

    private void setEventAmountItems(Event event, Payment payment) {
        event.getPayload().setTotalAmount(payment.getTotalAmount());
        event.getPayload().setTotalItems(payment.getTotalItems());
    }

    private Payment findByOrderIdAndTransactionId(Event event) {
        return paymentRepository
                .findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionalId())
                .orElseThrow(() -> new ValidationException("Payment not found by OrderID and TransactionID"));
    }

    private void validateAmount(Double amount) {
        if(amount < MIN_AMOUNT_VALUE) {
            throw new ValidationException("The minimum amount available is ".concat(MIN_AMOUNT_VALUE.toString()));
        }
    }

    private void changePaymentSuccess(Payment payment) {
        payment.setStatus(EPaymentStatus.SUCCESS);
        save(payment);
    }

    private void save(Payment payment) {
        paymentRepository.save(payment);
    }

    private void checkCurrentValidation(Event event) {
        if (paymentRepository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionalId())) {
            throw new ValidationException("There's another transactionId for this validation.");
        }
    }

    private void handleSuccess(Event event) {
        event.setStatus(ESagaStatus.SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Payment realized successfully!");

    }

    private void addHistory(Event event, String message) {
        var history = History
                .builder()
                .source(event.getSource())
                .status(event.getStatus())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();
        event.addToHistory(history);
    }

}
