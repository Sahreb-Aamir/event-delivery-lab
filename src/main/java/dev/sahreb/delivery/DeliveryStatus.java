package dev.sahreb.delivery;

import java.util.List;

public record DeliveryStatus(String status, List<DeliveryAttempt> attempts) {

    public DeliveryStatus {
        attempts = List.copyOf(attempts);
    }
}
