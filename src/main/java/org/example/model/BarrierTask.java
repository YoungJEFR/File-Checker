package org.example.model;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public record BarrierTask(
        CountDownLatch latch
) implements WorkerTask {

    public BarrierTask {
        Objects.requireNonNull(latch, "Latch must not be null");
    }
}
