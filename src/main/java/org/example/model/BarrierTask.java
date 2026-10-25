package org.example.model;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public final class BarrierTask implements WorkerTask {
    private final CompletableFuture<Void> barrierFuture;
    private final AtomicInteger remainingWorkers;

    public BarrierTask(int countWorkers) {
        if (countWorkers < 1) {
            throw new IllegalArgumentException("countWorkers must be greater than 0");
        }
        barrierFuture = new CompletableFuture<>();
        remainingWorkers = new AtomicInteger(countWorkers);
    }

    public void arrive() {
        if (remainingWorkers.decrementAndGet() == 0) {
            barrierFuture.complete(null);
        }
    }

    public CompletableFuture<Void> getCompletionFuture() {
        return barrierFuture.copy();
    }
}
