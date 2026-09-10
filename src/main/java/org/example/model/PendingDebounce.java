package org.example.model;

import java.util.concurrent.ScheduledFuture;

public class PendingDebounce {
    private final int numberTask;
    private final ScheduledFuture<?> expectedFuture;

    public PendingDebounce(int numberTask , ScheduledFuture<?> expectedFuture) {
        this.expectedFuture = expectedFuture;
        this.numberTask = numberTask;
    }

    public int getNumberTask() {
        return numberTask;
    }

    public ScheduledFuture<?> getExpectedFuture() {
        return expectedFuture;
    }
}

