package org.example.recovery;

import java.util.concurrent.ScheduledFuture;

record RecoveryState(
        int attemptCount,
        ScheduledFuture<?> scheduledFuture
) {
}
