package org.example.recovery;

import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileRecoveryCoordinator {
    private final ConcurrentHashMap<Path, RecoveryState> activeRecoveries;
    private final ScheduledExecutorService recoveryScheduler;
    private final TaskRouter router;
    private final int maxAttempts;
    private final RecoveryExhaustedHandler exhaustedHandler;

    private static final long INITIAL_DELAY = 500L;

    public FileRecoveryCoordinator(
            ScheduledExecutorService recoveryScheduler,
            TaskRouter router,
            int maxAttempts,
            RecoveryExhaustedHandler exhaustedHandler
    ) {
        this.recoveryScheduler = recoveryScheduler;
        this.activeRecoveries = new ConcurrentHashMap<>();
        this.router = router;
        this.exhaustedHandler = exhaustedHandler;

        if (exhaustedHandler == null) {
            throw new NullPointerException("exhaustedHandler is null");
        }
        if (maxAttempts > 0) {
            this.maxAttempts = maxAttempts;
        } else {
            throw new IllegalArgumentException("maxAttempts must be greater than 0");
        }
    }


    public boolean onFailure(
            FileTask failedTask,
            IOException cause
    ) {

        if (failedTask.taskSource() == TaskSource.NORMAL) {
            AtomicBoolean recoveryStarted = new AtomicBoolean(false);
            activeRecoveries.computeIfAbsent(
                    failedTask.path(),
                    path -> {
                        ScheduledFuture<?> retryFuture = recoveryScheduler.schedule(() -> {
                                    try {
                                        router.route(new FileTask(
                                                failedTask.path(),
                                                failedTask.changeType(),
                                                TaskSource.RECOVERY
                                        ));
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                },
                                INITIAL_DELAY,
                                TimeUnit.MILLISECONDS
                        );
                        RecoveryState newState = new RecoveryState(
                                1,
                                retryFuture
                        );
                        recoveryStarted.set(true);
                        return newState;
                    }
            );
            return recoveryStarted.get();
        } else if (failedTask.taskSource() == TaskSource.RECOVERY) {
            RecoveryState state = activeRecoveries.get(failedTask.path());
            if (state != null) {
                if (state.attemptCount() < maxAttempts) {
                    long delay = INITIAL_DELAY * (1L << state.attemptCount());
                    ScheduledFuture<?> newScheduledFuture = recoveryScheduler.schedule(() -> {
                                try {
                                    router.route(failedTask);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            },
                            delay,
                            TimeUnit.MILLISECONDS
                    );
                    int count = state.attemptCount() + 1;
                    RecoveryState newState = new RecoveryState(
                            count,
                            newScheduledFuture
                    );
                    activeRecoveries.put(failedTask.path(), newState);
                } else {
                    boolean removed = activeRecoveries.remove(
                            failedTask.path(),
                            state
                    );

                    if (removed) {
                        exhaustedHandler.handle(failedTask, cause);
                    }
                }
            }
            return false;
        } else if (failedTask.taskSource() == TaskSource.RECONCILIATION) {
            System.err.println("Recovery failed: " + cause.getMessage() + ". Путь " + failedTask.path());
            cause.printStackTrace(System.err);

            return false;
        } else {
            return false;
        }

    }

    public void onSuccess(Path path) {
        RecoveryState state = activeRecoveries.remove(path);
        if (state != null) {
            ScheduledFuture<?> scheduledFuture = state.scheduledFuture();

            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }
    }
}
