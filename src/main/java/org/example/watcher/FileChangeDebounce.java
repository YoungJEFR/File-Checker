package org.example.watcher;

import org.example.model.FileTask;
import org.example.model.PendingDebounce;
import org.example.route.TaskRouter;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FileChangeDebounce {
    private final ScheduledExecutorService scheduled;
    private final Map<Path, PendingDebounce> tasks = new ConcurrentHashMap<>();
    private final TaskRouter taskRouter;
    private final AtomicInteger numberTask = new AtomicInteger(0);
    private final Object lifecycleLock = new Object();
    private boolean acceptingRequest = true;

    public FileChangeDebounce(
            ScheduledExecutorService scheduled,
            TaskRouter taskRouter
    ) {
        this.scheduled = scheduled;
        this.taskRouter = taskRouter;
    }

    public void debounceOnModify(FileTask fileTask) {
        synchronized (lifecycleLock) {
            if (!acceptingRequest) {
                return;
            }

            tasks.compute(fileTask.path(), (path, oldTask) -> {
                if (oldTask != null) {
                    oldTask.getExpectedFuture().cancel(false);
                }

                int currentNumber = numberTask.getAndIncrement();

                ScheduledFuture<?> newTask = scheduled.schedule(() -> {
                            try {
                                taskRouter.route(fileTask);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            } finally {
                                tasks.computeIfPresent(path, (key, actualPending) -> {
                                    if (actualPending.getNumberTask() == currentNumber) {
                                        return null;
                                    }

                                    return actualPending;
                                });
                            }
                        },
                        500,
                        TimeUnit.MILLISECONDS
                );

                PendingDebounce pendingDebounce = new PendingDebounce(currentNumber, newTask);

                return pendingDebounce;
            });
        }
    }


    public void shutdown() {
        synchronized (lifecycleLock) {
            if (!acceptingRequest) {
                return;
            }


            tasks.forEach((key, task) -> {
                task.getExpectedFuture().cancel(false);
            });

            tasks.clear();

            acceptingRequest = false;
        }
    }

    public void debounceCancel(Path path) {
        PendingDebounce pendingDebounce = tasks.remove(path);

        if (pendingDebounce != null) {
            pendingDebounce.getExpectedFuture().cancel(false);
        }
    }
}
