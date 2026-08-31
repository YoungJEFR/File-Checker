package org.example.watcher;

import org.example.model.FileTask;
import org.example.route.TaskRouter;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;

public class FileChangeDebounce {
    private final ScheduledExecutorService scheduled;
    private final Map<Path, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();
    private final TaskRouter taskRouter;
    public FileChangeDebounce(
            ScheduledExecutorService scheduled,
            TaskRouter taskRouter
    ) {
        this.scheduled = scheduled;
        this.taskRouter = taskRouter;
    }

    public void debounceOnModify(FileTask fileTask) {
        ScheduledFuture<?> oldFile = tasks.get(fileTask.path());
        if (oldFile != null) {
            oldFile.cancel(false);
        }

        ScheduledFuture<?> newTask = scheduled.schedule(() -> {

            try {
                taskRouter.route(fileTask);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            tasks.remove(fileTask.path());
        },
        500,
                TimeUnit.MILLISECONDS
        );

        tasks.put(fileTask.path(), newTask);
    }

    public void debounceCancel(Path path) {
        ScheduledFuture<?> future = tasks.remove(path);

        if (future != null) {
            future.cancel(false);
        }
    }
}
