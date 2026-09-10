package org.example.watcher;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FileWatcherOverflowTest {

    @TempDir
    Path tempDir;

    @Test
    void overflowShouldRequestDirectoryReconciliation() throws Exception {
        Path file = Files.writeString(
                tempDir.resolve("found-after-overflow.md"),
                "content"
        );
        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(20);
        TaskRouter router = new TaskRouter(List.of(queue));
        ExecutorService reconciliationExecutor =
                Executors.newSingleThreadExecutor();
        ScheduledExecutorService debounceExecutor =
                Executors.newSingleThreadScheduledExecutor();

        try {
            DirectoryReconciler reconciler = new DirectoryReconciler(
                    new FileScanner(),
                    new FileIndex(new ConcurrentHashMap<>()),
                    router
            );
            DirectoryReconciliationService reconciliationService =
                    new DirectoryReconciliationService(
                            reconciliationExecutor,
                            reconciler
                    );
            FileWatcher watcher = new FileWatcher(
                    tempDir,
                    new FileChangeDebounce(debounceExecutor, router),
                    router,
                    new WatchRegistrar(new ConcurrentHashMap<>()),
                    reconciliationService,
                    new CountDownLatch(1)
            );

            watcher.handleOverflow(tempDir);

            FileTask reconciliationTask = (FileTask) queue.poll(
                    2,
                    TimeUnit.SECONDS
            );

            assertNotNull(
                    reconciliationTask,
                    "OVERFLOW не запустил сверку директории"
            );
            assertEquals(file, reconciliationTask.path());
            assertEquals(
                    ChangeType.MODIFIED,
                    reconciliationTask.changeType()
            );
            assertEquals(
                    TaskSource.RECONCILIATION,
                    reconciliationTask.taskSource()
            );
        } finally {
            reconciliationExecutor.shutdownNow();
            reconciliationExecutor.awaitTermination(2, TimeUnit.SECONDS);
            debounceExecutor.shutdownNow();
            debounceExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }
}
