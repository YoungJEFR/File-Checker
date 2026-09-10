package example;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.reconciliation.DirectoryRecoveryHandler;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DirectoryRecoveryHandlerTest {

    @TempDir
    Path tempDir;

    private ExecutorService executor;
    private BlockingQueue<WorkerTask> queue;
    private DirectoryRecoveryHandler handler;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        queue = new ArrayBlockingQueue<>(20);
        TaskRouter router = new TaskRouter(List.of(queue));
        DirectoryReconciler reconciler = new DirectoryReconciler(
                new FileScanner(),
                new FileIndex(new ConcurrentHashMap<>()),
                router
        );
        DirectoryReconciliationService reconciliationService =
                new DirectoryReconciliationService(executor, reconciler);
        handler = new DirectoryRecoveryHandler(reconciliationService);
    }

    @AfterEach
    void stopExecutor() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void shouldReconcileParentDirectoryAsynchronously() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("docs"));
        Path actualFile = Files.write(
                directory.resolve("actual.md"),
                new byte[10]
        );
        FileTask failedTask = new FileTask(
                directory.resolve("failed.md"),
                ChangeType.MODIFIED,
                TaskSource.RECOVERY
        );

        handler.handle(failedTask, new IOException("last failure"));

        FileTask reconciliationTask = (FileTask) queue.poll(
                2,
                TimeUnit.SECONDS
        );

        assertEquals(actualFile, reconciliationTask.path());
        assertEquals(ChangeType.MODIFIED, reconciliationTask.changeType());
        assertEquals(
                TaskSource.RECONCILIATION,
                reconciliationTask.taskSource()
        );
    }

    @Test
    void pathWithoutParentShouldNotSubmitReconciliation() {
        FileTask failedTask = new FileTask(
                Path.of("failed.md"),
                ChangeType.MODIFIED,
                TaskSource.RECOVERY
        );

        assertDoesNotThrow(() ->
                handler.handle(
                        failedTask,
                        new IOException("last failure")
                )
        );
        assertNull(queue.poll());
    }
}
