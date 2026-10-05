package org.example.watcher;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(5)
class FileWatcherModifyTest {
    @TempDir
    Path tempDir;

    private final ScheduledExecutorService debounceExecutor =
            Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService reconciliationExecutor =
            Executors.newSingleThreadExecutor();
    private final List<FileTask> receivedByDebounce = new ArrayList<>();
    private final BlockingQueue<WorkerTask> queue = new LinkedBlockingQueue<>();

    @Test
    void modifyOfMarkdownDirectoryShouldNotReachDebounce() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("folder.md"));
        FileWatcher watcher = createWatcher();

        // Тот же обработчик вызывается из watch() для ENTRY_MODIFY.
        watcher.handleModify(directory);

        assertTrue(receivedByDebounce.isEmpty(),
                "Папка с расширением .md не должна передаваться в debounce");
        assertTrue(queue.isEmpty(), "Папка не должна попадать в очередь файлов");
    }

    @Test
    void modifyOfFileInsideMarkdownDirectoryShouldReachDebounce() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("folder.md"));
        Path file = Files.writeString(directory.resolve("note.md"), "content");
        FileWatcher watcher = createWatcher();

        watcher.handleModify(file);

        assertEquals(List.of(new FileTask(file, ChangeType.MODIFIED)),
                receivedByDebounce);
    }

    private FileWatcher createWatcher() {
        TaskRouter router = new TaskRouter(List.of(queue));
        // Записываем вызовы без таймера: тестируем watcher, а не debounce.
        FileChangeDebounce debounce = new FileChangeDebounce(debounceExecutor, router) {
            @Override
            public void debounceOnModify(FileTask task) {
                receivedByDebounce.add(task);
            }
        };
        DirectoryReconciliationService reconciliation = new DirectoryReconciliationService(
                reconciliationExecutor,
                new DirectoryReconciler(new FileScanner(), new FileIndex(), router)
        );
        return new FileWatcher(tempDir, debounce, router,
                new WatchRegistrar(new ConcurrentHashMap<>()),
                reconciliation, new CountDownLatch(1));
    }

    @AfterEach
    void stopExecutors() throws InterruptedException {
        debounceExecutor.shutdownNow();
        reconciliationExecutor.shutdownNow();
        assertTrue(debounceExecutor.awaitTermination(2, TimeUnit.SECONDS));
        assertTrue(reconciliationExecutor.awaitTermination(2, TimeUnit.SECONDS));
    }
}
