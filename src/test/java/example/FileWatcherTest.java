package example;

import org.example.filescanner.FileScanner;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.example.watcher.FileWatcher;
import org.example.watcher.WatchRegistrar;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWatcherTest {

    private static final Duration WATCHER_START_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(3);

    @TempDir
    Path tempDir;

    private ScheduledExecutorService debounceExecutor;
    private ExecutorService rescanExecutor;
    private Thread watcherThread;
    private FileWatcher fileWatcher;

    private BlockingQueue<WorkerTask> queue;
    private CountDownLatch watcherReady;

    @AfterEach
    void stopThreads() throws InterruptedException {
        boolean watcherStopped = true;

        if (watcherThread != null) {
            watcherThread.interrupt();
            watcherThread.join(2_000);
            watcherStopped = !watcherThread.isAlive();
        }

        if (rescanExecutor != null) {
            rescanExecutor.shutdownNow();
            rescanExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }

        if (debounceExecutor != null) {
            debounceExecutor.shutdownNow();
            debounceExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }

        assertTrue(
                watcherStopped,
                "FileWatcher не завершился после interrupt"
        );
    }

    @Test
    void shouldRouteMarkdownFileCreatedInRootDirectory() throws Exception {
        startWatcher();

        Path file = tempDir.resolve("test.md");
        Files.writeString(file, "Hello");

        FileTask task = pollTaskFor(file, EVENT_TIMEOUT);

        assertNotNull(task, "Watcher не заметил созданный .md-файл");
        assertEquals(file, task.path());
    }

    @Test
    void shouldIgnoreNonMarkdownFile() throws Exception {
        startWatcher();

        Files.writeString(tempDir.resolve("test.txt"), "Hello");

        WorkerTask task = queue.poll(700, TimeUnit.MILLISECONDS);

        assertNull(task, "Watcher не должен создавать задачу для .txt-файла");
    }

    @Test
    void shouldDiscoverFileInsideDirectoryCreatedAfterWatcherStart() throws Exception {
        startWatcher();

        Path nestedDirectory = Files.createDirectories(
                tempDir.resolve("new-folder").resolve("nested")
        );
        Path file = nestedDirectory.resolve("inside.md");

        // Файл создаётся сразу: тест проверяет совместную работу регистрации
        // новой директории и повторного сканирования её содержимого.
        Files.writeString(file, "Hello");

        FileTask task = pollTaskFor(file, EVENT_TIMEOUT);

        assertNotNull(
                task,
                "Watcher и rescan не обнаружили файл в новой директории"
        );
        assertEquals(file, task.path());
    }

    @Test
    void shouldSignalReadinessAfterRegisteringExistingDirectories()
            throws Exception {
        Path nestedDirectory = Files.createDirectories(
                tempDir.resolve("existing").resolve("nested")
        );

        startWatcher();

        assertEquals(0, watcherReady.getCount());

        Path file = Files.writeString(
                nestedDirectory.resolve("after-ready.md"),
                "Hello"
        );
        FileTask task = pollTaskFor(file, EVENT_TIMEOUT);

        assertNotNull(
                task,
                "Вложенная директория не была зарегистрирована до сигнала готовности"
        );
    }

    @Test
    void startupFailureShouldReleaseLatchAndExposeCause()
            throws Exception {
        Path missingDirectory = tempDir.resolve("missing");

        startWatcher(missingDirectory);

        assertEquals(0, watcherReady.getCount());
        assertNotNull(
                fileWatcher.getStartupFailure(),
                "FileWatcher не сохранил ошибку запуска"
        );
    }

    private void startWatcher() throws InterruptedException {
        startWatcher(tempDir);
    }

    private void startWatcher(Path root) throws InterruptedException {
        queue = new ArrayBlockingQueue<>(100);

        TaskRouter router = new TaskRouter(List.of(queue));

        debounceExecutor = Executors.newSingleThreadScheduledExecutor();
        rescanExecutor = Executors.newSingleThreadExecutor();

        FileChangeDebounce debounce = new FileChangeDebounce(
                debounceExecutor,
                router
        );

        WatchRegistrar watchRegistrar =
                new WatchRegistrar(new ConcurrentHashMap<>());
        watcherReady = new CountDownLatch(1);

        DirectoryReconciler directoryReconciler =
                new DirectoryReconciler(
                        new FileScanner(),
                        new FileIndex(new ConcurrentHashMap<>()),
                        router
                );

        DirectoryReconciliationService reconciliationService =
                new DirectoryReconciliationService(
                        rescanExecutor,
                        directoryReconciler
                );

        fileWatcher = new FileWatcher(
                root,
                debounce,
                router,
                watchRegistrar,
                reconciliationService,
                watcherReady
        );

        watcherThread = new Thread(fileWatcher, "FileWatcherTest");
        watcherThread.start();

        assertTrue(
                watcherReady.await(
                        WATCHER_START_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS
                ),
                "Watcher не сообщил о готовности"
        );
    }

    private FileTask pollTaskFor(
            Path expectedPath,
            Duration timeout
    ) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline) {
            WorkerTask workerTask = queue.poll(
                    100,
                    TimeUnit.MILLISECONDS
            );

            if (workerTask instanceof FileTask task
                    && expectedPath.equals(task.path())) {
                return task;
            }
        }

        return null;
    }
}
