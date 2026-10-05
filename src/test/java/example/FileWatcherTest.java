package example;

import org.example.filescanner.FileScanner;
import org.example.model.FileIndexerSnapshot;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;
import org.example.service.FileIndexerService;
import org.example.watcher.FileChangeDebounce;
import org.example.watcher.FileWatcher;
import org.example.watcher.WatchRegistrar;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void movingDirectoryOutsideRootShouldRemoveItsFilesFromIndex()
            throws IOException, InterruptedException {
        Path watchedDirectory =
                Files.createDirectory(tempDir.resolve("watched"));

        Path nestedDirectory =
                Files.createDirectory(watchedDirectory.resolve("nested"));

        Path outsideDirectory = Files.createDirectory(
                tempDir.resolve("outside")
        );

        Path files1 = Files.write(
                nestedDirectory.resolve("file1.md"),
                new byte[10]
        );

        Path files2 = Files.write(
                nestedDirectory.resolve("file2.md"),
                new byte[20]
        );


        try (FileIndexerService service = new FileIndexerService(
                3,
                watchedDirectory,
                3)) {

            service.start();

            FileIndexerSnapshot snapshotMap = service.snapshot();

            assertEquals(2, snapshotMap.countFiles());
            assertEquals(30, snapshotMap.totalBytes());
            assertEquals(0, snapshotMap.errorCount());
            assertEquals(
                    Set.of(files1, files2),
                    snapshotMap.files().keySet()
            );

            Files.move(
                    nestedDirectory,
                    outsideDirectory.resolve(nestedDirectory.getFileName())
            );

            FileIndexerSnapshot newSnapshotMap = service.snapshot();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);

            while (System.nanoTime() < deadline
                    && !(newSnapshotMap.files().isEmpty()
                    && newSnapshotMap.countFiles() == 0
                    && newSnapshotMap.totalBytes() == 0)){
                Thread.sleep(20);
                newSnapshotMap = service.snapshot();
            }

            assertTrue(newSnapshotMap.files().isEmpty());

            assertEquals(0, newSnapshotMap.countFiles());
            assertEquals(0, newSnapshotMap.totalBytes());
            assertEquals(0, newSnapshotMap.errorCount());

        }

    }

    @Test
    void deletingDirectoryTreeShouldRemoveItsFilesFromIndex()
    throws IOException, InterruptedException {
        Path watchedDirectory =
                Files.createDirectory(tempDir.resolve("watched"));

        Path nestedDirectory =
                Files.createDirectory(watchedDirectory.resolve("nested"));


        Path files1 = Files.write(
                nestedDirectory.resolve("file1.md"),
                new byte[10]
        );

        Path files2 = Files.write(
                nestedDirectory.resolve("file2.md"),
                new byte[20]
        );


        try (FileIndexerService service = new FileIndexerService(
                3,
                watchedDirectory,
                3)) {

            service.start();

            FileIndexerSnapshot snapshotMap = service.snapshot();

            assertEquals(2, snapshotMap.countFiles());
            assertEquals(30, snapshotMap.totalBytes());
            assertEquals(0, snapshotMap.errorCount());
            assertEquals(
                    Set.of(files1, files2),
                    snapshotMap.files().keySet()
            );

            Files.delete(files1);
            Files.delete(files2);
            Files.delete(nestedDirectory);

            FileIndexerSnapshot newSnapshotMap = service.snapshot();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);

            while (System.nanoTime() < deadline
                    && !(newSnapshotMap.files().isEmpty()
                    && newSnapshotMap.countFiles() == 0
                    && newSnapshotMap.totalBytes() == 0)){
                Thread.sleep(20);
                newSnapshotMap = service.snapshot();
            }

            assertTrue(newSnapshotMap.files().isEmpty());

            assertEquals(0, newSnapshotMap.countFiles());
            assertEquals(0, newSnapshotMap.totalBytes());
            assertEquals(0, newSnapshotMap.errorCount());

        }
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
                        new FileIndex(),
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
