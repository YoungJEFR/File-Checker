package example;

import org.example.filescanner.FileScanner;
import org.example.model.BarrierTask;
import org.example.model.ChangeType;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.model.StopTask;
import org.example.model.WorkerTask;
import org.example.pipeline.FileProducer;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.example.watcher.FileWatcher;
import org.example.watcher.WatchRegistrar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InitialScanConcurrencyTest {

    private static final long TIMEOUT_SECONDS = 5;

    @TempDir
    Path tempDir;

    @Test
    void changesDuringInitialScanShouldConvergeAfterBarrier()
            throws Exception {
        Path modifiedFile = Files.write(
                tempDir.resolve("modified.md"),
                new byte[10]
        );
        Path deletedFile = Files.write(
                tempDir.resolve("deleted.md"),
                new byte[15]
        );
        Path createdFile = tempDir.resolve("created.md");

        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(100);
        CountDownLatch watchedChanges = new CountDownLatch(3);
        TaskRouter router = new RecordingTaskRouter(
                List.of(queue),
                Map.of(
                        createdFile, ChangeType.CREATED,
                        modifiedFile, ChangeType.MODIFIED,
                        deletedFile, ChangeType.DELETED
                ),
                watchedChanges
        );
        ConcurrentHashMap<Path, FileInfo> indexMap =
                new ConcurrentHashMap<>();
        FileIndex fileIndex = new FileIndex(indexMap);
        FilesStat filesStat = new FilesStat(
                new AtomicInteger(),
                new AtomicInteger(),
                new LongAdder()
        );
        ScheduledExecutorService recoveryExecutor =
                Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService debounceExecutor =
                Executors.newSingleThreadScheduledExecutor();
        ExecutorService reconciliationExecutor =
                Executors.newSingleThreadExecutor();
        FileRecoveryCoordinator recoveryCoordinator =
                new FileRecoveryCoordinator(
                        recoveryExecutor,
                        router,
                        3,
                        (failedTask, cause) -> { }
                );
        Thread workerThread = new Thread(
                new FileWorker(
                        queue,
                        filesStat,
                        fileIndex,
                        recoveryCoordinator
                ),
                "InitialScanWorkerTest"
        );
        CountDownLatch watcherReady = new CountDownLatch(1);
        FileWatcher watcher = new FileWatcher(
                tempDir,
                new FileChangeDebounce(debounceExecutor, router),
                router,
                new WatchRegistrar(new ConcurrentHashMap<>()),
                new DirectoryReconciliationService(
                        reconciliationExecutor,
                        new DirectoryReconciler(
                                new FileScanner(),
                                fileIndex,
                                router
                        )
                ),
                watcherReady
        );
        Thread watcherThread = new Thread(
                watcher,
                "InitialScanWatcherTest"
        );
        CountDownLatch scanStarted = new CountDownLatch(1);
        CountDownLatch allowScan = new CountDownLatch(1);
        Thread producerThread = new Thread(
                new FileProducer(
                        tempDir,
                        router,
                        new PausingFileScanner(scanStarted, allowScan)
                ),
                "InitialScanProducerTest"
        );

        try {
            workerThread.start();
            watcherThread.start();
            assertTrue(
                    watcherReady.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Watcher не сообщил о готовности"
            );

            producerThread.start();
            assertTrue(
                    scanStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Initial scan не достиг контрольной точки"
            );

            Files.write(createdFile, new byte[25]);
            Files.write(modifiedFile, new byte[40]);
            Files.delete(deletedFile);

            assertTrue(
                    watchedChanges.await(
                            TIMEOUT_SECONDS,
                            TimeUnit.SECONDS
                    ),
                    "Watcher не передал все изменения во время initial scan"
            );

            allowScan.countDown();
            producerThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse(
                    producerThread.isAlive(),
                    "Initial scan не завершился"
            );

            CountDownLatch barrierReached = new CountDownLatch(1);
            queue.put(new BarrierTask(barrierReached));
            assertTrue(
                    barrierReached.await(
                            TIMEOUT_SECONDS,
                            TimeUnit.SECONDS
                    ),
                    "Worker не достиг барьера initial scan"
            );

            Set<Path> diskPaths = scanMarkdownPaths(tempDir);
            assertEquals(diskPaths, fileIndex.snapshotPaths());
            assertEquals(diskPaths.size(), filesStat.getCountFiles().get());
            assertEquals(totalBytes(diskPaths), filesStat.getCountByteFiles().sum());
        } finally {
            allowScan.countDown();
            producerThread.interrupt();
            producerThread.join(2_000);

            watcherThread.interrupt();
            watcherThread.join(2_000);
            queue.offer(new StopTask());
            workerThread.interrupt();
            workerThread.join(2_000);

            reconciliationExecutor.shutdownNow();
            reconciliationExecutor.awaitTermination(2, TimeUnit.SECONDS);
            debounceExecutor.shutdownNow();
            debounceExecutor.awaitTermination(2, TimeUnit.SECONDS);
            recoveryExecutor.shutdownNow();
            recoveryExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private Set<Path> scanMarkdownPaths(Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".md"))
                    .collect(Collectors.toSet());
        }
    }

    private long totalBytes(Set<Path> paths) throws IOException {
        long result = 0;
        for (Path path : paths) {
            result += Files.size(path);
        }
        return result;
    }

    private static final class PausingFileScanner extends FileScanner {

        private final CountDownLatch scanStarted;
        private final CountDownLatch allowScan;

        private PausingFileScanner(
                CountDownLatch scanStarted,
                CountDownLatch allowScan
        ) {
            this.scanStarted = scanStarted;
            this.allowScan = allowScan;
        }

        @Override
        public void scanFile(Path root, FileHandler handler)
                throws InterruptedException, IOException {
            scanStarted.countDown();
            allowScan.await();
            super.scanFile(root, handler);
        }
    }

    private static final class RecordingTaskRouter extends TaskRouter {

        private final Map<Path, ChangeType> expectedChanges;
        private final Set<Path> observedPaths = ConcurrentHashMap.newKeySet();
        private final CountDownLatch watchedChanges;

        private RecordingTaskRouter(
                List<BlockingQueue<WorkerTask>> queues,
                Map<Path, ChangeType> expectedChanges,
                CountDownLatch watchedChanges
        ) {
            super(queues);
            this.expectedChanges = expectedChanges;
            this.watchedChanges = watchedChanges;
        }

        @Override
        public void route(FileTask fileTask) throws InterruptedException {
            super.route(fileTask);

            if (expectedChanges.get(fileTask.path()) == fileTask.changeType()
                    && observedPaths.add(fileTask.path())) {
                watchedChanges.countDown();
            }
        }
    }
}
