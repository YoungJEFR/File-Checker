package example;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.model.StopTask;
import org.example.model.WorkerTask;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IndexResilienceIntegrationTest {

    private static final int STRESS_FILE_COUNT = 200;
    private static final int EVENTS_PER_FILE = 4;
    private static final int STRESS_WORKER_COUNT = 4;

    @TempDir
    Path tempDir;

    @Test
    void reconciliationShouldMakeIndexAndStatisticsMatchDisk()
            throws Exception {
        Path changedFile = Files.write(
                tempDir.resolve("changed.md"),
                new byte[40]
        );
        Path diskOnlyFile = Files.write(
                tempDir.resolve("disk-only.md"),
                new byte[25]
        );
        Path indexOnlyFile = tempDir.resolve("index-only.md");

        ConcurrentHashMap<Path, FileInfo> indexMap =
                new ConcurrentHashMap<>();
        FileIndex fileIndex = new FileIndex(indexMap);
        fileIndex.addToMap(fileInfo(changedFile, 10));
        fileIndex.addToMap(fileInfo(indexOnlyFile, 30));
        FilesStat filesStat = filesStat(2, 40);

        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(20);
        TaskRouter router = new TaskRouter(List.of(queue));
        ScheduledExecutorService recoveryScheduler =
                Executors.newSingleThreadScheduledExecutor();
        FileRecoveryCoordinator recoveryCoordinator =
                recoveryCoordinator(recoveryScheduler, router);
        Thread worker = new Thread(
                new FileWorker(
                        queue,
                        filesStat,
                        fileIndex,
                        recoveryCoordinator
                ),
                "ReconciliationWorkerTest"
        );

        try {
            worker.start();

            new DirectoryReconciler(
                    new FileScanner(),
                    fileIndex,
                    router
            ).reconcile(tempDir);

            queue.put(stopTask());
            worker.join(5_000);

            Set<Path> diskPaths = scanMarkdownPaths(tempDir);
            long diskBytes = totalBytes(diskPaths);

            assertFalse(worker.isAlive(), "Worker не завершил reconciliation");
            assertEquals(diskPaths, fileIndex.snapshotPaths());
            assertEquals(40, fileIndex.getFileInfo(changedFile).fileSize());
            assertEquals(25, fileIndex.getFileInfo(diskOnlyFile).fileSize());
            assertEquals(diskPaths.size(), filesStat.getCountFiles().get());
            assertEquals(diskBytes, filesStat.getCountByteFiles().sum());
            assertEquals(0, filesStat.getErrorFiles().get());
        } finally {
            worker.interrupt();
            worker.join(2_000);
            recoveryScheduler.shutdownNow();
            recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void manyRepeatedEventsShouldLeaveConsistentIndexAndStatistics()
            throws Exception {
        List<BlockingQueue<WorkerTask>> queues = new ArrayList<>();
        for (int i = 0; i < STRESS_WORKER_COUNT; i++) {
            queues.add(new ArrayBlockingQueue<>(32));
        }

        ConcurrentHashMap<Path, FileInfo> indexMap =
                new ConcurrentHashMap<>();
        FileIndex fileIndex = new FileIndex(indexMap);
        FilesStat filesStat = filesStat(0, 0);
        TaskRouter router = new TaskRouter(queues);
        ScheduledExecutorService recoveryScheduler =
                Executors.newSingleThreadScheduledExecutor();
        FileRecoveryCoordinator recoveryCoordinator =
                recoveryCoordinator(recoveryScheduler, router);
        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < STRESS_WORKER_COUNT; i++) {
            Thread worker = new Thread(
                    new FileWorker(
                            queues.get(i),
                            filesStat,
                            fileIndex,
                            recoveryCoordinator
                    ),
                    "StressWorker-" + i
            );
            workers.add(worker);
            worker.start();
        }

        long expectedBytes = 0;

        try {
            for (int i = 0; i < STRESS_FILE_COUNT; i++) {
                int size = i % 50 + 1;
                Path file = Files.write(
                        tempDir.resolve("file-" + i + ".md"),
                        new byte[size]
                );
                expectedBytes += size;

                router.route(new FileTask(file, ChangeType.CREATED));
                for (int event = 1; event < EVENTS_PER_FILE; event++) {
                    router.route(new FileTask(file, ChangeType.MODIFIED));
                }
            }

            for (BlockingQueue<WorkerTask> queue : queues) {
                queue.put(stopTask());
            }

            for (Thread worker : workers) {
                worker.join(10_000);
                assertFalse(
                        worker.isAlive(),
                        "Worker не завершился после большого количества событий"
                );
            }

            assertEquals(STRESS_FILE_COUNT, indexMap.size());
            assertEquals(
                    STRESS_FILE_COUNT,
                    filesStat.getCountFiles().get()
            );
            assertEquals(expectedBytes, filesStat.getCountByteFiles().sum());
            assertEquals(0, filesStat.getErrorFiles().get());
        } finally {
            for (Thread worker : workers) {
                worker.interrupt();
                worker.join(2_000);
            }
            recoveryScheduler.shutdownNow();
            recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private FileRecoveryCoordinator recoveryCoordinator(
            ScheduledExecutorService scheduler,
            TaskRouter router
    ) {
        return new FileRecoveryCoordinator(
                scheduler,
                router,
                3,
                (failedTask, cause) -> {
                }
        );
    }

    private FilesStat filesStat(int fileCount, long byteCount) {
        LongAdder bytes = new LongAdder();
        bytes.add(byteCount);
        return new FilesStat(
                new AtomicInteger(fileCount),
                new AtomicInteger(),
                bytes
        );
    }

    private FileInfo fileInfo(Path path, long size) {
        return new FileInfo(
                FileTime.fromMillis(0),
                path.getFileName().toString(),
                size,
                path
        );
    }

    private StopTask stopTask() {
        return new StopTask();
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
}
