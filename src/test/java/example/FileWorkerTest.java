package example;

import org.example.model.ChangeType;
import org.example.model.BarrierTask;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.model.StopTask;
import org.example.model.WorkerTask;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWorkerTest {

    @TempDir
    Path tempDir;

    private ConcurrentHashMap<Path, FileInfo> indexMap;
    private FilesStat filesStat;
    private FileIndex fileIndex;

    @BeforeEach
    void setUp() {
        indexMap = new ConcurrentHashMap<>();
        filesStat = new FilesStat(
                new AtomicInteger(),
                new AtomicInteger(),
                new LongAdder()
        );
        fileIndex = new FileIndex(indexMap);
    }

    @Test
    void repeatedCreateShouldNotDuplicateFileOrBytes() throws Exception {
        Path file = tempDir.resolve("a.md");
        Files.write(file, new byte[100]);

        processTasks(
                new FileTask(file, ChangeType.CREATED),
                new FileTask(file, ChangeType.CREATED)
        );

        assertEquals(1, indexMap.size());
        assertEquals(1, filesStat.getCountFiles().get());
        assertEquals(100, filesStat.getCountByteFiles().sum());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void repeatedCreateAfterSizeChangeShouldApplyDifferenceOnlyOnce()
            throws Exception {
        Path file = tempDir.resolve("a.md");
        Files.write(file, new byte[100]);

        processTasks(
                new FileTask(file, ChangeType.CREATED)
        );

        Files.write(file, new byte[150]);

        processTasks(
                new FileTask(file, ChangeType.CREATED),
                new FileTask(file, ChangeType.CREATED)
        );

        assertEquals(1, indexMap.size());
        assertEquals(1, filesStat.getCountFiles().get());
        assertEquals(150, filesStat.getCountByteFiles().sum());
        assertEquals(150, indexMap.get(file).fileSize());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void repeatedDeleteShouldNotMakeStatisticsNegative() throws Exception {
        Path file = tempDir.resolve("a.md");
        Files.write(file, new byte[100]);

        processTasks(
                new FileTask(file, ChangeType.CREATED)
        );

        Files.delete(file);

        processTasks(
                new FileTask(file, ChangeType.DELETED),
                new FileTask(file, ChangeType.DELETED)
        );

        assertEquals(0, indexMap.size());
        assertEquals(0, filesStat.getCountFiles().get());
        assertEquals(0, filesStat.getCountByteFiles().sum());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void deleteOfUnknownFileShouldNotStopWorker() throws Exception {
        Path unknownFile = tempDir.resolve("unknown.md");
        Path nextFile = tempDir.resolve("next.md");
        Files.write(nextFile, new byte[50]);

        processTasks(
                new FileTask(unknownFile, ChangeType.DELETED),
                new FileTask(nextFile, ChangeType.CREATED)
        );

        assertEquals(1, indexMap.size());
        assertEquals(50, indexMap.get(nextFile).fileSize());
        assertEquals(1, filesStat.getCountFiles().get());
        assertEquals(50, filesStat.getCountByteFiles().sum());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void modifyOfUnknownExistingFileShouldAddItToIndex() throws Exception {
        Path file = tempDir.resolve("unknown.md");
        Files.write(file, new byte[80]);

        processTasks(
                new FileTask(file, ChangeType.MODIFIED)
        );

        assertEquals(1, indexMap.size());
        assertEquals(80, indexMap.get(file).fileSize());
        assertEquals(1, filesStat.getCountFiles().get());
        assertEquals(80, filesStat.getCountByteFiles().sum());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void modifyOfMissingFileShouldBeTreatedAsDeletionAndNotStopWorker()
            throws Exception {
        Path missingFile = tempDir.resolve("missing.md");
        Path nextFile = tempDir.resolve("next.md");
        Files.write(nextFile, new byte[50]);

        processTasks(
                new FileTask(missingFile, ChangeType.MODIFIED),
                new FileTask(nextFile, ChangeType.CREATED)
        );

        assertEquals(1, indexMap.size());
        assertEquals(50, indexMap.get(nextFile).fileSize());
        assertEquals(1, filesStat.getCountFiles().get());
        assertEquals(50, filesStat.getCountByteFiles().sum());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void missingModifiedFileShouldRemoveStaleIndexAndStatistics()
            throws Exception {
        Path file = tempDir.resolve("a.md");
        Files.write(file, new byte[100]);

        processTasks(new FileTask(file, ChangeType.CREATED));

        Files.delete(file);

        processTasks(new FileTask(file, ChangeType.MODIFIED));

        assertEquals(0, indexMap.size());
        assertEquals(0, filesStat.getCountFiles().get());
        assertEquals(0, filesStat.getCountByteFiles().sum());
        assertEquals(0, filesStat.getErrorFiles().get());
    }

    @Test
    void barrierShouldConfirmEarlierTasksAndWorkerShouldContinue()
            throws Exception {
        Path firstFile = Files.write(
                tempDir.resolve("first.md"),
                new byte[10]
        );
        Path secondFile = Files.write(
                tempDir.resolve("second.md"),
                new byte[20]
        );
        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(100);
        ScheduledExecutorService recoveryScheduler =
                Executors.newSingleThreadScheduledExecutor();
        TaskRouter router = new TaskRouter(List.of(queue));
        FileRecoveryCoordinator recoveryCoordinator =
                new FileRecoveryCoordinator(
                        recoveryScheduler,
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
                "BarrierWorkerTest"
        );
        CountDownLatch barrierReached = new CountDownLatch(1);

        try {
            workerThread.start();
            queue.put(new FileTask(firstFile, ChangeType.CREATED));
            queue.put(new BarrierTask(barrierReached));

            assertTrue(
                    barrierReached.await(2, TimeUnit.SECONDS),
                    "Worker не достиг BarrierTask"
            );
            assertEquals(10, indexMap.get(firstFile).fileSize());
            assertTrue(
                    workerThread.isAlive(),
                    "BarrierTask ошибочно завершил worker"
            );

            queue.put(new FileTask(secondFile, ChangeType.CREATED));
            queue.put(new StopTask());
            workerThread.join(2_000);

            assertFalse(workerThread.isAlive());
            assertEquals(20, indexMap.get(secondFile).fileSize());
            assertEquals(2, filesStat.getCountFiles().get());
            assertEquals(30, filesStat.getCountByteFiles().sum());
        } finally {
            workerThread.interrupt();
            workerThread.join(2_000);
            recoveryScheduler.shutdownNow();
            recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private void processTasks(FileTask... tasks) throws InterruptedException {
        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(100);
        ScheduledExecutorService recoveryScheduler =
                Executors.newSingleThreadScheduledExecutor();
        TaskRouter router = new TaskRouter(List.of(queue));
        FileRecoveryCoordinator recoveryCoordinator =
                new FileRecoveryCoordinator(
                        recoveryScheduler,
                        router,
                        3,
                        (failedTask, cause) -> { }
                );

        for (FileTask task : tasks) {
            queue.put(task);
        }

        queue.put(new StopTask());

        Thread workerThread = new Thread(
                new FileWorker(
                        queue,
                        filesStat,
                        fileIndex,
                        recoveryCoordinator
                ),
                "FileWorkerTest"
        );

        try {
            workerThread.start();
            workerThread.join(2_000);

            assertFalse(
                    workerThread.isAlive(),
                    "Worker не завершился после STOP-задачи"
            );
        } finally {
            recoveryScheduler.shutdownNow();
            recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
    }
}
