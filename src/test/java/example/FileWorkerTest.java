package example;

import org.example.model.*;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.processor.FileTaskProcessor;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;

class FileWorkerTest {

    @TempDir
    Path tempDir;

    private FilesStat filesStat;
    private FileIndex fileIndex;
    private final Object indexStateLock = new Object();

    @BeforeEach
    void setUp() {
        filesStat = new FilesStat();
        fileIndex = new FileIndex();
    }

    @Test
    @Timeout(10)
    void replacingIndexedFileWithDirectoryShouldRemoveEntryWithoutRecovery()
            throws Exception {
        Path file = Files.write(tempDir.resolve("notes.md"), new byte[10]);
        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(10);
        CountDownLatch initialProcessed = new CountDownLatch(1);
        CountDownLatch modifyEntered = new CountDownLatch(1);
        CountDownLatch allowModify = new CountDownLatch(1);
        CountDownLatch modificationProcessed = new CountDownLatch(1);
        AtomicInteger failureCalls = new AtomicInteger();
        AtomicInteger successCalls = new AtomicInteger();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        TaskRouter router = new TaskRouter(List.of(queue));
        FileRecoveryCoordinator recovery = new FileRecoveryCoordinator(
                scheduler, router, 3, (task, cause) -> { }
        ) {
            @Override
            public boolean onFailure(FileTask task, IOException cause) {
                failureCalls.incrementAndGet();
                return super.onFailure(task, cause);
            }

            @Override
            public void onSuccess(Path path) {
                successCalls.incrementAndGet();
                super.onSuccess(path);
            }
        };
        FileTaskProcessor processor = task -> {
            if (task.changeType() == ChangeType.MODIFIED) {
                modifyEntered.countDown();
                allowModify.await();
            }
            return FileProcessor.process(task);
        };
        Thread worker = new Thread(new FileWorker(
                queue, filesStat, fileIndex, recovery, processor, indexStateLock
        ), "FileReplacedWithDirectoryTest");
        worker.setUncaughtExceptionHandler((thread, cause) -> workerFailure.set(cause));

        try {
            worker.start();
            queue.put(new FileTask(file, ChangeType.CREATED));
            queue.put(new BarrierTask(initialProcessed));
            assertTrue(initialProcessed.await(2, TimeUnit.SECONDS),
                    "Worker не обработал исходный файл");
            assertEquals(10, fileIndex.getFileInfo(file).fileSize());
            assertEquals(1, filesStat.countFiles());
            assertEquals(10, filesStat.totalBytes());
            assertEquals(0, filesStat.countError());

            queue.put(new FileTask(file, ChangeType.MODIFIED));
            queue.put(new BarrierTask(modificationProcessed));
            assertTrue(modifyEntered.await(2, TimeUnit.SECONDS),
                    "Worker не вошёл в обработку MODIFY");

            // Меняем тип пути после получения задачи, но до чтения атрибутов.
            Files.delete(file);
            Files.createDirectory(file);
            assertTrue(Files.isDirectory(file));
            allowModify.countDown();
            assertTrue(modificationProcessed.await(2, TimeUnit.SECONDS),
                    "Worker не обработал замену файла папкой");

            assertNull(fileIndex.getFileInfo(file));
            assertTrue(fileIndex.snapshotMap().isEmpty());
            assertEquals(0, filesStat.countFiles());
            assertEquals(0, filesStat.totalBytes());
            assertEquals(0, filesStat.countError());
            assertEquals(0, failureCalls.get(), "Recovery не должен запускаться для папки");
            assertEquals(2, successCalls.get(),
                    "CREATE и удаление неподходящей записи должны закрывать recovery");

            queue.put(new StopTask());
            worker.join(2_000);
            assertFalse(worker.isAlive(), "Worker не завершился после StopTask");
            assertNull(workerFailure.get(), "Worker завершился с необработанной ошибкой");
        } finally {
            allowModify.countDown();
            worker.interrupt();
            worker.join(2_000);
            recovery.shutdown();
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void injectedProcessorShouldBlockAndResumeWorker()
            throws Exception {
        Path file = Files.write(
                tempDir.resolve("file.md"),
                new byte[10]
        );
        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(5);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch allowWorker = new CountDownLatch(1);
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
        FileTaskProcessor processor = fileTask -> {
            workerEntered.countDown();
            allowWorker.await();
            return FileProcessor.process(fileTask);
        };

        FileWorker fileWorker = new FileWorker(
                queue,
                filesStat,
                fileIndex,
                recoveryCoordinator,
                processor,
                indexStateLock
        );
        Thread workerThread = new Thread(
                fileWorker,
                "ControlledFileWorkerTest"
        );

        try {
            queue.put(new FileTask(file, ChangeType.CREATED));
            workerThread.start();

            assertTrue(
                    workerEntered.await(2, TimeUnit.SECONDS),
                    "Worker не вошёл в тестовый processor"
            );
            assertTrue(
                    workerThread.isAlive(),
                    "Worker завершился до разрешения обработки"
            );
            assertNull(
                    fileIndex.getFileInfo(file),
                    "Файл попал в индекс до освобождения processor"
            );

            queue.put(new StopTask());
            allowWorker.countDown();
            workerThread.join(2_000);

            assertFalse(
                    workerThread.isAlive(),
                    "Worker не завершился после StopTask"
            );
            assertNotNull(fileIndex.getFileInfo(file));
            assertEquals(1, filesStat.countFiles());
            assertEquals(10, filesStat.totalBytes());
        } finally {
            allowWorker.countDown();
            queue.offer(new StopTask());
            workerThread.interrupt();
            workerThread.join(2_000);

            recoveryScheduler.shutdownNow();
            recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void fullQueueShouldProcessFileTasksBeforeStopTask()
            throws Exception {
        Path firstFile = Files.write(
                tempDir.resolve("first.md"),
                new byte[10]
        );
        Path secondFile = Files.write(
                tempDir.resolve("second.md"),
                new byte[20]
        );
        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(1);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch allowWorker = new CountDownLatch(1);
        CountDownLatch stopPutStarted = new CountDownLatch(1);
        CountDownLatch stopPutCompleted = new CountDownLatch(1);
        AtomicReference<Throwable> stopFailure = new AtomicReference<>();
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
        FileTaskProcessor processor = fileTask -> {
            workerEntered.countDown();
            allowWorker.await();
            return FileProcessor.process(fileTask);
        };
        Thread workerThread = new Thread(
                new FileWorker(
                        queue,
                        filesStat,
                        fileIndex,
                        recoveryCoordinator,
                        processor,
                        indexStateLock
                ),
                "FullQueueFileWorkerTest"
        );
        Thread stopSender = new Thread(() -> {
            stopPutStarted.countDown();
            try {
                queue.put(new StopTask());
                stopPutCompleted.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopFailure.set(e);
            } catch (Throwable failure) {
                stopFailure.set(failure);
            }
        }, "FullQueueStopSenderTest");

        try {
            queue.put(new FileTask(firstFile, ChangeType.CREATED));
            workerThread.start();

            assertTrue(
                    workerEntered.await(2, TimeUnit.SECONDS),
                    "Worker не начал обработку первого файла"
            );

            queue.put(new FileTask(secondFile, ChangeType.CREATED));
            assertEquals(
                    0,
                    queue.remainingCapacity(),
                    "Очередь не была полностью заполнена"
            );

            stopSender.start();
            assertTrue(
                    stopPutStarted.await(2, TimeUnit.SECONDS),
                    "Поток не начал отправку StopTask"
            );
            assertEquals(
                    1,
                    stopPutCompleted.getCount(),
                    "StopTask попал в заполненную очередь"
            );
            assertTrue(
                    stopSender.isAlive(),
                    "Отправитель StopTask не ожидал свободного места"
            );

            allowWorker.countDown();

            assertTrue(
                    stopPutCompleted.await(2, TimeUnit.SECONDS),
                    "StopTask не был добавлен после освобождения места"
            );
            stopSender.join(2_000);
            workerThread.join(2_000);

            assertNull(
                    stopFailure.get(),
                    "Отправка StopTask завершилась с ошибкой"
            );
            assertFalse(
                    stopSender.isAlive(),
                    "Поток отправки StopTask не завершился"
            );
            assertFalse(
                    workerThread.isAlive(),
                    "Worker не завершился после StopTask"
            );
            assertNotNull(fileIndex.getFileInfo(firstFile));
            assertNotNull(fileIndex.getFileInfo(secondFile));
            assertEquals(2, filesStat.countFiles());
            assertEquals(30, filesStat.totalBytes());
            assertTrue(queue.isEmpty());
        } finally {
            allowWorker.countDown();
            queue.offer(new StopTask());

            stopSender.interrupt();
            workerThread.interrupt();
            stopSender.join(2_000);
            workerThread.join(2_000);

            recoveryScheduler.shutdownNow();
            recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void repeatedCreateShouldNotDuplicateFileOrBytes() throws Exception {
        Path file = tempDir.resolve("a.md");
        Files.write(file, new byte[100]);

        processTasks(
                new FileTask(file, ChangeType.CREATED),
                new FileTask(file, ChangeType.CREATED)
        );

        assertEquals(1, fileIndex.snapshotMap().size());
        assertEquals(1, filesStat.countFiles());
        assertEquals(100, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
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

        assertEquals(1, fileIndex.snapshotMap().size());
        assertEquals(1, filesStat.countFiles());
        assertEquals(150, filesStat.totalBytes());
        assertEquals(150, fileIndex.getFileInfo(file).fileSize());
        assertEquals(0, filesStat.countError());
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

        assertEquals(0, fileIndex.snapshotMap().size());
        assertEquals(0, filesStat.countFiles());
        assertEquals(0, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
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

        assertEquals(1, fileIndex.snapshotMap().size());
        assertEquals(50, fileIndex.getFileInfo(nextFile).fileSize());
        assertEquals(1, filesStat.countFiles());
        assertEquals(50, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
    }

    @Test
    void modifyOfUnknownExistingFileShouldAddItToIndex() throws Exception {
        Path file = tempDir.resolve("unknown.md");
        Files.write(file, new byte[80]);

        processTasks(
                new FileTask(file, ChangeType.MODIFIED)
        );

        assertEquals(1, fileIndex.snapshotMap().size());
        assertEquals(80, fileIndex.getFileInfo(file).fileSize());
        assertEquals(1, filesStat.countFiles());
        assertEquals(80, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
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

        assertEquals(1, fileIndex.snapshotMap().size());
        assertEquals(50, fileIndex.getFileInfo(nextFile).fileSize());
        assertEquals(1, filesStat.countFiles());
        assertEquals(50, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
    }

    @Test
    void missingModifiedFileShouldRemoveStaleIndexAndStatistics()
            throws Exception {
        Path file = tempDir.resolve("a.md");
        Files.write(file, new byte[100]);

        processTasks(new FileTask(file, ChangeType.CREATED));

        Files.delete(file);

        processTasks(new FileTask(file, ChangeType.MODIFIED));

        assertEquals(0, fileIndex.snapshotMap().size());
        assertEquals(0, filesStat.countFiles());
        assertEquals(0, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
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
                        (failedTask, cause) -> {
                        }
                );
        Thread workerThread = new Thread(
                new FileWorker(
                        queue,
                        filesStat,
                        fileIndex,
                        recoveryCoordinator,
                        indexStateLock
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
            assertEquals(10, fileIndex.getFileInfo(firstFile).fileSize());
            assertTrue(
                    workerThread.isAlive(),
                    "BarrierTask ошибочно завершил worker"
            );

            queue.put(new FileTask(secondFile, ChangeType.CREATED));
            queue.put(new StopTask());
            workerThread.join(2_000);

            assertFalse(workerThread.isAlive());
            assertEquals(20, fileIndex.getFileInfo(secondFile).fileSize());
            assertEquals(2, filesStat.countFiles());
            assertEquals(30, filesStat.totalBytes());
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
                        (failedTask, cause) -> {
                        }
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
                        recoveryCoordinator,
                        indexStateLock
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
