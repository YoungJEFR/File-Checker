package org.example.service;

import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.processor.FileProcessor;
import org.example.processor.FileTaskProcessor;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FileIndexerServiceLifecycleTest {

    private static final long TIMEOUT_SECONDS = 5;

    @TempDir
    Path tempDir;

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void tryWithResourcesShouldShutdownStartedService()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);

        try (service) {
            service.start();
            assertEquals(FileIndexerState.RUNNING, stateOf(service));
        }

        assertFullyStopped(service, ownedThreads);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void shutdownBeforeStartShouldStopAllOwnedThreads()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);

        try {
            service.shutdown();
            assertFullyStopped(service, ownedThreads);

            service.shutdown();
            assertFullyStopped(service, ownedThreads);
        } finally {
            service.shutdown();

            for (Thread ownedThread : ownedThreads) {
                ownedThread.interrupt();
                ownedThread.join(2_000);
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void secondStartShouldBeRejectedWithoutStoppingRunningService()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);

        try {
            service.start();
            assertEquals(FileIndexerState.RUNNING, stateOf(service));

            assertThrows(
                    IllegalStateException.class,
                    service::start
            );

            assertEquals(FileIndexerState.RUNNING, stateOf(service));
        } finally {
            service.shutdown();
            assertFullyStopped(service, ownedThreads);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void startAfterShutdownShouldBeRejected()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);

        try {
            service.shutdown();
            assertFullyStopped(service, ownedThreads);

            assertThrows(
                    IllegalStateException.class,
                    service::start
            );

            assertFullyStopped(service, ownedThreads);
        } finally {
            service.shutdown();
        }
    }

    @RepeatedTest(10)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void concurrentStartAndShutdownShouldTerminateAllOwnedThreads()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();

        Thread startThread = new Thread(() -> {
            ready.countDown();
            try {
                startGate.await();
                service.start();
            } catch (Throwable failure) {
                startFailure.set(failure);
            }
        }, "LifecycleStartTest");

        Thread shutdownThread = new Thread(() -> {
            ready.countDown();
            try {
                startGate.await();
                service.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        }, "LifecycleShutdownTest");

        try {
            startThread.start();
            shutdownThread.start();

            assertTrue(
                    ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Потоки start и shutdown не достигли стартовой точки"
            );
            startGate.countDown();

            startThread.join(
                    TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
            );
            shutdownThread.join(
                    TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
            );

            assertFalse(
                    startThread.isAlive(),
                    "start() завис при конкурентном shutdown()"
            );
            assertFalse(
                    shutdownThread.isAlive(),
                    "shutdown() не завершился"
            );
            assertNull(
                    shutdownFailure.get(),
                    "shutdown() завершился с ошибкой"
            );

            Throwable failure = startFailure.get();
            assertTrue(
                    failure == null || failure instanceof IllegalStateException,
                    () -> "start() завершился с неожиданной ошибкой: " + failure
            );
            assertEquals(FileIndexerState.STOPPED, stateOf(service));

            for (Thread ownedThread : ownedThreads) {
                assertFalse(
                        ownedThread.isAlive(),
                        () -> ownedThread.getName() + " остался работать"
                );
            }
        } finally {
            startGate.countDown();
            service.shutdown();

            startThread.interrupt();
            shutdownThread.interrupt();
            startThread.join(2_000);
            shutdownThread.join(2_000);

            for (Thread ownedThread : ownedThreads) {
                ownedThread.interrupt();
                ownedThread.join(2_000);
            }
        }
    }

    @RepeatedTest(10)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void concurrentShutdownCallsShouldWaitForCompleteTermination()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();

        Thread firstShutdown = new Thread(() -> {
            ready.countDown();
            try {
                startGate.await();
                service.shutdown();
                assertFullyStopped(service, ownedThreads);
            } catch (Throwable failure) {
                firstFailure.set(failure);
            }
        }, "FirstLifecycleShutdownTest");

        Thread secondShutdown = new Thread(() -> {
            ready.countDown();
            try {
                startGate.await();
                service.shutdown();
                assertFullyStopped(service, ownedThreads);
            } catch (Throwable failure) {
                secondFailure.set(failure);
            }
        }, "SecondLifecycleShutdownTest");

        try {
            service.start();
            assertEquals(FileIndexerState.RUNNING, stateOf(service));

            firstShutdown.start();
            secondShutdown.start();

            assertTrue(
                    ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Оба shutdown-потока не достигли стартовой точки"
            );
            startGate.countDown();

            firstShutdown.join(
                    TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
            );
            secondShutdown.join(
                    TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
            );

            assertFalse(
                    firstShutdown.isAlive(),
                    "Первый shutdown() не завершился"
            );
            assertFalse(
                    secondShutdown.isAlive(),
                    "Второй shutdown() не дождался завершения"
            );
            assertNull(
                    firstFailure.get(),
                    "Первый shutdown() завершился с ошибкой"
            );
            assertNull(
                    secondFailure.get(),
                    "Второй shutdown() завершился с ошибкой"
            );
            assertEquals(FileIndexerState.STOPPED, stateOf(service));

            for (Thread ownedThread : ownedThreads) {
                assertFalse(
                        ownedThread.isAlive(),
                        () -> ownedThread.getName() + " остался работать"
                );
            }
        } finally {
            startGate.countDown();
            service.shutdown();

            firstShutdown.interrupt();
            secondShutdown.interrupt();
            firstShutdown.join(2_000);
            secondShutdown.join(2_000);

            for (Thread ownedThread : ownedThreads) {
                ownedThread.interrupt();
                ownedThread.join(2_000);
            }
        }
    }

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void interruptedShutdownShouldCompleteAndRestoreInterruptFlag()
            throws Exception {
        FileIndexerService service = new FileIndexerService(
                2,
                tempDir,
                3
        );
        List<Thread> ownedThreads = ownedThreads(service);
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        AtomicBoolean interruptedAfterShutdown = new AtomicBoolean(false);

        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                service.shutdown();

                interruptedAfterShutdown.set(
                        Thread.currentThread().isInterrupted()
                );
                assertFullyStopped(service, ownedThreads);
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        }, "InterruptedLifecycleShutdownTest");

        try {
            service.start();
            assertEquals(FileIndexerState.RUNNING, stateOf(service));

            shutdownThread.start();
            shutdownThread.join(
                    TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
            );

            assertFalse(
                    shutdownThread.isAlive(),
                    "Прерванный shutdown() не завершился"
            );
            assertNull(
                    shutdownFailure.get(),
                    "Прерванный shutdown() завершился с ошибкой"
            );
            assertTrue(
                    interruptedAfterShutdown.get(),
                    "shutdown() не восстановил interruption-флаг"
            );
            assertEquals(FileIndexerState.STOPPED, stateOf(service));

            for (Thread ownedThread : ownedThreads) {
                assertFalse(
                        ownedThread.isAlive(),
                        () -> ownedThread.getName() + " остался работать"
                );
            }
        } finally {
            service.shutdown();

            shutdownThread.interrupt();
            shutdownThread.join(2_000);

            for (Thread ownedThread : ownedThreads) {
                ownedThread.interrupt();
                ownedThread.join(2_000);
            }
        }
    }

    private static void assertFullyStopped(
            FileIndexerService service,
            List<Thread> ownedThreads
    ) throws ReflectiveOperationException {
        if (stateOf(service) != FileIndexerState.STOPPED) {
            throw new AssertionError(
                    "shutdown() вернулся до перехода сервиса в STOPPED"
            );
        }

        for (Thread ownedThread : ownedThreads) {
            if (ownedThread.isAlive()) {
                throw new AssertionError(
                        ownedThread.getName()
                                + " был жив после возврата shutdown()"
                );
            }
        }
    }

    private static FileIndexerState stateOf(
            FileIndexerService service
    ) throws ReflectiveOperationException {
        Field stateField = FileIndexerService.class
                .getDeclaredField("indexerState");
        stateField.setAccessible(true);
        return (FileIndexerState) stateField.get(service);
    }

    private static List<Thread> ownedThreads(
            FileIndexerService service
    ) throws ReflectiveOperationException {
        List<Thread> result = new ArrayList<>();

        Field workersField = FileIndexerService.class
                .getDeclaredField("workers");
        workersField.setAccessible(true);
        result.addAll(Arrays.asList(
                (Thread[]) workersField.get(service)
        ));

        result.add(threadField(service, "watcherThread"));
        result.add(threadField(service, "producerThread"));
        return result;
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void shutdownShouldWaitForFullQueueAndProcessAcceptedTasks()
            throws Exception {
        Path watchedDirectory = Files.createDirectory(tempDir.resolve("watched"));
        Path tasksDirectory = Files.createDirectory(tempDir.resolve("tasks"));

        Path firstFile = Files.write(tasksDirectory.resolve("first.md"),
                new byte[10]);
        Path secondFile = Files.write(tasksDirectory.resolve("second.md"),
                new byte[20]);

        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch allowWorker = new CountDownLatch(1);

        CountDownLatch shutdownStarted = new CountDownLatch(1);
        CountDownLatch shutdownFinished = new CountDownLatch(1);
        AtomicReference<Throwable> shutdownFailure =
                new AtomicReference<>();

        FileTaskProcessor processor = fileTask -> {
            workerEntered.countDown();
            allowWorker.await();

            return FileProcessor.process(fileTask);
        };

        FileIndexerService indexerService = new FileIndexerService(
                1,
                watchedDirectory,
                3,
                1,
                processor
        );

        BlockingQueue<WorkerTask> queue =
                queuesOf(indexerService).get(0);
        List<Thread> threads = ownedThreads(indexerService);

        Thread shutdownThread = new Thread(() -> {
            shutdownStarted.countDown();

            try {
                indexerService.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            } finally {
                shutdownFinished.countDown();
            }
        }, "FullQueueShutdownTest");

        try {
            indexerService.start();
            queue.put(new FileTask(firstFile, ChangeType.CREATED));
            assertTrue(
                    workerEntered.await(2, TimeUnit.SECONDS),
                    "Worker не вошёл в обработку первого файла"
            );

            queue.put(new FileTask(secondFile, ChangeType.CREATED));


            assertEquals(0,
                    queue.remainingCapacity(),
                    "Очередь не заполнена");

            shutdownThread.start();
            assertTrue(
                    shutdownStarted.await(2, TimeUnit.SECONDS),
                    "Shutdown не начался"
            );

            assertFalse(
                    shutdownFinished.await(200, TimeUnit.MILLISECONDS),
                    "shutdown завершился до освобождения Worker"
            );

            assertTrue(
                    shutdownThread.isAlive(),
                    "shutdown-поток завершился при заполненной очереди"
            );

            allowWorker.countDown();
            assertTrue(
                    shutdownFinished.await(5, TimeUnit.SECONDS),
                    "shutdown не завершился после освобождения Worker"
            );

            shutdownThread.join(2_000);

            assertNull(
                    shutdownFailure.get(),
                    () -> "shutdown завершился с ошибкой: "
                            + shutdownFailure.get()
            );

            assertFalse(
                    shutdownThread.isAlive(),
                    "Shutdown поток жив");

            assertFullyStopped(indexerService, threads);


            var snapshot = indexerService.snapshot();

            assertTrue(snapshot.files().containsKey(firstFile));
            assertTrue(snapshot.files().containsKey(secondFile));
            assertEquals(
                    2,
                    snapshot.countFiles()
            );
            assertEquals(
                    30,
                    snapshot.totalBytes()
            );

        } finally {
            allowWorker.countDown();

            shutdownThread.join(2_000);
            if (shutdownThread.isAlive()) {
                shutdownThread.interrupt();
            }

            indexerService.shutdown();
            shutdownThread.join(2_000);

            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    thread.interrupt();
                    thread.join(2_000);
                }
            }
        }

    }

    @SuppressWarnings("unchecked")
    private static List<BlockingQueue<WorkerTask>> queuesOf(FileIndexerService service)
            throws ReflectiveOperationException {
        Field queuesField = FileIndexerService.class
                .getDeclaredField("queues");

        queuesField.setAccessible(true);

        List<BlockingQueue<WorkerTask>> queues = (List<BlockingQueue<WorkerTask>>)
                queuesField.get(service);

        return List.copyOf(queues);
    }

    private static Thread threadField(
            FileIndexerService service,
            String fieldName
    ) throws ReflectiveOperationException {
        Field field = FileIndexerService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Thread) field.get(service);
    }
}
