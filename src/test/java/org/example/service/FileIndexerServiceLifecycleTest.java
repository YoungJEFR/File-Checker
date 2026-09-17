package org.example.service;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileIndexerServiceLifecycleTest {

    private static final long TIMEOUT_SECONDS = 5;

    @TempDir
    Path tempDir;

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

    private static Thread threadField(
            FileIndexerService service,
            String fieldName
    ) throws ReflectiveOperationException {
        Field field = FileIndexerService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Thread) field.get(service);
    }
}
