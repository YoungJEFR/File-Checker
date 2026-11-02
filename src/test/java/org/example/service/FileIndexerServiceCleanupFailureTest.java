package org.example.service;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class FileIndexerServiceCleanupFailureTest {
    @TempDir
    Path tempDir;

    @Test
    void cleanupFailuresShouldNotSkipLaterStagesAndShouldPreserveTheirOrder() throws Exception {
        FileIndexerService service = new FileIndexerService(2, tempDir, 3);
        RuntimeException first = new IllegalStateException("Reconciliation cleanup failed");
        RuntimeException second = new IllegalStateException("Recovery cleanup failed");
        RuntimeException third = new IllegalStateException("Debounce cleanup failed");
        List<String> calls = new CopyOnWriteArrayList<>();
        installCleanupFailures(service, first, second, third, calls);

        try {
            service.start();
            assertDoesNotThrow(service::shutdown);

            assertSame(first, terminationCause(service));
            assertArrayEquals(new Throwable[]{second, third}, first.getSuppressed());
            assertEquals(List.of("reconciliation", "recovery", "debounce"), calls);
            assertAllResourcesStopped(service);

            // Повторная остановка не должна повторять очистку или добавлять ошибки снова.
            service.shutdown();
            assertEquals(3, calls.size());
            assertArrayEquals(new Throwable[]{second, third}, first.getSuppressed());
        } finally {
            service.shutdown();
        }
    }

    @Test
    void workerFailureShouldRemainPrimaryWhenCleanupAlsoFails() throws Exception {
        RuntimeException workerFailure = new IllegalStateException("Worker failed");
        RuntimeException cleanupFailure = new IllegalStateException("Recovery cleanup failed");
        FileIndexerService service = new FileIndexerService(2, tempDir, 3, 10, task -> {
            throw workerFailure;
        });
        installCleanupFailures(service, null, cleanupFailure, null, new CopyOnWriteArrayList<>());

        try {
            service.start();
            TaskRouter router = (TaskRouter) field(service, "taskRouter");
            router.route(new FileTask(tempDir.resolve("problem.md"), ChangeType.CREATED));

            assertSame(workerFailure, terminationCause(service));
            assertArrayEquals(new Throwable[]{cleanupFailure}, workerFailure.getSuppressed());
            assertAllResourcesStopped(service);
        } finally {
            service.shutdown();
        }
    }

    @Test
    void sameExceptionFromTwoCleanupStagesShouldNotCauseSelfSuppression() throws Exception {
        FileIndexerService service = new FileIndexerService(2, tempDir, 3);
        RuntimeException sharedFailure = new IllegalStateException("Shared cleanup failure");
        List<String> calls = new CopyOnWriteArrayList<>();
        installCleanupFailures(service, null, sharedFailure, sharedFailure, calls);

        try {
            service.start();
            assertDoesNotThrow(service::shutdown);

            assertSame(sharedFailure, terminationCause(service));
            assertEquals(0, sharedFailure.getSuppressed().length);
            assertEquals(List.of("recovery", "debounce"), calls);
            assertAllResourcesStopped(service);
        } finally {
            service.shutdown();
        }
    }

    @Test
    void cleanupFailureShouldNotPreventRestoringInterruptFlag() throws Exception {
        FileIndexerService service = new FileIndexerService(2, tempDir, 3);
        RuntimeException expected = new IllegalStateException("Debounce cleanup failed");
        installCleanupFailures(service, null, null, expected, new CopyOnWriteArrayList<>());
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        Thread stopper = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                service.shutdown();
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        }, "InterruptedCleanupFailureTest");

        try {
            service.start();
            stopper.start();
            stopper.join(5_000);
            assertFalse(stopper.isAlive(), "Остановка не завершилась");
            assertNull(shutdownFailure.get());
            assertTrue(interruptRestored.get(), "Потерян interrupt-флаг вызывающего потока");
            assertSame(expected, terminationCause(service));
            assertAllResourcesStopped(service);
        } finally {
            service.shutdown();
            stopper.interrupt();
            stopper.join(2_000);
        }
    }

    private static Throwable terminationCause(FileIndexerService service) {
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> service.getTermination().get(5, TimeUnit.SECONDS));
        return failure.getCause();
    }

    private static void assertAllResourcesStopped(FileIndexerService service) throws Exception {
        Thread supervisor = (Thread) field(service, "watcherSupervisorThread");
        supervisor.join(5_000);
        assertEquals(FileIndexerState.STOPPED, field(service, "indexerState"));
        List<Thread> threads = new ArrayList<>(List.of((Thread[]) field(service, "workers")));
        for (String name : List.of("watcherThread", "producerThread", "watcherSupervisorThread")) {
            threads.add((Thread) field(service, name));
        }
        for (Thread thread : threads) {
            assertFalse(thread.isAlive(), thread.getName() + " остался работать");
        }
        for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
            assertTrue(((ExecutorService) field(service, name)).isTerminated(),
                    name + " остался работать");
        }
    }

    private static void installCleanupFailures(
            FileIndexerService service, RuntimeException reconciliationFailure,
            RuntimeException recoveryFailure, RuntimeException debounceFailure, List<String> calls
    ) throws Exception {
        TaskRouter router = (TaskRouter) field(service, "taskRouter");
        // Подменяем только точки остановки. Каждый дубль сначала закрывает настоящую службу,
        // затем выбрасывает заданную ошибку; остальные ресурсы должен закрыть сам shutdown().
        if (reconciliationFailure != null) {
            DirectoryReconciliationService original =
                    (DirectoryReconciliationService) field(service, "reconciliationService");
            setField(service, "reconciliationService", new DirectoryReconciliationService(
                    (ExecutorService) field(service, "rescanExecutor"),
                    new DirectoryReconciler(new FileScanner(), new FileIndex(), router)
            ) {
                @Override
                public void shutdown() {
                    original.shutdown();
                    calls.add("reconciliation");
                    throw reconciliationFailure;
                }
            });
        }
        if (recoveryFailure != null) {
            FileRecoveryCoordinator original =
                    (FileRecoveryCoordinator) field(service, "fileRecoveryCoordinator");
            setField(service, "fileRecoveryCoordinator", new FileRecoveryCoordinator(
                    (ScheduledExecutorService) field(service, "recoveryExecutor"),
                    router, 3, (task, cause) -> { }
            ) {
                @Override
                public void shutdown() {
                    original.shutdown();
                    calls.add("recovery");
                    throw recoveryFailure;
                }
            });
        }
        if (debounceFailure != null) {
            FileChangeDebounce original = (FileChangeDebounce) field(service, "debounce");
            setField(service, "debounce", new FileChangeDebounce(
                    (ScheduledExecutorService) field(service, "debounceExecutor"), router
            ) {
                @Override
                public void shutdown() {
                    original.shutdown();
                    calls.add("debounce");
                    throw debounceFailure;
                }
            });
        }
    }

    private static Object field(FileIndexerService service, String name) throws Exception {
        Field field = FileIndexerService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(service);
    }

    private static void setField(FileIndexerService service, String name, Object value) throws Exception {
        Field field = FileIndexerService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(service, value);
    }
}
