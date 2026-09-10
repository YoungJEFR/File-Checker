package example;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.reconciliation.DirectoryRecoveryHandler;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.recovery.RecoveryExhaustedHandler;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileRecoveryCoordinatorTest {

    @TempDir
    Path tempDir;

    private RecordingScheduler recoveryScheduler;
    private RecordingExhaustedHandler exhaustedHandler;
    private BlockingQueue<WorkerTask> queue;
    private TaskRouter router;

    @BeforeEach
    void setUp() {
        recoveryScheduler = new RecordingScheduler();
        exhaustedHandler = new RecordingExhaustedHandler();
        queue = new ArrayBlockingQueue<>(100);
        router = new TaskRouter(List.of(queue));
    }

    @AfterEach
    void stopScheduler() throws InterruptedException {
        recoveryScheduler.shutdownNow();
        recoveryScheduler.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void shouldAcceptPositiveMaxAttempts() {
        assertDoesNotThrow(() ->
                new FileRecoveryCoordinator(
                        recoveryScheduler,
                        router,
                        3,
                        exhaustedHandler
                )
        );
    }

    @Test
    void shouldRejectNonPositiveMaxAttempts() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new FileRecoveryCoordinator(
                        recoveryScheduler,
                        router,
                        0,
                        exhaustedHandler
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new FileRecoveryCoordinator(
                        recoveryScheduler,
                        router,
                        -1,
                        exhaustedHandler
                )
        );
    }

    @Test
    void repeatedNormalFailureForSamePathShouldScheduleOnlyOneRetry() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        FileTask failedTask = normalTask("a.md");
        IOException failure = new IOException("test failure");

        assertTrue(
                coordinator.onFailure(failedTask, failure),
                "Первая ошибка должна создать новую цепочку"
        );

        for (int i = 0; i < 9; i++) {
            assertFalse(
                    coordinator.onFailure(failedTask, failure),
                    "Существующая цепочка ошибочно отмечена как новая"
            );
        }

        assertEquals(
                1,
                recoveryScheduler.scheduledCount(),
                "Для одного Path была создана лишняя retry-задача"
        );
        assertEquals(500L, recoveryScheduler.delayMillis(0));

        recoveryScheduler.runTask(0);

        assertEquals(asRecovery(failedTask), queue.poll());
        assertNull(queue.poll());
    }

    @Test
    void accessDeniedShouldStartNormalRecoveryChain() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        FileTask failedTask = normalTask("denied.md");

        boolean recoveryStarted = coordinator.onFailure(
                failedTask,
                new AccessDeniedException(failedTask.path().toString())
        );

        assertTrue(recoveryStarted);
        assertEquals(1, recoveryScheduler.scheduledCount());
        assertEquals(500L, recoveryScheduler.delayMillis(0));

        recoveryScheduler.runTask(0);

        assertEquals(asRecovery(failedTask), queue.poll());
        assertNull(queue.poll());
    }

    @Test
    void shouldUseExponentialDelaysAndStopAtMaxAttempts() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        IOException failure = new IOException("test failure");
        FileTask normalTask = normalTask("a.md");

        coordinator.onFailure(normalTask, failure);

        assertEquals(1, recoveryScheduler.scheduledCount());
        assertEquals(500L, recoveryScheduler.delayMillis(0));
        recoveryScheduler.runTask(0);
        FileTask firstRetry = (FileTask) queue.poll();
        assertEquals(asRecovery(normalTask), firstRetry);

        assertFalse(coordinator.onFailure(firstRetry, failure));

        assertEquals(2, recoveryScheduler.scheduledCount());
        assertEquals(1_000L, recoveryScheduler.delayMillis(1));
        recoveryScheduler.runTask(1);
        FileTask secondRetry = (FileTask) queue.poll();
        assertEquals(asRecovery(normalTask), secondRetry);

        assertFalse(coordinator.onFailure(secondRetry, failure));

        assertEquals(3, recoveryScheduler.scheduledCount());
        assertEquals(2_000L, recoveryScheduler.delayMillis(2));
        recoveryScheduler.runTask(2);
        FileTask thirdRetry = (FileTask) queue.poll();
        assertEquals(asRecovery(normalTask), thirdRetry);

        IOException lastFailure = new IOException("last failure");
        assertFalse(coordinator.onFailure(thirdRetry, lastFailure));

        assertEquals(
                3,
                recoveryScheduler.scheduledCount(),
                "После достижения maxAttempts была создана лишняя попытка"
        );
        assertNull(queue.poll());
        assertEquals(1, exhaustedHandler.callCount());
        assertEquals(thirdRetry, exhaustedHandler.failedTask());
        assertEquals(lastFailure, exhaustedHandler.cause());

        assertFalse(coordinator.onFailure(thirdRetry, failure));

        assertEquals(
                1,
                exhaustedHandler.callCount(),
                "Для завершённой цепочки handler вызван повторно"
        );
    }

    @Test
    void exhaustedRetriesShouldReconcileParentDirectory() throws Exception {
        Path actualFile = Files.writeString(
                tempDir.resolve("actual.md"),
                "content"
        );
        ExecutorService reconciliationExecutor =
                Executors.newSingleThreadExecutor();

        try {
            DirectoryReconciler reconciler = new DirectoryReconciler(
                    new FileScanner(),
                    new FileIndex(new ConcurrentHashMap<>()),
                    router
            );
            DirectoryReconciliationService reconciliationService =
                    new DirectoryReconciliationService(
                            reconciliationExecutor,
                            reconciler
                    );
            DirectoryRecoveryHandler recoveryHandler =
                    new DirectoryRecoveryHandler(reconciliationService);
            FileRecoveryCoordinator coordinator =
                    new FileRecoveryCoordinator(
                            recoveryScheduler,
                            router,
                            3,
                            recoveryHandler
                    );
            IOException failure = new IOException("test failure");
            FileTask failedTask = normalTask("failed.md");

            coordinator.onFailure(failedTask, failure);

            recoveryScheduler.runTask(0);
            FileTask firstRetry = (FileTask) queue.poll();
            coordinator.onFailure(firstRetry, failure);

            recoveryScheduler.runTask(1);
            FileTask secondRetry = (FileTask) queue.poll();
            coordinator.onFailure(secondRetry, failure);

            recoveryScheduler.runTask(2);
            FileTask thirdRetry = (FileTask) queue.poll();
            coordinator.onFailure(thirdRetry, failure);

            FileTask reconciliationTask = (FileTask) queue.poll(
                    2,
                    TimeUnit.SECONDS
            );

            assertNotNull(
                    reconciliationTask,
                    "После исчерпания retry не запустилась сверка директории"
            );
            assertEquals(actualFile, reconciliationTask.path());
            assertEquals(
                    ChangeType.MODIFIED,
                    reconciliationTask.changeType()
            );
            assertEquals(
                    TaskSource.RECONCILIATION,
                    reconciliationTask.taskSource()
            );
            assertEquals(3, recoveryScheduler.scheduledCount());
        } finally {
            reconciliationExecutor.shutdownNow();
            reconciliationExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void staleRecoveryTaskShouldNotStartNewRecoveryChain() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        FileTask staleRetry = new FileTask(
                tempDir.resolve("a.md"),
                ChangeType.MODIFIED,
                TaskSource.RECOVERY
        );

        assertFalse(
                coordinator.onFailure(
                        staleRetry,
                        new IOException("test failure")
                )
        );

        assertEquals(0, recoveryScheduler.scheduledCount());
        assertNull(queue.poll());
    }

    @Test
    void reconciliationFailureShouldNotStartRetryOrAnotherRescan() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        FileTask reconciliationTask = new FileTask(
                tempDir.resolve("problem.md"),
                ChangeType.MODIFIED,
                TaskSource.RECONCILIATION
        );

        boolean recoveryStarted = coordinator.onFailure(
                reconciliationTask,
                new IOException("reconciliation failure")
        );

        assertFalse(recoveryStarted);
        assertEquals(0, recoveryScheduler.scheduledCount());
        assertEquals(0, exhaustedHandler.callCount());
        assertNull(queue.poll());
    }

    @Test
    void recoveryChainsForDifferentPathsShouldHaveIndependentDelays() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        IOException failure = new IOException("test failure");
        FileTask firstNormalTask = normalTask("a.md");
        FileTask secondNormalTask = normalTask("b.md");

        coordinator.onFailure(firstNormalTask, failure);
        coordinator.onFailure(secondNormalTask, failure);

        assertEquals(500L, recoveryScheduler.delayMillis(0));
        assertEquals(500L, recoveryScheduler.delayMillis(1));

        recoveryScheduler.runTask(0);
        recoveryScheduler.runTask(1);
        FileTask firstRetry = (FileTask) queue.poll();
        FileTask secondRetry = (FileTask) queue.poll();

        coordinator.onFailure(firstRetry, failure);
        coordinator.onFailure(secondRetry, failure);

        assertEquals(1_000L, recoveryScheduler.delayMillis(2));
        assertEquals(1_000L, recoveryScheduler.delayMillis(3));
    }

    @Test
    void successShouldCancelPendingRetry() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        FileTask failedTask = normalTask("a.md");

        coordinator.onFailure(
                failedTask,
                new IOException("test failure")
        );

        assertFalse(recoveryScheduler.isCancelled(0));

        coordinator.onSuccess(failedTask.path());

        assertTrue(recoveryScheduler.isCancelled(0));
        recoveryScheduler.runTask(0);
        assertNull(
                queue.poll(),
                "Отменённая retry-задача попала в очередь"
        );
    }

    @Test
    void repeatedSuccessShouldBeSafe() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        Path path = tempDir.resolve("a.md");

        assertDoesNotThrow(() -> {
            coordinator.onSuccess(path);
            coordinator.onSuccess(path);
        });
    }

    @Test
    void successForOnePathShouldNotCancelAnotherPathRetry() {
        FileRecoveryCoordinator coordinator = coordinatorWithThreeAttempts();
        FileTask firstTask = normalTask("a.md");
        FileTask secondTask = normalTask("b.md");
        IOException failure = new IOException("test failure");

        coordinator.onFailure(firstTask, failure);
        coordinator.onFailure(secondTask, failure);
        coordinator.onSuccess(firstTask.path());

        assertTrue(recoveryScheduler.isCancelled(0));
        assertFalse(recoveryScheduler.isCancelled(1));

        recoveryScheduler.runTask(0);
        recoveryScheduler.runTask(1);

        assertEquals(asRecovery(secondTask), queue.poll());
        assertNull(queue.poll());
    }

    private FileRecoveryCoordinator coordinatorWithThreeAttempts() {
        return new FileRecoveryCoordinator(
                recoveryScheduler,
                router,
                3,
                exhaustedHandler
        );
    }

    private FileTask normalTask(String fileName) {
        return new FileTask(
                tempDir.resolve(fileName),
                ChangeType.MODIFIED
        );
    }

    private FileTask asRecovery(FileTask task) {
        return new FileTask(
                task.path(),
                task.changeType(),
                TaskSource.RECOVERY
        );
    }

    private static final class RecordingScheduler
            extends ScheduledThreadPoolExecutor {

        private final List<ScheduledCall> scheduledCalls = new ArrayList<>();

        private RecordingScheduler() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> schedule(
                Runnable command,
                long delay,
                TimeUnit unit
        ) {
            ManualScheduledFuture future = new ManualScheduledFuture(
                    command,
                    unit.toMillis(delay)
            );
            scheduledCalls.add(new ScheduledCall(
                    unit.toMillis(delay),
                    future
            ));
            return future;
        }

        private int scheduledCount() {
            return scheduledCalls.size();
        }

        private long delayMillis(int index) {
            return scheduledCalls.get(index).delayMillis();
        }

        private boolean isCancelled(int index) {
            return scheduledCalls.get(index).future().isCancelled();
        }

        private void runTask(int index) {
            scheduledCalls.get(index).future().run();
        }
    }

    private record ScheduledCall(
            long delayMillis,
            ManualScheduledFuture future
    ) {
    }

    private static final class RecordingExhaustedHandler
            implements RecoveryExhaustedHandler {

        private int callCount;
        private FileTask failedTask;
        private IOException cause;

        @Override
        public void handle(FileTask failedTask, IOException cause) {
            callCount++;
            this.failedTask = failedTask;
            this.cause = cause;
        }

        private int callCount() {
            return callCount;
        }

        private FileTask failedTask() {
            return failedTask;
        }

        private IOException cause() {
            return cause;
        }
    }

    private static final class ManualScheduledFuture
            extends FutureTask<Void>
            implements ScheduledFuture<Void> {

        private final long delayMillis;

        private ManualScheduledFuture(
                Runnable command,
                long delayMillis
        ) {
            super(command, null);
            this.delayMillis = delayMillis;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(
                    getDelay(TimeUnit.NANOSECONDS),
                    other.getDelay(TimeUnit.NANOSECONDS)
            );
        }
    }
}
