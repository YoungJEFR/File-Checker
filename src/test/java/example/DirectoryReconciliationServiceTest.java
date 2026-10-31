package example;

import org.example.filescanner.FileScanner;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryReconciliationServiceTest {

    @TempDir
    Path tempDir;

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void stopExecutor() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void successfulReconciliationShouldNotCompleteServiceFuture() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch reconciliationTaskFinished = new CountDownLatch(1);
        DirectoryReconciliationService service = service(directory ->
                calls.incrementAndGet()
        );
        CompletableFuture<Void> failureSignal = service.getTerminationFuture();

        assertFalse(failureSignal.isDone());

        service.request(tempDir);
        // В однопоточном executor эта задача начнётся после задачи сверки.
        executor.submit(reconciliationTaskFinished::countDown);

        assertTrue(reconciliationTaskFinished.await(2, TimeUnit.SECONDS),
                "Задача сверки не завершилась");
        assertEquals(1, calls.get(), "Сверка должна выполниться один раз");
        assertFalse(failureSignal.isDone(),
                "Успешная сверка не должна завершать future службы");
        assertFalse(service.getTerminationFuture().isDone(),
                "Внутренний future службы должен оставаться незавершённым");
    }

    @Test
    void runtimeFailureShouldPublishOriginalCauseAndRemoveDirectoryStatus() throws Exception {
        IllegalStateException expected = new IllegalStateException("Reconciliation failed");
        DirectoryReconciliationService service = service(directory -> {
            throw expected;
        });
        CompletableFuture<Void> failureSignal = service.getTerminationFuture();

        service.request(tempDir);

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> failureSignal.get(2, TimeUnit.SECONDS));
        assertSame(expected, failure.getCause());
        awaitExecutor();
        assertTrue(statusesOf(service).isEmpty(),
                "После аварии не должно оставаться статуса RUNNING");
    }

    @Test
    void runtimeFailureShouldDiscardRequestedAdditionalPass() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstPassStarted = new CountDownLatch(1);
        CountDownLatch allowFailure = new CountDownLatch(1);
        IllegalStateException expected = new IllegalStateException("Active pass failed");
        DirectoryReconciliationService service = service(directory -> {
            calls.incrementAndGet();
            firstPassStarted.countDown();
            allowFailure.await();
            throw expected;
        });

        try {
            service.request(tempDir);
            assertTrue(firstPassStarted.await(2, TimeUnit.SECONDS));
            service.request(tempDir);
            service.request(tempDir);
            assertTrue(statusesOf(service).containsKey(tempDir.toAbsolutePath().normalize()));

            allowFailure.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> service.getTerminationFuture().get(2, TimeUnit.SECONDS));
            assertSame(expected, failure.getCause());
            awaitExecutor();
            assertEquals(1, calls.get(), "После аварии повторный проход запускаться не должен");
            assertTrue(statusesOf(service).isEmpty(),
                    "После аварии не должно оставаться статуса RUNNING_AGAIN");
        } finally {
            allowFailure.countDown();
        }
    }

    @Test
    void completingFutureCopyShouldNotHideLaterReconciliationFailure() throws Exception {
        IllegalStateException expected = new IllegalStateException("Reconciliation failed");
        DirectoryReconciliationService service = service(directory -> {
            throw expected;
        });
        CompletableFuture<Void> externalCopy = service.getTerminationFuture();
        assertTrue(externalCopy.complete(null));
        CompletableFuture<Void> actualSignal = service.getTerminationFuture();
        assertFalse(actualSignal.isDone(), "Внешний код не должен завершать внутренний future");

        service.request(tempDir);

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> actualSignal.get(2, TimeUnit.SECONDS));
        assertSame(expected, failure.getCause());
        assertNull(externalCopy.get(2, TimeUnit.SECONDS));
    }

    @Test
    void firstRequestShouldStartOneReconciliation() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirectoryReconciliationService service = service(directory ->
                calls.incrementAndGet()
        );

        service.request(tempDir);
        awaitExecutor();

        assertEquals(1, calls.get());
    }

    @Test
    void requestsDuringRunningReconciliationShouldCauseOneAdditionalPass()
            throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstPassStarted = new CountDownLatch(1);
        CountDownLatch allowFirstPassToFinish = new CountDownLatch(1);
        CountDownLatch secondPassFinished = new CountDownLatch(1);

        DirectoryReconciliationService service = service(directory -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                firstPassStarted.countDown();
                allowFirstPassToFinish.await();
            }
            if (call == 2) {
                secondPassFinished.countDown();
            }
        });

        service.request(tempDir);
        assertTrue(firstPassStarted.await(2, TimeUnit.SECONDS));

        for (int i = 0; i < 10; i++) {
            service.request(tempDir);
        }

        allowFirstPassToFinish.countDown();

        assertTrue(secondPassFinished.await(2, TimeUnit.SECONDS));
        awaitExecutor();
        assertEquals(2, calls.get());
    }

    @Test
    void requestAfterCompletionShouldStartNewReconciliation()
            throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirectoryReconciliationService service = service(directory ->
                calls.incrementAndGet()
        );

        service.request(tempDir);
        awaitExecutor();
        service.request(tempDir);
        awaitExecutor();

        assertEquals(2, calls.get());
    }

    @Test
    void differentDirectoriesShouldBeReconciledIndependently()
            throws Exception {
        Path firstDirectory = Files.createDirectory(tempDir.resolve("first"));
        Path secondDirectory = Files.createDirectory(tempDir.resolve("second"));
        ConcurrentLinkedQueue<Path> reconciledDirectories =
                new ConcurrentLinkedQueue<>();
        DirectoryReconciliationService service = service(
                reconciledDirectories::add
        );

        service.request(firstDirectory);
        service.request(secondDirectory);
        awaitExecutor();

        assertEquals(2, reconciledDirectories.size());
        assertEquals(
                Set.of(
                        firstDirectory.toAbsolutePath().normalize(),
                        secondDirectory.toAbsolutePath().normalize()
                ),
                Set.copyOf(reconciledDirectories)
        );
    }

    @Test
    void IOExceptionShouldNotPreventLaterRequest() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirectoryReconciliationService service = service(directory -> {
            if (calls.incrementAndGet() == 1) {
                throw new IOException("expected test failure");
            }
        });

        service.request(tempDir);
        awaitExecutor();
        service.request(tempDir);
        awaitExecutor();

        assertEquals(2, calls.get());
        assertFalse(service.getTerminationFuture().isDone(),
                "Обработанная IOException не должна считаться аварией службы");
    }

    @Test
    void interruptionShouldNotPreventLaterRequest() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirectoryReconciliationService service = service(directory -> {
            calls.incrementAndGet();
            throw new InterruptedException("expected test interruption");
        });

        service.request(tempDir);
        awaitExecutor();
        service.request(tempDir);
        awaitExecutor();

        assertEquals(2, calls.get());
    }

    @Test
    void requestAfterShutdownShouldNotStartReconciliation() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirectoryReconciliationService service = service(directory ->
                calls.incrementAndGet()
        );

        service.shutdown();
        service.request(tempDir);
        awaitExecutor();

        assertEquals(
                0,
                calls.get(),
                "Reconciliation был запущен после shutdown"
        );
    }

    @Test
    void shutdownDuringRunningReconciliationShouldFinishAcceptedPassesOnly()
            throws Exception {
        AtomicInteger calls = new AtomicInteger();

        CountDownLatch firstPassStarted = new CountDownLatch(1);
        CountDownLatch allowFirstPassToFinish = new CountDownLatch(1);
        CountDownLatch secondPassFinished = new CountDownLatch(1);
        DirectoryReconciliationService service = service(directory -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                firstPassStarted.countDown();
                allowFirstPassToFinish.await();
            } else if (call == 2) {
                secondPassFinished.countDown();
            }
        });

        service.request(tempDir);

        try {
            assertTrue(
                    firstPassStarted.await(2, TimeUnit.SECONDS),
                    "Первый reconciliation не начался"
            );

            service.request(tempDir);

            service.shutdown();

            service.request(tempDir);

            allowFirstPassToFinish.countDown();

            assertTrue(
                    secondPassFinished.await(2, TimeUnit.SECONDS),
                    "Принятый до shutdown второй проход не завершился"
            );

            awaitExecutor();

            assertEquals(
                    2,
                    calls.get(),
                    "После shutdown был выполнен лишний reconciliation"
            );
        } finally {
            allowFirstPassToFinish.countDown();
        }
    }

    @Test
    void uncheckedIOExceptionShouldNotPreventLaterRequest()
            throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirectoryReconciliationService service = service(directory -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                throw new UncheckedIOException(
                        new IOException("Ошибка обхода для теста")
                );
            }
        });

        service.request(tempDir);
        awaitExecutor();
        assertEquals(1, calls.get());

        service.request(tempDir);
        awaitExecutor();
        assertEquals(2, calls.get());
        assertFalse(service.getTerminationFuture().isDone(),
                "Обработанная UncheckedIOException не должна считаться аварией службы");
    }

    private static Map<?, ?> statusesOf(
            DirectoryReconciliationService service
    ) throws ReflectiveOperationException {
        Field field = DirectoryReconciliationService.class
                .getDeclaredField("reconciliationStatusMap");
        field.setAccessible(true);
        return Map.copyOf((Map<?, ?>) field.get(service));
    }

    private DirectoryReconciliationService service(
            ReconciliationAction action
    ) {
        return new DirectoryReconciliationService(
                executor,
                new ControlledDirectoryReconciler(action)
        );
    }

    private void awaitExecutor() throws Exception {
        executor.submit(() -> {
        }).get(2, TimeUnit.SECONDS);
    }

    @FunctionalInterface
    private interface ReconciliationAction {
        void run(Path directory) throws IOException, InterruptedException;
    }

    private static class ControlledDirectoryReconciler
            extends DirectoryReconciler {

        private final ReconciliationAction action;

        private ControlledDirectoryReconciler(ReconciliationAction action) {
            super(
                    new FileScanner(),
                    new FileIndex(),
                    new TaskRouter(List.of(
                            new ArrayBlockingQueue<WorkerTask>(1)
                    ))
            );
            this.action = action;
        }

        @Override
        public void reconcile(Path directory)
                throws IOException, InterruptedException {
            action.run(directory);
        }
    }
}
