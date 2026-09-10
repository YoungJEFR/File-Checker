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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                    new FileIndex(new ConcurrentHashMap<>()),
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
