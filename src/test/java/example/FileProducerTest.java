package example;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.pipeline.FileProducer;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(5)
class FileProducerTest {
    private final Path root = Path.of("watched");
    private final BlockingQueue<WorkerTask> queue = new LinkedBlockingQueue<>();
    private final TaskRouter router = new TaskRouter(List.of(queue));

    @Test
    void shouldStoreIOExceptionFromScanner() throws Exception {
        IOException expected = new IOException("Scan failed");
        FileScanner scanner = new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler) throws IOException {
                throw expected;
            }
        };
        FileProducer producer = new FileProducer(root, router, scanner);

        runAndJoin(producer);

        assertSame(expected, producer.getScanFailure());
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> producer.getCompletionFuture().get(1, TimeUnit.SECONDS));
        assertSame(expected, failure.getCause());
        assertTrue(queue.isEmpty());
    }

    @Test
    void shouldStoreCauseOfUncheckedIOException() throws Exception {
        IOException expected = new IOException("Lazy traversal failed");
        FileScanner scanner = new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler) {
                throw new UncheckedIOException(expected);
            }
        };
        FileProducer producer = new FileProducer(root, router, scanner);

        runAndJoin(producer);

        assertSame(expected, producer.getScanFailure());
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> producer.getCompletionFuture().get(1, TimeUnit.SECONDS));
        assertSame(expected, failure.getCause());
        assertTrue(queue.isEmpty());
    }

    @Test
    void successfulScanShouldRouteFilesWithoutFailure() throws Exception {
        Path first = root.resolve("first.md");
        Path second = root.resolve("second.md");
        FileScanner scanner = new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler)
                    throws InterruptedException {
                assertEquals(root, directory);
                handler.handle(first);
                handler.handle(second);
            }
        };
        FileProducer producer = new FileProducer(root, router, scanner);

        CompletableFuture<Void> completion = producer.getCompletionFuture();
        assertFalse(completion.isDone());
        runAndJoin(producer);

        assertNull(completion.get(1, TimeUnit.SECONDS));
        assertNull(producer.getScanFailure());
        assertEquals(new FileTask(first, ChangeType.CREATED), queue.poll());
        assertEquals(new FileTask(second, ChangeType.CREATED), queue.poll());
        assertTrue(queue.isEmpty());
    }

    @Test
    void runtimeFailureShouldCompleteFutureWithOriginalCause() throws Exception {
        IllegalStateException expected = new IllegalStateException("Unexpected scanner failure");
        FileProducer producer = new FileProducer(root, router, new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler) {
                throw expected;
            }
        });

        runAndJoin(producer);

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> producer.getCompletionFuture().get(1, TimeUnit.SECONDS));
        assertSame(expected, failure.getCause());
        assertTrue(queue.isEmpty());
    }

    @Test
    void interruptionShouldCompleteFutureAndRestoreInterruptFlag() throws Exception {
        InterruptedException expected = new InterruptedException("Scan interrupted");
        FileProducer producer = new FileProducer(root, router, new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler) throws InterruptedException {
                throw expected;
            }
        });
        AtomicBoolean interruptRestored = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            producer.run();
            interruptRestored.set(Thread.currentThread().isInterrupted());
        }, "InterruptedProducerTest");
        try {
            thread.start();
            thread.join(2_000);
            assertFalse(thread.isAlive());
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> producer.getCompletionFuture().get(1, TimeUnit.SECONDS));
            assertSame(expected, failure.getCause());
            assertTrue(interruptRestored.get());
        } finally {
            thread.interrupt();
            thread.join(2_000);
        }
    }

    @Test
    void completingReturnedFutureShouldNotCompleteProducer() throws Exception {
        FileProducer producer = new FileProducer(root, router, new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler) {
                // Пустое сканирование успешно завершится только после запуска Producer.
            }
        });
        producer.getCompletionFuture().complete(null);
        CompletableFuture<Void> actualCompletion = producer.getCompletionFuture();
        assertFalse(actualCompletion.isDone());

        runAndJoin(producer);

        assertNull(actualCompletion.get(1, TimeUnit.SECONDS));
    }

    private static void runAndJoin(FileProducer producer) throws Exception {
        java.util.concurrent.atomic.AtomicReference<Throwable> uncaught =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread thread = new Thread(producer, "ProducerTest");
        thread.setUncaughtExceptionHandler((ignored, failure) -> uncaught.set(failure));
        try {
            thread.start();
            thread.join(2_000);
            assertFalse(thread.isAlive(), "Producer did not finish");
            assertNull(uncaught.get(), "Producer threw an uncaught exception");
        } finally {
            thread.interrupt();
            thread.join(2_000);
        }
    }
}
