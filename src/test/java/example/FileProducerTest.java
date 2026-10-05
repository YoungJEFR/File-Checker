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

        runAndJoin(producer);

        assertNull(producer.getScanFailure());
        assertEquals(new FileTask(first, ChangeType.CREATED), queue.poll());
        assertEquals(new FileTask(second, ChangeType.CREATED), queue.poll());
        assertTrue(queue.isEmpty());
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
