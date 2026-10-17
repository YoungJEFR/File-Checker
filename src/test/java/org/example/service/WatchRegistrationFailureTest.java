package org.example.service;

import org.example.filescanner.FileScanner;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.watcher.WatchRegistrar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class WatchRegistrationFailureTest {
    @TempDir
    Path tempDir;

    @Test
    void runtimeRegistrationFailureShouldFailStartAndTerminateServiceResources() throws Exception {
        IllegalStateException expected = new IllegalStateException("Unexpected registration failure");
        AtomicBoolean streamClosed = new AtomicBoolean();
        AtomicBoolean initialScanCalled = new AtomicBoolean();
        Map<WatchKey, Path> registrations = new ConcurrentHashMap<>();
        WatchRegistrar registrar = new WatchRegistrar(registrations, root -> Stream.concat(
                Stream.of(root),
                Stream.<Path>generate(() -> {
                    throw expected;
                })
        ).onClose(() -> streamClosed.set(true)));
        FileScanner scanner = new FileScanner() {
            @Override
            public void scanFile(Path root, FileHandler handler) {
                initialScanCalled.set(true);
            }
        };
        FileIndexerService service = new FileIndexerService(
                2, tempDir, 3, 10, FileProcessor::process, scanner,
                new FileIndex(), registrar
        );

        try (service) {
            IllegalStateException actual = assertThrows(IllegalStateException.class, service::start);

            assertSame(expected, actual, "Сервис должен передать исходную runtime-ошибку");
            assertFalse(initialScanCalled.get(), "Initial scan не должен запускаться");
            assertTrue(streamClosed.get(), "Поток обхода должен закрыться");
            assertEquals(1, registrations.size(), "Ошибка должна возникнуть после частичной регистрации");
            assertTrue(registrations.keySet().stream().noneMatch(WatchKey::isValid));
            // До close(): start() должен сам остановить все созданные ресурсы.
            assertEquals(FileIndexerState.STOPPED, field(service, "indexerState"));
            for (Thread worker : (Thread[]) field(service, "workers")) {
                assertFalse(worker.isAlive(), worker.getName() + " остался работать");
            }
            assertFalse(((Thread) field(service, "watcherThread")).isAlive());
            for (String name : List.of("producerThread", "watcherSupervisorThread")) {
                assertEquals(Thread.State.NEW, ((Thread) field(service, name)).getState(),
                        name + " не должен запускаться после ошибки регистрации");
            }
            for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                assertTrue(((ExecutorService) field(service, name)).isTerminated(),
                        name + " не завершён");
            }
        }
    }

    @Test
    void lazyWalkFailureShouldBecomeIOExceptionAndCloseStream() throws Exception {
        IOException expected = new IOException("Lazy directory walk failed");
        AtomicBoolean streamClosed = new AtomicBoolean();
        Map<WatchKey, Path> registrations = new ConcurrentHashMap<>();
        WatchRegistrar registrar = failingRegistrar(registrations, expected, streamClosed);

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            IOException actual = assertThrows(IOException.class,
                    () -> registrar.registerRecursively(tempDir, watchService));

            assertSame(expected, actual, "Наружу должна передаваться исходная причина");
            assertEquals(1, registrations.size(),
                    "Ошибка должна возникнуть после регистрации первой папки");
            assertTrue(streamClosed.get(), "Поток обхода должен закрыться при ошибке");
        }
        assertTrue(registrations.keySet().stream().noneMatch(WatchKey::isValid));
    }

    @Test
    void registrationFailureShouldFailStartAndTerminateServiceResources() throws Exception {
        IOException expected = new IOException("Watcher registration failed");
        AtomicBoolean streamClosed = new AtomicBoolean();
        AtomicBoolean initialScanCalled = new AtomicBoolean();
        Map<WatchKey, Path> registrations = new ConcurrentHashMap<>();
        WatchRegistrar registrar = failingRegistrar(registrations, expected, streamClosed);
        FileScanner scanner = new FileScanner() {
            @Override
            public void scanFile(Path root, FileHandler handler) {
                initialScanCalled.set(true);
            }
        };
        FileIndexerService service = new FileIndexerService(
                2, tempDir, 3, 10, FileProcessor::process, scanner,
                new FileIndex(), registrar
        );

        try (service) {
            IOException actual = assertThrows(IOException.class, service::start);

            assertSame(expected, actual);
            assertFalse(initialScanCalled.get(), "Initial scan не должен начаться после ошибки watcher");
            assertTrue(streamClosed.get());
            assertEquals(1, registrations.size());
            assertTrue(registrations.keySet().stream().noneMatch(WatchKey::isValid),
                    "WatchService должен закрыться вместе с регистрациями");
            // Проверяем до close(): start() обязан самостоятельно очистить ресурсы.
            assertEquals(FileIndexerState.STOPPED, field(service, "indexerState"));
            for (Thread worker : (Thread[]) field(service, "workers")) {
                assertFalse(worker.isAlive(), worker.getName() + " остался работать");
            }
            assertFalse(((Thread) field(service, "watcherThread")).isAlive());
            Thread producer = (Thread) field(service, "producerThread");
            assertEquals(Thread.State.NEW, producer.getState(), "Producer не должен запускаться");
            for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                assertTrue(((ExecutorService) field(service, name)).isTerminated(),
                        name + " не завершён");
            }
        }
    }

    private static WatchRegistrar failingRegistrar(Map<WatchKey, Path> registrations,
                                                   IOException cause,
                                                   AtomicBoolean streamClosed) {
        return new WatchRegistrar(registrations, root -> Stream.concat(
                Stream.of(root),
                Stream.<Path>generate(() -> {
                    // Ошибка возникает при чтении следующего элемента, не при создании stream.
                    throw new UncheckedIOException(cause);
                })
        ).onClose(() -> streamClosed.set(true)));
    }

    private static Object field(FileIndexerService service, String name)
            throws ReflectiveOperationException {
        Field field = FileIndexerService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(service);
    }
}
