package org.example.service;

import org.example.filescanner.FileScanner;
import org.example.model.*;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.processor.FileTaskProcessor;
import org.example.watcher.FileWatcher;
import org.example.watcher.WatchRegistrar;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FileIndexerServiceLifecycleTest {

    private static final long TIMEOUT_SECONDS = 5;

    @TempDir
    Path tempDir;

    @Test
    @Timeout(10)
    void normalShutdownShouldCompleteServiceTerminationSuccessfully() throws Exception {
        FileIndexerService service = new FileIndexerService(2, tempDir, 3);
        List<Thread> threads = ownedThreads(service);

        try {
            service.start();
            // Сохраняем одну копию: она должна получить результат будущей остановки.
            CompletableFuture<Void> termination = service.getTermination();
            assertFalse(termination.isDone(), "Работающий сервис не должен сообщать о завершении");

            service.shutdown();

            assertNull(termination.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(termination.isDone());
            assertFalse(termination.isCompletedExceptionally(),
                    "Обычный shutdown не должен завершаться с ошибкой");
            assertFullyStopped(service, threads);
            for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                Field field = FileIndexerService.class.getDeclaredField(name);
                field.setAccessible(true);
                assertTrue(((ExecutorService) field.get(service)).isTerminated(),
                        name + " остался работать после завершения сервиса");
            }

            // Повторная остановка не должна менять ранее опубликованный результат.
            service.shutdown();
            assertNull(termination.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            service.shutdown();
        }
    }

    @Test
    @Timeout(15)
    void deletingNestedDirectoryShouldKeepServiceRunning() throws Exception {
        Path root = Files.createDirectory(tempDir.resolve("watched"))
                .toAbsolutePath().normalize();
        Path nested = Files.createDirectory(root.resolve("nested"));
        Path oldFile = Files.write(nested.resolve("old.md"), new byte[10]);
        Map<WatchKey, Path> registrations = new ConcurrentHashMap<>();
        FileIndexerService service = new FileIndexerService(
                2, root, 3, 100, FileProcessor::process, new FileScanner(),
                new FileIndex(), new WatchRegistrar(registrations)
        );
        List<Thread> threads = ownedThreads(service);

        try {
            service.start();
            assertEquals(Set.of(root, nested), Set.copyOf(registrations.values()));
            assertEquals(Set.of(oldFile), service.snapshot().files().keySet());
            assertEquals(10, service.snapshot().totalBytes());

            Files.delete(oldFile);
            Files.delete(nested);

            // Ждём и очистку индекса, и удаление невалидного ключа вложенной папки.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            FileIndexerSnapshot snapshot = service.snapshot();
            while (System.nanoTime() < deadline
                    && !(registrations.size() == 1 && registrations.containsValue(root)
                    && snapshot.files().isEmpty() && snapshot.countFiles() == 0
                    && snapshot.totalBytes() == 0)) {
                Thread.sleep(20);
                snapshot = service.snapshot();
            }
            assertEquals(Set.of(root), Set.copyOf(registrations.values()),
                    "Удаление вложенной папки должно убрать только её регистрацию");
            assertTrue(snapshot.files().isEmpty());
            assertEquals(0, snapshot.countFiles());
            assertEquals(0, snapshot.totalBytes());
            assertEquals(FileIndexerState.RUNNING, stateOf(service));
            assertTrue(threadField(service, "watcherThread").isAlive());
            assertTrue(threadField(service, "watcherSupervisorThread").isAlive());

            // Не только проверяем флаг: watcher должен обработать новое событие после удаления.
            Path newFile = Files.write(root.resolve("still-watched.md"), new byte[20]);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            snapshot = service.snapshot();
            while (System.nanoTime() < deadline
                    && !(snapshot.files().keySet().equals(Set.of(newFile))
                    && snapshot.countFiles() == 1 && snapshot.totalBytes() == 20)) {
                Thread.sleep(20);
                snapshot = service.snapshot();
            }
            assertEquals(Set.of(newFile), snapshot.files().keySet());
            assertEquals(1, snapshot.countFiles());
            assertEquals(20, snapshot.totalBytes());
            assertEquals(0, snapshot.errorCount());
            assertEquals(FileIndexerState.RUNNING, stateOf(service));
        } finally {
            service.shutdown();
        }
        assertFullyStopped(service, threads);
    }

    @Test
    @Timeout(10)
    void deletingRootShouldAutomaticallyShutdownService() throws Exception {
        Path root = Files.createDirectory(tempDir.resolve("watched"));
        FileIndexerService service = new FileIndexerService(2, root, 3);
        List<Thread> threads = ownedThreads(service);
        Thread supervisor = threadField(service, "watcherSupervisorThread");
        AtomicReference<Throwable> supervisorFailure = new AtomicReference<>();
        supervisor.setUncaughtExceptionHandler((thread, failure) -> supervisorFailure.set(failure));
        Field watcherField = FileIndexerService.class.getDeclaredField("fileWatcher");
        watcherField.setAccessible(true);
        FileWatcher watcher = (FileWatcher) watcherField.get(service);

        try {
            service.start();
            assertEquals(FileIndexerState.RUNNING, stateOf(service));
            assertTrue(threadField(service, "watcherThread").isAlive());
            assertTrue(supervisor.isAlive());
            CompletableFuture<Void> termination = watcher.getTermination();
            CompletableFuture<Void> serviceTermination = service.getTermination();
            assertFalse(serviceTermination.isDone(), "Работающий сервис ещё не завершён");

            // Удаляем только созданный пустой корень, не саму @TempDir.
            Files.delete(root);

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> termination.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertInstanceOf(RootWatchLostException.class, failure.getCause());
            assertTrue(failure.getCause().getMessage().contains(root.toString()));

            ExecutionException serviceFailure = assertThrows(ExecutionException.class,
                    () -> serviceTermination.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertSame(failure.getCause(), serviceFailure.getCause(),
                    "Сервис должен сохранить исходную причину завершения watcher");
            assertTrue(serviceTermination.isCompletedExceptionally());
            // Сигнал сервиса должен приходить после очистки, не только после выхода watcher.
            assertEquals(FileIndexerState.STOPPED, stateOf(service));
            for (Thread thread : threads) {
                if (thread != supervisor) {
                    assertFalse(thread.isAlive(), thread.getName() + " остался работать");
                }
            }
            for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                Field field = FileIndexerService.class.getDeclaredField(name);
                field.setAccessible(true);
                assertTrue(((ExecutorService) field.get(service)).isTerminated(),
                        name + " остался работать после потери корня");
            }
            // Supervisor публикует результат перед выходом из своего run(): ждём и этот выход.
            supervisor.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse(supervisor.isAlive(), "Supervisor не остановил сервис после потери корня");
            assertNull(supervisorFailure.get());
            assertFullyStopped(service, threads);
        } finally {
            // До проверок shutdown не вызываем: остановка должна произойти автоматически.
            service.shutdown();
        }
    }

    @Test
    @Timeout(10)
    void interruptingSupervisorShouldShutdownService() throws Exception {
        FileIndexerService service = new FileIndexerService(2, tempDir, 3);
        List<Thread> threads = ownedThreads(service);
        Thread supervisor = threadField(service, "watcherSupervisorThread");
        AtomicReference<Throwable> supervisorFailure = new AtomicReference<>();
        supervisor.setUncaughtExceptionHandler((thread, failure) -> supervisorFailure.set(failure));

        try {
            service.start();
            assertEquals(FileIndexerState.RUNNING, stateOf(service));
            assertTrue(supervisor.isAlive(), "Supervisor не запущен");
            assertTrue(threadField(service, "watcherThread").isAlive(),
                    "Watcher должен работать до прерывания supervisor");

            supervisor.interrupt();
            supervisor.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

            assertFalse(supervisor.isAlive(), "Supervisor не завершился после прерывания");
            assertNull(supervisorFailure.get(), "Supervisor завершился с необработанной ошибкой");
            // Не вызываем shutdown() до проверок: остановить сервис должен сам supervisor.
            assertFullyStopped(service, threads);
            for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                Field field = FileIndexerService.class.getDeclaredField(name);
                field.setAccessible(true);
                ExecutorService executor = (ExecutorService) field.get(service);
                assertTrue(executor.isTerminated(), name + " остался работать");
            }
        } finally {
            // Очистка нужна и при падении теста, например если в catch вернули return.
            service.shutdown();
        }
    }

    @Test
    @Timeout(10)
    void snapshotShouldNotWaitForFileProcessing() throws Exception {
        Path watched = Files.createDirectory(tempDir.resolve("watched"));
        // Файл вне наблюдаемой папки: задачу подаём сами, без событий ОС.
        Path file = Files.write(tempDir.resolve("file.md"), new byte[10])
                .toAbsolutePath().normalize();
        CountDownLatch processorEntered = new CountDownLatch(1);
        CountDownLatch allowProcessing = new CountDownLatch(1);
        CountDownLatch processingFinished = new CountDownLatch(1);

        FileTaskProcessor processor = task -> {
            processorEntered.countDown();
            allowProcessing.await();
            return FileProcessor.process(task);
        };
        FileIndexerService service = new FileIndexerService(
                1, watched, 3, 10, processor
        );
        ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor();

        try {
            service.start();
            BlockingQueue<WorkerTask> queue = queuesOf(service).get(0);
            queue.put(new FileTask(file, ChangeType.CREATED));
            queue.put(new BarrierTask(processingFinished));
            assertTrue(processorEntered.await(2, TimeUnit.SECONDS),
                    "Worker не вошёл в процессор");

            Future<FileIndexerSnapshot> future = snapshotExecutor.submit(service::snapshot);
            // Если process() удерживает indexStateLock, get() завершится по таймауту.
            FileIndexerSnapshot before = future.get(2, TimeUnit.SECONDS);

            assertEquals(1L, allowProcessing.getCount(),
                    "Snapshot должен вернуться до освобождения процессора");
            assertTrue(before.files().isEmpty());
            assertEquals(0, before.countFiles());
            assertEquals(0, before.totalBytes());
            assertEquals(0, before.errorCount());

            allowProcessing.countDown();
            assertTrue(processingFinished.await(2, TimeUnit.SECONDS),
                    "Worker не дошёл до барьера после обработки файла");

            FileIndexerSnapshot after = service.snapshot();
            assertEquals(java.util.Set.of(file), after.files().keySet());
            assertEquals(1, after.countFiles());
            assertEquals(10, after.totalBytes());
            assertEquals(0, after.errorCount());
            assertTrue(before.files().isEmpty(), "Старый snapshot не должен изменяться");
        } finally {
            // Сначала освобождаем worker, иначе shutdown() будет ждать его.
            allowProcessing.countDown();
            service.shutdown();
            snapshotExecutor.shutdownNow();
            assertTrue(snapshotExecutor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    @Timeout(10)
    void initialScanIOExceptionShouldFailStartAndStopService() throws Exception {
        assertInitialScanFailureStopsService(false);
    }

    @Test
    @Timeout(10)
    void initialScanUncheckedIOExceptionShouldFailStartAndStopService() throws Exception {
        assertInitialScanFailureStopsService(true);
    }

    private void assertInitialScanFailureStopsService(boolean unchecked) throws Exception {
        IOException expected = new IOException("Initial scan failed");
        Path file = Files.write(tempDir.resolve("accepted.md"), new byte[10]);
        AtomicBoolean scannerCalled = new AtomicBoolean();
        FileScanner scanner = new FileScanner() {
            @Override
            public void scanFile(Path directory, FileHandler handler)
                    throws IOException, InterruptedException {
                scannerCalled.set(true);
                // Ошибка возникает после передачи части файлов в очередь.
                handler.handle(file);
                if (unchecked) {
                    throw new UncheckedIOException(expected);
                }
                throw expected;
            }
        };
        FileIndexerService service = new FileIndexerService(
                2, tempDir, 3, 100, FileProcessor::process, scanner
        );
        List<Thread> threads = ownedThreads(service);

        try (service) {
            IOException actual = assertThrows(IOException.class, service::start);

            assertSame(expected, actual);
            assertTrue(scannerCalled.get(), "Ошибка должна происходить в initial scan");
            // Проверяем до close(): start() должен сам выполнить shutdown().
            assertFullyStopped(service, threads);
            for (String name : List.of(
                    "debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                Field field = FileIndexerService.class.getDeclaredField(name);
                field.setAccessible(true);
                ExecutorService executor = (ExecutorService) field.get(service);
                assertTrue(executor.isTerminated(), name + " остался работать");
            }
        }
    }

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
        result.add(threadField(service, "watcherSupervisorThread"));
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

    @Test
    @Timeout(10)
    void snapshotShouldWaitUntilIndexAndStatisticsAreUpdatedTogether() throws Exception {
        Path watched = Files.createDirectory(tempDir.resolve("watched"));
        Path file = Files.write(tempDir.resolve("file.md"), new byte[10])
                .toAbsolutePath().normalize();
        CountDownLatch indexUpdate = new CountDownLatch(1);
        CountDownLatch allowStatisticsUpdate = new CountDownLatch(1);
        CountDownLatch snapshotStarted = new CountDownLatch(1);

        FileIndex newFileIndex = new FileIndex() {
            @Override
            public FileInfo addToMap(FileInfo fileInfo){
                FileInfo info = super.addToMap(fileInfo);
                indexUpdate.countDown();
                try {
                    allowStatisticsUpdate.await();
                }catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Тестовое ожидание прервано", e);
                }
                return info;
            }
        };

        FileIndexerService service = new FileIndexerService(
                1, watched, 3, 10, FileProcessor::process,
                new FileScanner(), newFileIndex
        );
        ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor();

        try {
            service.start();
            queuesOf(service).get(0).put(new FileTask(file, ChangeType.CREATED));
            assertTrue(indexUpdate.await(2, TimeUnit.SECONDS),
                    "Worker не добавил файл в индекс");
            assertNotNull(newFileIndex.getFileInfo(file));

            Future<FileIndexerSnapshot> snapshotFuture = snapshotExecutor.submit(() -> {
                snapshotStarted.countDown();
                return service.snapshot();
            });
            assertTrue(snapshotStarted.await(2, TimeUnit.SECONDS),
                    "Поток snapshot не начал выполнение");

            assertThrows(TimeoutException.class,
                    () -> snapshotFuture.get(200, TimeUnit.MILLISECONDS),
                    "Snapshot вернулся посередине обновления индекса и статистики");

            allowStatisticsUpdate.countDown();
            FileIndexerSnapshot snapshot = snapshotFuture.get(2, TimeUnit.SECONDS);

            assertEquals(java.util.Set.of(file), snapshot.files().keySet());
            assertEquals(1, snapshot.countFiles());
            assertEquals(10, snapshot.totalBytes());
            assertEquals(0, snapshot.errorCount());
            assertEquals(snapshot.files().size(), snapshot.countFiles());
            assertEquals(snapshot.files().values().stream()
                    .mapToLong(FileInfo::fileSize).sum(), snapshot.totalBytes());
        } finally {
            allowStatisticsUpdate.countDown();
            service.shutdown();
            snapshotExecutor.shutdownNow();
            assertTrue(snapshotExecutor.awaitTermination(2, TimeUnit.SECONDS));
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
