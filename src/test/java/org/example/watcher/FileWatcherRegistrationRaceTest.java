package org.example.watcher;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class FileWatcherRegistrationRaceTest {
    @TempDir
    Path tempDir;

    @Test
    void disappearingDirectoryDuringWalkOpeningShouldReconcileParentAndKeepWatching()
            throws Exception {
        assertDisappearingDirectoryIsRecoverable(false);
    }

    @Test
    void disappearingDirectoryDuringLazyWalkShouldReconcileParentAndKeepWatching()
            throws Exception {
        assertDisappearingDirectoryIsRecoverable(true);
    }

    private void assertDisappearingDirectoryIsRecoverable(boolean lazyFailure)
            throws Exception {
        Path root = tempDir.toAbsolutePath().normalize();
        Path parent = Files.createDirectory(root.resolve("parent"));
        // Расширение .md проверяет, что после ошибки не появится файловый CREATE.
        Path disappearing = parent.resolve("folder.md");
        CountDownLatch registrationEntered = new CountDownLatch(1);
        CountDownLatch watcherReady = new CountDownLatch(1);
        BlockingQueue<WorkerTask> tasks = new LinkedBlockingQueue<>();
        BlockingQueue<Path> reconciliationRequests = new LinkedBlockingQueue<>();
        TaskRouter router = new TaskRouter(List.of(tasks));
        ScheduledExecutorService debounceExecutor = Executors.newSingleThreadScheduledExecutor();
        ExecutorService reconciliationExecutor = Executors.newSingleThreadExecutor();
        FileChangeDebounce debounce = new FileChangeDebounce(debounceExecutor, router);
        DirectoryReconciliationService reconciliation = new DirectoryReconciliationService(
                reconciliationExecutor,
                new DirectoryReconciler(new FileScanner(), new FileIndex(), router)
        ) {
            @Override
            public void request(Path directory) {
                // Проверяем запрос watcher без выполнения самой сверки.
                reconciliationRequests.add(directory.toAbsolutePath().normalize());
            }
        };
        WatchRegistrar registrar = new WatchRegistrar(new ConcurrentHashMap<>(), directory -> {
            if (!directory.toAbsolutePath().normalize().equals(disappearing)) {
                return Files.walk(directory);
            }

            // Имитируем исчезновение после Files.isDirectory(), не создавая DELETE:
            // иначе его запрос сверки мог бы скрыть отсутствие запроса из catch.
            registrationEntered.countDown();
            NoSuchFileException expected = new NoSuchFileException(disappearing.toString());
            if (lazyFailure) {
                return Stream.<Path>generate(() -> {
                    throw new UncheckedIOException(expected);
                });
            }
            throw expected;
        });
        FileWatcher watcher = new FileWatcher(
                root, debounce, router, registrar, reconciliation, watcherReady
        );
        Thread watcherThread = new Thread(watcher, "RegistrationRaceWatcherTest");
        CompletableFuture<Void> termination = watcher.getTermination();

        try {
            watcherThread.start();
            assertTrue(watcherReady.await(3, TimeUnit.SECONDS));
            assertFalse(termination.isDone(), "Watcher не запустился");

            Files.createDirectory(disappearing);
            assertTrue(registrationEntered.await(3, TimeUnit.SECONDS),
                    "Watcher не дошёл до регистрации новой папки");
            assertEquals(parent, reconciliationRequests.poll(3, TimeUnit.SECONDS),
                    "Нужна сверка родителя, а не исчезнувшей папки или корня");
            assertTrue(Files.isDirectory(disappearing),
                    "Исчезновение имитируется исключением без дополнительного DELETE");

            // Новое событие доказывает, что watcher продолжил цикл после ошибки.
            Path nextFile = Files.writeString(parent.resolve("next.md"), "content");
            List<FileTask> received = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean nextCreateReceived = false;
            while (!nextCreateReceived) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "Watcher перестал принимать CREATE после ошибки");
                WorkerTask task = tasks.poll(remaining, TimeUnit.NANOSECONDS);
                assertNotNull(task, "Watcher перестал принимать CREATE после ошибки");
                if (task instanceof FileTask fileTask) {
                    received.add(fileTask);
                    nextCreateReceived = fileTask.path().equals(nextFile)
                            && fileTask.changeType() == ChangeType.CREATED;
                }
            }
            assertTrue(received.stream().noneMatch(task -> task.path().equals(disappearing)
                            && task.changeType() == ChangeType.CREATED),
                    "После ошибки регистрации папка .md не должна отправляться как файловый CREATE");
            assertTrue(reconciliationRequests.stream().noneMatch(disappearing::equals),
                    "Нельзя запрашивать rescan исчезнувшей папки");
            assertFalse(termination.isDone(), "Локальная ошибка завершила watcher");
            assertTrue(watcherThread.isAlive());
        } finally {
            watcherThread.interrupt();
            watcherThread.join(3_000);
            debounce.shutdown();
            reconciliation.shutdown();
            debounceExecutor.shutdownNow();
            reconciliationExecutor.shutdownNow();
            assertTrue(debounceExecutor.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(reconciliationExecutor.awaitTermination(2, TimeUnit.SECONDS));
            assertFalse(watcherThread.isAlive(), "Watcher не завершился при очистке теста");
        }
    }
}
