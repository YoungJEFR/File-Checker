package org.example.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class FileIndexerServiceExecutorShutdownTest {
    @TempDir
    Path tempDir;

    private FileIndexerService service;

    @BeforeEach
    void createService() {
        service = new FileIndexerService(2, tempDir, 3);
    }

    @AfterEach
    void cleanup() {
        // Один тест намеренно восстанавливает interrupt-флаг: не передаём его JUnit.
        Thread.interrupted();
        service.shutdown();
    }

    @Test
    void gracefulTerminationShouldNotForceShutdown() throws Exception {
        ControlledExecutor executor = new ControlledExecutor(Outcome.TERMINATED);

        stopExecutor(executor);

        assertEquals(List.of("shutdown", "await"), executor.calls);
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void terminationAfterShutdownNowShouldSucceed() throws Exception {
        ControlledExecutor executor = new ControlledExecutor(Outcome.TIMEOUT, Outcome.TERMINATED);

        stopExecutor(executor);

        assertEquals(List.of("shutdown", "await", "shutdownNow", "await"), executor.calls);
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void exhaustedWaitsShouldFailWithoutInventingInterruption() {
        ControlledExecutor executor = new ControlledExecutor(Outcome.TIMEOUT, Outcome.TIMEOUT);

        assertThrows(IllegalStateException.class, () -> stopExecutor(executor));

        assertEquals(List.of("shutdown", "await", "shutdownNow", "await"), executor.calls);
        assertFalse(Thread.currentThread().isInterrupted(),
                "Таймаут сам по себе не является прерыванием вызывающего потока");
    }

    @Test
    void successfulTerminationAfterInterruptionShouldReportInterruptedException() {
        ControlledExecutor executor = new ControlledExecutor(Outcome.INTERRUPTED, Outcome.TERMINATED);

        assertThrows(InterruptedException.class, () -> stopExecutor(executor));

        assertEquals(List.of("shutdown", "await", "await"), executor.calls);
        assertTrue(executor.isTerminated());
    }

    @Test
    void exhaustedWaitsAfterInterruptionShouldRestoreInterruptAndReportTimeout() {
        // Прерывание именно второго ожидания: метод должен повторить его, а не выйти раньше.
        ControlledExecutor executor = new ControlledExecutor(
                Outcome.TIMEOUT, Outcome.INTERRUPTED, Outcome.TIMEOUT
        );

        assertThrows(IllegalStateException.class, () -> stopExecutor(executor));

        assertTrue(Thread.currentThread().isInterrupted(),
                "Сообщая о таймауте, нельзя потерять ранее полученное прерывание");
        assertEquals(List.of("shutdown", "await", "shutdownNow", "await", "await"), executor.calls);
    }

    @Test
    void executorTimeoutShouldFailServiceTerminationAndContinueRemainingCleanup() throws Exception {
        ControlledExecutor executor = new ControlledExecutor(Outcome.TIMEOUT, Outcome.TIMEOUT);
        assertTimeoutFailsServiceAndContinuesCleanup("rescanExecutor", executor);
    }

    @Test
    void recoveryExecutorTimeoutShouldFailServiceTerminationAndStillStopWorkers() throws Exception {
        ControlledScheduledExecutor executor = new ControlledScheduledExecutor(Outcome.TIMEOUT);
        assertTimeoutFailsServiceAndContinuesCleanup("recoveryExecutor", executor);
        assertEquals(List.of("shutdownNow", "await"), ((ControlledExecutor) executor).calls);
    }

    private void assertTimeoutFailsServiceAndContinuesCleanup(
            String executorName, ControlledExecutor executor
    ) throws Exception {
        // Настоящий executor ещё не запускался. Подмена создаёт таймаут без зависшего потока.
        Field field = FileIndexerService.class.getDeclaredField(executorName);
        field.setAccessible(true);
        ExecutorService original = (ExecutorService) field.get(service);
        field.set(service, executor);

        try {
            service.start();
            assertDoesNotThrow(service::shutdown);

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> service.getTermination().get(2, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertFalse(executor.isTerminated(), "Тест должен моделировать незавершившийся executor");
            assertFalse(Thread.currentThread().isInterrupted());
            for (Thread worker : (Thread[]) value("workers")) {
                assertFalse(worker.isAlive(), "Workers должны остановиться после ошибки executor");
            }
            for (String name : List.of("debounceExecutor", "rescanExecutor", "recoveryExecutor")) {
                if (!name.equals(executorName)) {
                    assertTrue(((ExecutorService) value(name)).isTerminated(),
                            name + " не завершился");
                }
            }
            for (String name : List.of("watcherThread", "producerThread", "watcherSupervisorThread")) {
                Thread thread = (Thread) value(name);
                thread.join(2_000);
                assertFalse(thread.isAlive(), name + " остался работать");
            }
        } finally {
            original.shutdownNow();
            assertTrue(original.awaitTermination(2, TimeUnit.SECONDS));
            service.shutdown();
        }
    }

    private Object value(String name) throws Exception {
        Field field = FileIndexerService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(service);
    }

    private void stopExecutor(ExecutorService executor) throws Exception {
        Method method = FileIndexerService.class.getDeclaredMethod("stopExecutor", ExecutorService.class);
        method.setAccessible(true);
        try {
            method.invoke(service, executor);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw new AssertionError(e.getCause());
        }
    }

    private enum Outcome { TERMINATED, TIMEOUT, INTERRUPTED }

    private static class ControlledScheduledExecutor extends ControlledExecutor
            implements ScheduledExecutorService {
        ControlledScheduledExecutor(Outcome... outcomes) {
            super(outcomes);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Тест не планирует задачи");
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> task, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Тест не планирует задачи");
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long delay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("Тест не планирует задачи");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("Тест не планирует задачи");
        }
    }

    private static class ControlledExecutor extends AbstractExecutorService {
        private final ArrayDeque<Outcome> outcomes;
        private final List<String> calls = new ArrayList<>();
        private boolean shutdown;
        private boolean terminated;

        ControlledExecutor(Outcome... outcomes) {
            this.outcomes = new ArrayDeque<>(Arrays.asList(outcomes));
        }

        @Override
        public void shutdown() {
            calls.add("shutdown");
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            calls.add("shutdownNow");
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() { return shutdown; }

        @Override
        public boolean isTerminated() { return terminated; }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            calls.add("await");
            Outcome outcome = outcomes.removeFirst();
            if (outcome == Outcome.INTERRUPTED) {
                throw new InterruptedException("Test interruption");
            }
            terminated = outcome == Outcome.TERMINATED;
            return terminated;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("Этот дубль используется только для остановки");
        }
    }
}
