package example;

import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.model.WorkerTask;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FileChangeDebounceTest {
    private BlockingQueue<WorkerTask> queue;
    private ScheduledExecutorService scheduler;
    private FileChangeDebounce debounce;
    private Path path;
    private FileTask fileTask;
    private TaskRouter router;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newScheduledThreadPool(1);
        queue = new LinkedBlockingQueue<>();
        List<BlockingQueue<WorkerTask>> queues = List.of(queue);
        router = new TaskRouter(queues);

        debounce = new FileChangeDebounce(scheduler, router);

        path = Path.of("test.md");

        fileTask = new FileTask(path, ChangeType.MODIFIED);
    }

    @Test
    void manyRapidModifiesForSamePathShouldProduceOneTask()
            throws InterruptedException {
        for (int i = 0; i < 2000; i++) {
            debounce.debounceOnModify(fileTask);
        }

        WorkerTask result = queue.poll(2, TimeUnit.SECONDS);

        assertNotNull(result, "Debounce не отправил задачу");

        FileTask routedTask = assertInstanceOf(
                FileTask.class,
                result,
                "В очередь попала не файловая задача"
        );

        assertEquals(path, routedTask.path());
        assertEquals(ChangeType.MODIFIED, routedTask.changeType());
        assertEquals(TaskSource.NORMAL, routedTask.taskSource());

        WorkerTask unexpectedTask = queue.poll(
                700,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                unexpectedTask,
                "Debounce отправил большее одной задачи"
        );

    }

    @Test
    void cancelPendingModifyShouldProduceNoTask()
            throws InterruptedException {
        debounce.debounceOnModify(fileTask);
        debounce.debounceCancel(path);

        WorkerTask result = queue.poll(
                600,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                result,
                "Задача появилась в Debounce"
        );


    }

    @Test
    void concurrentModifiesForSamePathShouldProduceOneTask()
            throws InterruptedException {
        final int countThread = 20;
        ExecutorService callerExecutor = Executors.newFixedThreadPool(countThread);
        CountDownLatch readyLatch = new CountDownLatch(countThread);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(countThread);

        try {
            for (int i = 0; i < countThread; i++) {
                callerExecutor.submit(() -> {
                    readyLatch.countDown();

                    try {
                        startLatch.await();
                        debounce.debounceOnModify(fileTask);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }


            assertTrue(
                    readyLatch.await(2, TimeUnit.SECONDS),
                    "Не все потоки подготовились"
            );

            startLatch.countDown();

            assertTrue(
                    doneLatch.await(2, TimeUnit.SECONDS),
                    "Не все конкурентные задачи завершились"
            );

            WorkerTask result = queue.poll(2, TimeUnit.SECONDS);

            assertNotNull(result, "Debounce не отправил задачу");

            FileTask routedTask = assertInstanceOf(
                    FileTask.class,
                    result,
                    "В очередь попала не файловая задача"
            );

            assertEquals(ChangeType.MODIFIED, routedTask.changeType());

            WorkerTask unexpectedTask = queue.poll(
                    700,
                    TimeUnit.MILLISECONDS
            );

            assertNull(
                    unexpectedTask,
                    "В debounce появилась лишняя задача"
            );
        } finally {
            callerExecutor.shutdownNow();
            callerExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void secondModifyShouldRestartDebounceDelay() throws InterruptedException {
        debounce.debounceOnModify(fileTask);

        WorkerTask result = queue.poll(
                300,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                result,
                "Появилась задача в debounce"
        );

        debounce.debounceOnModify(fileTask);

        WorkerTask unexpectedTask = queue.poll(
                300,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                unexpectedTask,
                "Появилась задача в debounce"
        );

        WorkerTask expectedTask = queue.poll(
                1,
                TimeUnit.SECONDS
        );

        assertNotNull(
                expectedTask,
                "Задача не появилась"
        );

        FileTask routedTask = assertInstanceOf(
                FileTask.class,
                expectedTask,
                "В очередь попала задача другого типа"
        );

        assertEquals(path, routedTask.path());
        assertEquals(ChangeType.MODIFIED, routedTask.changeType());
    }

    @Test
    void completedOldTaskShouldNotRemoveNewerPendingTask()
            throws InterruptedException {
        BlockingFirstRouteTaskRouter blockingRouter =
                new BlockingFirstRouteTaskRouter(List.of(queue));
        FileChangeDebounce controlledDebounce =
                new FileChangeDebounce(scheduler, blockingRouter);

        try {
            controlledDebounce.debounceOnModify(fileTask);

            assertTrue(
                    blockingRouter.firstRouteStarted.await(
                            1,
                            TimeUnit.SECONDS
                    ),
                    "Первая задача не начала выполняться"
            );

            controlledDebounce.debounceOnModify(fileTask);
            blockingRouter.allowFirstRouteToFinish.countDown();

            WorkerTask firstTask = queue.poll(
                    1,
                    TimeUnit.SECONDS
            );

            assertNotNull(
                    firstTask,
                    "Первая уже запущенная задача не попала в очередь"
            );

            Thread.sleep(100);

            controlledDebounce.debounceCancel(path);

            WorkerTask unexpectedSecondTask = queue.poll(
                    700,
                    TimeUnit.MILLISECONDS
            );

            assertNull(
                    unexpectedSecondTask,
                    "Старая задача удалила новую из Map, поэтому её не удалось отменить"
            );
        } finally {
            blockingRouter.allowFirstRouteToFinish.countDown();
        }
    }

    @Test
    void createFollowedByModifyShouldPreserveOrder() throws InterruptedException {
        FileTask fileTask1 = new FileTask(path, ChangeType.CREATED);
        router.route(fileTask1);

        FileTask fileTask2 = new FileTask(path, ChangeType.MODIFIED);
        debounce.debounceOnModify(fileTask2);

        WorkerTask result = queue.poll(
                100,
                TimeUnit.MILLISECONDS
        );

        FileTask task = assertInstanceOf(
                FileTask.class,
                result,
                "Пришел другой файл"
        );

        assertEquals(path, task.path());
        assertEquals(ChangeType.CREATED, task.changeType(), "Пришла другая задача");

        WorkerTask unexpectedTask = queue.poll(
                400,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                unexpectedTask,
                "MODIFIED появился раньше завершения debounce"
        );

        WorkerTask expectedTask = queue.poll(
                1,
                TimeUnit.SECONDS
        );

        task = assertInstanceOf(
                FileTask.class,
                expectedTask,
                "Пришел другой файл"
        );

        assertEquals(path, task.path());
        assertEquals(ChangeType.MODIFIED, task.changeType());

        WorkerTask extraTask = queue.poll(
                700,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                extraTask,
                "После CREATE и MODIFY появилась лишняя задача"
        );
    }

    @Test
    void modifyThenDeleteThenCreateShouldCancelStaleModify() throws InterruptedException {
        FileTask modifiedTask = new FileTask(path, ChangeType.MODIFIED);

        debounce.debounceOnModify(modifiedTask);
        debounce.debounceCancel(path);

        FileTask deletedTask = new FileTask(path, ChangeType.DELETED);

        router.route(deletedTask);

        WorkerTask result = queue.poll(
                100,
                TimeUnit.MILLISECONDS
        );

        FileTask deletedResult = assertInstanceOf(
                FileTask.class,
                result,
                "Пришел другой файл"
        );

        assertEquals(path, deletedResult.path());
        assertEquals(ChangeType.DELETED, deletedResult.changeType());

        FileTask createdTask = new FileTask(path, ChangeType.CREATED);

        router.route(createdTask);

        result = queue.poll(
                700,
                TimeUnit.MILLISECONDS
        );

        FileTask createdResult = assertInstanceOf(
                FileTask.class,
                result,
                "Пришел другой файл"
        );

        assertEquals(path, createdResult.path());
        assertEquals(ChangeType.CREATED, createdResult.changeType());

        result = queue.poll(
                700,
                TimeUnit.MILLISECONDS
        );

        assertNull(result, "В очереди есть еще объекты");

    }

    @Test
    void shutdownShouldCancelPendingModify() throws InterruptedException {
        debounce.debounceOnModify(fileTask);

        debounce.shutdown();

        WorkerTask result = queue.poll(
                700,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                result,
                "После shutdown ожидающий MODIFY попал в очередь"
        );
    }

    @Test
    void modifyAfterShutdownShouldNotBeAccepted() throws InterruptedException {
        debounce.shutdown();

        debounce.debounceOnModify(fileTask);

        WorkerTask result = queue.poll(
                700,
                TimeUnit.MILLISECONDS
        );

        assertNull(
                result,
                "Debounce принял новый MODIFY после shutdown"
        );
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(
                2,
                TimeUnit.SECONDS
        );
    }

    private static final class BlockingFirstRouteTaskRouter
            extends TaskRouter {

        private final CountDownLatch firstRouteStarted = new CountDownLatch(1);
        private final CountDownLatch allowFirstRouteToFinish = new CountDownLatch(1);
        private final AtomicInteger routeCalls = new AtomicInteger();

        public BlockingFirstRouteTaskRouter(List<BlockingQueue<WorkerTask>> queues) {
            super(queues);
        }

        @Override
        public void route(FileTask fileTask) throws InterruptedException {
            int callNumber = routeCalls.incrementAndGet();
            if (callNumber == 1) {
                firstRouteStarted.countDown();
                allowFirstRouteToFinish.await();
            }
            super.route(fileTask);
        }
    }
}
