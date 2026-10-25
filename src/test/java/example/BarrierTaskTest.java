package example;

import org.example.model.BarrierTask;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BarrierTaskTest {

    @Test
    void concurrentArrivalsShouldWaitForLastWorker() throws Exception {
        BarrierTask barrier = new BarrierTask(20);
        CompletableFuture<Void> completion = barrier.getCompletionFuture();
        CountDownLatch startGate = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(4);
        List<Future<?>> arrivals = new ArrayList<>();
        try {
            for (int i = 0; i < 19; i++) {
                arrivals.add(callers.submit(() -> {
                    startGate.await();
                    barrier.arrive();
                    return null;
                }));
            }
            startGate.countDown();
            for (Future<?> arrival : arrivals) {
                arrival.get(2, TimeUnit.SECONDS);
            }
            assertFalse(completion.isDone(), "Двадцатый Worker ещё не дошёл до барьера");

            barrier.arrive();

            assertNull(completion.get(1, TimeUnit.SECONDS));
        } finally {
            startGate.countDown();
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void shouldRejectNonPositiveWorkerCount() {
        assertThrows(IllegalArgumentException.class, () -> new BarrierTask(0));
        assertThrows(IllegalArgumentException.class, () -> new BarrierTask(-1));
    }

    @Test
    void shouldCompleteOnlyAfterAllWorkersArrive() throws Exception {
        BarrierTask barrier = new BarrierTask(3);
        CompletableFuture<Void> completion = barrier.getCompletionFuture();

        assertFalse(completion.isDone());
        barrier.arrive();
        assertFalse(completion.isDone());
        barrier.arrive();
        assertFalse(completion.isDone());
        barrier.arrive();

        assertNull(completion.get(1, TimeUnit.SECONDS));
    }

    @Test
    void barriersShouldHaveIndependentState() throws Exception {
        BarrierTask first = new BarrierTask(1);
        BarrierTask second = new BarrierTask(2);

        first.arrive();

        assertNull(first.getCompletionFuture().get(1, TimeUnit.SECONDS));
        assertFalse(second.getCompletionFuture().isDone());
        second.arrive();
        assertFalse(second.getCompletionFuture().isDone());
        second.arrive();
        assertNull(second.getCompletionFuture().get(1, TimeUnit.SECONDS));
    }

    @Test
    void completingReturnedFutureShouldNotReleaseBarrier() throws Exception {
        BarrierTask barrier = new BarrierTask(1);
        CompletableFuture<Void> returned = barrier.getCompletionFuture();

        returned.complete(null);

        CompletableFuture<Void> actualCompletion = barrier.getCompletionFuture();
        assertFalse(actualCompletion.isDone(),
                "Внешний код не должен завершать внутренний future барьера");
        barrier.arrive();
        assertNull(actualCompletion.get(1, TimeUnit.SECONDS));
    }
}
