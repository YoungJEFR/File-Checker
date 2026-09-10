package example;

import org.example.model.BarrierTask;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class BarrierTaskTest {

    @Test
    void shouldRejectNullLatch() {
        assertThrows(
                NullPointerException.class,
                () -> new BarrierTask(null)
        );
    }
}
