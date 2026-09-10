package example;

import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileTaskTest {

    private static final Path FILE = Path.of("test.md");

    @Test
    void shouldRejectNullPath() {
        assertThrows(
                NullPointerException.class,
                () -> new FileTask(
                        null,
                        ChangeType.CREATED,
                        TaskSource.NORMAL
                )
        );
    }

    @Test
    void shouldRejectNullChangeType() {
        assertThrows(
                NullPointerException.class,
                () -> new FileTask(
                        FILE,
                        null,
                        TaskSource.NORMAL
                )
        );
    }

    @Test
    void shouldRejectNullTaskSource() {
        assertThrows(
                NullPointerException.class,
                () -> new FileTask(
                        FILE,
                        ChangeType.CREATED,
                        null
                )
        );
    }

    @Test
    void convenienceConstructorsShouldSetExpectedDefaults() {
        FileTask createdTask = new FileTask(FILE);
        FileTask modifiedTask = new FileTask(
                FILE,
                ChangeType.MODIFIED
        );

        assertEquals(ChangeType.CREATED, createdTask.changeType());
        assertEquals(TaskSource.NORMAL, createdTask.taskSource());
        assertEquals(ChangeType.MODIFIED, modifiedTask.changeType());
        assertEquals(TaskSource.NORMAL, modifiedTask.taskSource());
    }
}
