package example;

import org.example.model.FileIndexerSnapshot;
import org.example.service.FileIndexerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileIndexerServiceSnapshotTest {
    @TempDir
    Path tempDir;

    @Test
    void snapshotShouldContainInitialFilesAndStatistics()
            throws Exception {
        Path file = Files.writeString(
                tempDir.resolve("test.md"),
                "Hello"
        );

        FileIndexerService service =
                new FileIndexerService(3, tempDir, 3);

        try {
            service.start();

            FileIndexerSnapshot snapshot = service.snapshot();

            assertEquals(1, snapshot.countFiles());
            assertEquals(Files.size(file), snapshot.totalBytes());
            assertEquals(0, snapshot.errorCount());
            assertTrue(snapshot.files().containsKey(file));
        } finally {
            service.shutdown();
        }
    }
}
