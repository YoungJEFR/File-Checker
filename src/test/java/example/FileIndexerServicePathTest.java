package example;

import org.example.model.FileIndexerSnapshot;
import org.example.service.FileIndexerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Timeout(15)
class FileIndexerServicePathTest {
    @TempDir
    Path tempDir;

    @Test
    void relativeRootShouldProduceAbsoluteNormalizedIndexKeys() throws Exception {
        Path file = Files.write(tempDir.resolve("initial.md"), new byte[10]);
        Path relativeRoot = relativeRoot();
        assertFalse(relativeRoot.isAbsolute());

        try (FileIndexerService service = new FileIndexerService(2, relativeRoot, 3)) {
            service.start();

            assertSingleFile(service.snapshot(), file, 10);
        }
    }

    @Test
    void rootContainingParentSegmentsShouldProduceNormalizedIndexKeys() throws Exception {
        Path file = Files.write(tempDir.resolve("initial.md"), new byte[20]);
        Path child = Files.createDirectory(tempDir.resolve("child"));
        Path rootWithParentSegment = child.resolve("..");
        assertNotEquals(rootWithParentSegment, rootWithParentSegment.normalize());

        try (FileIndexerService service =
                     new FileIndexerService(2, rootWithParentSegment, 3)) {
            service.start();

            assertSingleFile(service.snapshot(), file, 20);
        }
    }

    @Test
    void watcherWithRelativeRootShouldAlsoProduceAbsoluteNormalizedKeys() throws Exception {
        Path relativeRoot = relativeRoot();

        try (FileIndexerService service = new FileIndexerService(2, relativeRoot, 3)) {
            service.start();
            // Создаём после start(), чтобы файл нашёл watcher, а не initial scan.
            Path file = Files.write(tempDir.resolve("created-later.md"), new byte[30]);
            Path expected = file.toAbsolutePath().normalize();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            FileIndexerSnapshot snapshot = service.snapshot();

            while (System.nanoTime() < deadline
                    && !(snapshot.files().keySet().equals(Set.of(expected))
                    && snapshot.countFiles() == 1 && snapshot.totalBytes() == 30)) {
                Thread.sleep(20);
                snapshot = service.snapshot();
            }

            assertSingleFile(snapshot, file, 30);
        }
    }

    private Path relativeRoot() {
        Path currentDirectory = Path.of("").toAbsolutePath().normalize();
        Path absoluteRoot = tempDir.toAbsolutePath().normalize();
        // Windows не позволяет построить относительный путь между разными дисками.
        assumeTrue(currentDirectory.getRoot().equals(absoluteRoot.getRoot()),
                "Рабочая и временная папки должны находиться на одном диске");
        return currentDirectory.relativize(absoluteRoot);
    }

    private static void assertSingleFile(FileIndexerSnapshot snapshot, Path file, long size) {
        Path expected = file.toAbsolutePath().normalize();
        assertEquals(Set.of(expected), snapshot.files().keySet(),
                "В индексе должен быть только абсолютный нормализованный ключ");
        assertEquals(expected, snapshot.files().get(expected).path());
        assertEquals(1, snapshot.countFiles());
        assertEquals(size, snapshot.totalBytes());
        assertEquals(0, snapshot.errorCount());
    }
}
