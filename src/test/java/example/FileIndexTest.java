package example;

import org.example.model.FileInfo;
import org.example.processor.FileIndex;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileIndexTest {

    @Test
    void snapshotPathsShouldNotChangeWithIndex() {
        FileIndex fileIndex = new FileIndex(new ConcurrentHashMap<>());
        Path firstPath = Path.of("a.md");
        Path secondPath = Path.of("b.md");

        fileIndex.addToMap(fileInfo(firstPath));
        Set<Path> snapshot = fileIndex.snapshotPaths();

        fileIndex.deleteInMap(firstPath);
        fileIndex.addToMap(fileInfo(secondPath));

        assertEquals(Set.of(firstPath), snapshot);
        assertEquals(Set.of(secondPath), fileIndex.snapshotPaths());
        assertThrows(
                UnsupportedOperationException.class,
                () -> snapshot.add(secondPath)
        );
    }

    private FileInfo fileInfo(Path path) {
        return new FileInfo(
                FileTime.fromMillis(0),
                path.getFileName().toString(),
                10,
                path
        );
    }
}
