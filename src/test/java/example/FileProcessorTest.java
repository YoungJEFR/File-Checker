package example;

import org.example.model.ChangeType;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.NotRegularFileException;
import org.example.processor.FileProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileProcessorTest {

    @TempDir
    private Path tempDir;

    @Test
    void processShouldRejectDirectoryNamedMarkdown()
            throws IOException {
        Path directory = Files.createDirectory(tempDir.resolve("folder.md"));
        FileTask fileTask = new FileTask(directory, ChangeType.CREATED);

        assertThrows(
                NotRegularFileException.class,
                () -> FileProcessor.process(fileTask)
        );
    }

    @Test
    void processShouldReturnMetadataForRegularFile()
            throws IOException {
        Path file = Files.write(tempDir.resolve("file.md"), new byte[10]);

        FileInfo info = FileProcessor.process(new FileTask(file, ChangeType.CREATED));


        assertEquals(
                file,
                info.path(),
                "Пути не совпадают"
        );

        assertEquals(
                10,
                info.fileSize()
        );

        assertEquals(
                file.getFileName().toString(),
                info.fileName()
        );

        assertEquals(
                Files.getLastModifiedTime(file),
                info.lastModified()
        );
    }

    @Test
    void replacingIndexedFileWithDirectoryShouldRemoveEntryWithoutRecovery() {

    }
}
