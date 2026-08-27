package org.example.processor;

import org.example.model.FileInfo;
import org.example.model.FileTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileProcessor {
    public static FileInfo process(FileTask fileTask) throws IOException {
        Path path = fileTask.path();
        return new FileInfo(
                Files.getLastModifiedTime(path),
                path.getFileName().toString(),
                Files.size(path),
                path
        );
    }
}