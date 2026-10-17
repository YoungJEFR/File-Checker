package org.example.processor;

import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.NotRegularFileException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class FileProcessor {
    public static FileInfo process(FileTask fileTask) throws IOException {
        Path path = fileTask.path();

        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            throw new NotRegularFileException("Not a regular file: " + path.toString());
        }
        return new FileInfo(
                attributes.lastModifiedTime(),
                path.getFileName().toString(),
                attributes.size(),
                path
        );
    }
}