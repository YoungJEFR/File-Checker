package org.example;

import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

public record FileInfo(
        FileTime lastModified,
        String fileName,
        long fileSize,
        Path path
) {
    @Override
    public String toString() {
        return "file name: " + fileName + ", file size: " + fileSize + ", last modified: " + lastModified;
    }
}
