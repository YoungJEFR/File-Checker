package org.example;

import java.nio.file.Path;

public record FileChange(
        Path path,
        ChangeType type
) {
    @Override
    public String toString() {
        return "FileChange{" +
                "path=" + path.getFileName() +
                ", type=" + type +
                '}';
    }
}
