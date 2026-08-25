package org.example.model;

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
