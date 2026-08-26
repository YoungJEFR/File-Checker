package org.example.model;

import java.io.File;
import java.nio.file.Path;

public record FileTask(
        Path path,
        ChangeType changeType
) {
    public FileTask(Path path) {
        this(path, ChangeType.CREATED);
    }
}
