package org.example.model;

import java.nio.file.Path;

public record FileTask(
        Path path,
        ChangeType changeType,
        TaskSource taskSource
) {
    public FileTask(Path path, ChangeType changeType) {
        this(path, changeType, TaskSource.NORMAL);
    }

    public FileTask(Path path) {
        this(path, ChangeType.CREATED, TaskSource.NORMAL);
    }
}
