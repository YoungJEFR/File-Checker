package org.example.model;

import java.nio.file.Path;
import java.util.Objects;

public record FileTask(
        Path path,
        ChangeType changeType,
        TaskSource taskSource
) implements WorkerTask {

    public FileTask {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(changeType, "changeType must not be null");
        Objects.requireNonNull(taskSource, "taskSource must not be null");
    }

    public FileTask(Path path, ChangeType changeType) {
        this(path, changeType, TaskSource.NORMAL);
    }

    public FileTask(Path path) {
        this(path, ChangeType.CREATED, TaskSource.NORMAL);
    }
}
