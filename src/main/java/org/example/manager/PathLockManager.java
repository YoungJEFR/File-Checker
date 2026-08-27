package org.example.manager;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PathLockManager {
    private final ConcurrentHashMap<Path, ReentrantLock> locks = new ConcurrentHashMap<Path, ReentrantLock>();
    public ReentrantLock getLock(Path path) {
        return locks.computeIfAbsent(
                path,
                k -> new ReentrantLock()
        );
    }
}
