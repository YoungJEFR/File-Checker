package org.example.watcher;

import java.io.IOException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
public final class WatchRegistrar {

    private final Map<WatchKey, Path> registeredDirectories;

    public WatchRegistrar(Map<WatchKey, Path> registeredDirectories) {
        this.registeredDirectories = registeredDirectories;
    }

    public void registerRecursively(
            Path root,
            WatchService watchService
    ) throws IOException {

        try (Stream<Path> paths = Files.walk(root)) {
            Iterator<Path> directories = paths
                    .filter(Files::isDirectory)
                    .iterator();

            while (directories.hasNext()) {
                register(directories.next(), watchService);
            }
        }
    }

    private void register(
            Path directory,
            WatchService watchService
    ) throws IOException {

        WatchKey key = directory.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY
        );

        registeredDirectories.put(key, directory);
    }

    public Path directoryFor(WatchKey key) {
        return registeredDirectories.get(key);
    }

    public void remove(WatchKey key) {
        registeredDirectories.remove(key);
    }

    public boolean isEmpty() {
        return registeredDirectories.isEmpty();
    }
}