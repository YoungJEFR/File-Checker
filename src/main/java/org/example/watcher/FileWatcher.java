package org.example.watcher;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class FileWatcher implements Runnable {
    private final Path path;
    private final FileChangeDebounce debounce;
    private final TaskRouter taskRouter;
    private final WatchRegistrar watchRegistrar;
    private final ExecutorService executorService;
    private final FileScanner scanner;

    public FileWatcher(
            Path path,
            FileChangeDebounce debounce,
            TaskRouter taskRouter,
            WatchRegistrar watchRegistrar,
            ExecutorService executorService,
            FileScanner scanner
    ) {
        this.path = path;
        this.debounce = debounce;
        this.taskRouter = taskRouter;
        this.watchRegistrar = watchRegistrar;
        this.executorService = executorService;
        this.scanner = scanner;
    }

    public void watch() throws IOException, InterruptedException {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            watchRegistrar.registerRecursively(path, watcher);

            while (true) {
                WatchKey key = watcher.take();
                Path directory = watchRegistrar.directoryFor(key);

                if (directory == null) {
                    key.reset();
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    Path changedPath = (Path) event.context();
                    Path fullPath = directory.resolve(changedPath);

                    if (kind ==  StandardWatchEventKinds.ENTRY_CREATE
                    && Files.isDirectory(fullPath)) {
                        watchRegistrar.registerRecursively(fullPath, watcher);
                        requestRescan(fullPath);
                        continue;
                    }
                    if (fullPath.toString().endsWith(".md")) {
                        if (kind == StandardWatchEventKinds.ENTRY_MODIFY ) {
                            debounce.debounceOnModify(new FileTask(fullPath, ChangeType.MODIFIED));
                            continue;
                        }

                        System.out.println(
                                String.format("File changed: %s", changedPath.toString())
                        );

                        ChangeType changeType;
                        if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                            changeType = ChangeType.CREATED;
                        } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                            changeType = ChangeType.DELETED;
                            debounce.debounceCancel(fullPath);
                        } else {
                            continue;
                        }

                        taskRouter.route(new FileTask(fullPath, changeType));
                    }
                }

                if(!key.reset()){
                    watchRegistrar.remove(key);

                    if (watchRegistrar.isEmpty()) {
                        break;
                    }
                }
            }
        }
    }

    private void requestRescan(Path path) {
        executorService.submit(() -> {
            try {
                scanner.scanFile(
                        path,
                        path1 -> taskRouter.route(
                                new FileTask(path1, ChangeType.MODIFIED)
                        )

                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e){
                e.printStackTrace();
            }
        });
    }

    @Override
    public void run() {
        try {
            watch();
        }catch (InterruptedException t) {
            Thread.currentThread().interrupt();
        } catch (java.io.IOException e){
            e.printStackTrace();
        }
    }
}
