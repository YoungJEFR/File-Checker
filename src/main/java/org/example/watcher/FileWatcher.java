package org.example.watcher;

import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.CountDownLatch;

public class FileWatcher implements Runnable {
    private final CountDownLatch watcherReady;
    private final Path path;
    private final FileChangeDebounce debounce;
    private final TaskRouter taskRouter;
    private final WatchRegistrar watchRegistrar;
    private final DirectoryReconciliationService reconciliationService;
    private IOException startupFailure;

    public FileWatcher(
            Path path,
            FileChangeDebounce debounce,
            TaskRouter taskRouter,
            WatchRegistrar watchRegistrar,
            DirectoryReconciliationService reconciliationService,
            CountDownLatch watcherReady
    ) {
        this.path = path;
        this.debounce = debounce;
        this.taskRouter = taskRouter;
        this.watchRegistrar = watchRegistrar;
        this.reconciliationService = reconciliationService;
        this.watcherReady = watcherReady;
    }

    public void watch() throws IOException, InterruptedException {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            watchRegistrar.registerRecursively(path, watcher);

            watcherReady.countDown();
            System.out.println("Directory watcher started, all directory is registered");

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
                        handleOverflow(directory);
                        continue;
                    }

                    Path changedPath = (Path) event.context();
                    Path fullPath = directory.resolve(changedPath);

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE
                            && Files.isDirectory(fullPath)) {
                        watchRegistrar.registerRecursively(fullPath, watcher);
                        requestRescan(fullPath);
                        continue;
                    }
                    if (fullPath.toString().endsWith(".md")) {
                        if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
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

                if (!key.reset()) {
                    watchRegistrar.remove(key);

                    if (watchRegistrar.isEmpty()) {
                        break;
                    }
                }
            }
        }
    }

    private void requestRescan(Path path) {
        reconciliationService.request(path);
    }

    void handleOverflow(Path directory) {
        System.err.println(
                "WatchService OVERFLOW, запускается сверка: " + directory
        );
        requestRescan(directory);
    }

    public IOException getStartupFailure() {
        return startupFailure;
    }

    @Override
    public void run() {
        try {
            watch();
        } catch (InterruptedException t) {
            Thread.currentThread().interrupt();
        } catch (java.io.IOException e) {
            e.printStackTrace();
            startupFailure = e;
            watcherReady.countDown();
        }
    }
}
