package org.example.watcher;

import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.BlockingQueue;

public class FileWatcher implements Runnable {
    private final Path path;
    private final FileChangeDebounce debounce;
    private final TaskRouter taskRouter;

    public FileWatcher(
            Path path,
            FileChangeDebounce debounce,
            TaskRouter taskRouter
    ) {
        this.path = path;
        this.debounce = debounce;
        this.taskRouter = taskRouter;
    }

    public void watch() throws IOException, InterruptedException {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            path.register(
                    watcher,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY
            );

            while (true) {
                WatchKey key = watcher.take();

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    Path changedPath = (Path) event.context();
                    Path fullPath = path.resolve(changedPath);

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
                    break;
                }
            }
        }
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
