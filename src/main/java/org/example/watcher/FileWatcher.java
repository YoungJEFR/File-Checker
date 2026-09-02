package org.example.watcher;

import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class FileWatcher implements Runnable {
    private final Path path;
    private final FileChangeDebounce debounce;
    private final TaskRouter taskRouter;

    private final Map<WatchKey, Path> fileWatchers = new ConcurrentHashMap<>();

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
            registerAllDirectory(path, watcher);

            while (true) {
                WatchKey key = watcher.take();
                Path directory = fileWatchers.get(key);


                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    Path changedPath = (Path) event.context();
                    Path fullPath = directory.resolve(changedPath);

                    if (kind ==  StandardWatchEventKinds.ENTRY_CREATE) {
                        if(Files.isDirectory(fullPath)) {
                           registerAllDirectory(fullPath, watcher);
                            continue;
                        }
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
                    fileWatchers.remove(key);

                    if (fileWatchers.isEmpty()) {
                        break;
                    }
                }
            }
        }
    }

    public void registerAllDirectory(Path path, WatchService watcher) throws IOException {
        List<Path> files;
        try (Stream<Path> getAllDirectory = Files.walk(path)) {
            files = getAllDirectory
                    .filter(Files::isDirectory)
                    .toList();

            for (Path pathFile : files) {
                WatchKey key = pathFile.register(
                        watcher,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE,
                        StandardWatchEventKinds.ENTRY_MODIFY
                );
                fileWatchers.put(key, pathFile);
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
