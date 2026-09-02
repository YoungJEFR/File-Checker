package org.example.pipeline;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.Path;

public class FileProducer implements Runnable{
    private final Path path;
    private final TaskRouter taskRouter;
    private final FileScanner scanner;

    public FileProducer(Path path, TaskRouter taskRouter, FileScanner scanner) {
        this.path = path;
        this.taskRouter = taskRouter;
        this.scanner = scanner;
    }

    @Override
    public void run() {
        try {
            scanner.scanFile(
                    path,
                    path1 -> taskRouter.route(
                            new FileTask(path1, ChangeType.CREATED)
                    )
            );
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
            return;
        } catch (IOException e){
            e.printStackTrace();
        }

    }
}
