package org.example.pipeline;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class FileProducer implements Runnable{
    private final Path path;
    private final TaskRouter taskRouter;
    private final FileScanner scanner;
    private IOException scanFailure;
    private final CompletableFuture<Void> completionFuture;


    public FileProducer(Path path, TaskRouter taskRouter, FileScanner scanner) {
        this.path = path;
        this.taskRouter = taskRouter;
        this.scanner = scanner;
        completionFuture = new CompletableFuture<>();
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
            completionFuture.complete(null);
        } catch (InterruptedException e){
            completionFuture.completeExceptionally(e);
            Thread.currentThread().interrupt();
            return;
        } catch (IOException e){
            e.printStackTrace();
            scanFailure = e;
            completionFuture.completeExceptionally(e);
        } catch (UncheckedIOException e){
            scanFailure = e.getCause();
            completionFuture.completeExceptionally(e.getCause());
        } catch (RuntimeException e) {
            completionFuture.completeExceptionally(e);
        }
    }

    public CompletableFuture<Void> getCompletionFuture() {
        return completionFuture.copy();
    }

    public IOException getScanFailure() {
        return scanFailure;
    }
}
