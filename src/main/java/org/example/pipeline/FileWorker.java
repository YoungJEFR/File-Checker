package org.example.pipeline;

import org.example.model.FileInfo;
import org.example.model.FileTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class FileWorker implements Runnable {

    private final BlockingQueue<FileTask> queue;
    private final ConcurrentHashMap<Path, FileInfo> indexMap;


    public FileWorker(BlockingQueue<FileTask> queue, ConcurrentHashMap<Path, FileInfo> indexMap) {
        this.queue = queue;
        this.indexMap = indexMap;
    }

    @Override
    public void run() {

        while (true) {
            try {

                FileTask fileTask = queue.take();

                if (fileTask.path()
                        .getFileName()
                        .toString()
                        .equals("STOP")) {

                    return;
                }

                Path path = fileTask.path();

                FileInfo fileInfo = new FileInfo(
                        Files.getLastModifiedTime(path),
                        path.getFileName().toString(),
                        Files.size(path),
                        path
                );

                indexMap.put(path, fileInfo);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
