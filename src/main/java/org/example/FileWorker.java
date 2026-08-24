package org.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

public class FileWorker implements Runnable {

    private final BlockingQueue<FileTask> queue;

    public FileWorker(BlockingQueue<FileTask> queue) {
        this.queue = queue;
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

                System.out.println(
                        "Поток: "
                                + Thread.currentThread().getName()
                                + " | Обработал: "
                                + fileInfo
                );

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
