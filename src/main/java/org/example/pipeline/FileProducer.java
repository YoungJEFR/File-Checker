package org.example.pipeline;

import org.example.model.FileTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public class FileProducer implements Runnable{
    private final Path path;
    private BlockingQueue<FileTask> queue;

    public FileProducer(Path path, BlockingQueue<FileTask> queue) {
        this.path = path;
        this.queue = queue;
    }

    @Override
    public void run() {

        try (Stream<Path> stream = Files.walk(path)) {

            var iterator = stream
                    .filter(Files::isRegularFile)
                    .filter(path1 -> path1.toString().endsWith(".md"))
                    .iterator();

            while (iterator.hasNext()){
                Path file = iterator.next();

                if(Thread.currentThread().isInterrupted()){
                    return;
                }

                try{
                    queue.put(new FileTask(file));
                } catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    return;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
