package org.example.route;

import org.example.model.FileTask;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TaskRouter {
    private final List<BlockingQueue<FileTask>> queues;

    public TaskRouter(List<BlockingQueue<FileTask>> queues) {
        if (queues.isEmpty()) {
            throw new IllegalArgumentException("queues must not be empty");
        }

        this.queues = List.copyOf(queues);
    }

    public void route(FileTask fileTask) throws InterruptedException {
        Path path = fileTask.path();
        int index = Math.floorMod(path.hashCode(), queues.size());
        queues.get(index).put(fileTask);
    }
}
