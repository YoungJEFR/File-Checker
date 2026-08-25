package org.example.pipeline;

import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.processor.FileProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class FileWorker implements Runnable {

    private final BlockingQueue<FileTask> queue;
    private final ConcurrentHashMap<Path, FileInfo> indexMap;
    private final FilesStat filesStat;

    public FileWorker(BlockingQueue<FileTask> queue, ConcurrentHashMap<Path, FileInfo> indexMap,  FilesStat filesStat) {
        this.queue = queue;
        this.indexMap = indexMap;
        this.filesStat = filesStat;
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

                FileInfo fileInfo = FileProcessor.process(fileTask);

                filesStat.getCountFiles().incrementAndGet();
                filesStat.getCountByteFiles().add(fileInfo.fileSize());
                indexMap.put(fileInfo.path(), fileInfo);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;

            } catch (IOException e) {
                e.printStackTrace();
                filesStat.getErrorFiles().incrementAndGet();
            }
        }
    }
}
