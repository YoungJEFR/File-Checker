package org.example.pipeline;

import org.example.model.ChangeType;
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

                if (fileTask.changeType() == ChangeType.DELETED) {
                    FileInfo removed = indexMap.remove(fileTask.path());

                    if (removed != null) {
                        filesStat.getCountByteFiles().add(-removed.fileSize());
                        filesStat.getCountFiles().decrementAndGet();
                    }

                    continue;
                }
                if (fileTask.changeType() == ChangeType.MODIFIED) {
                    FileInfo oldFile = indexMap.get(fileTask.path());
                    FileInfo newFile = FileProcessor.process(fileTask);

                    if (oldFile != null) {
                        long difference = newFile.fileSize() - oldFile.fileSize();
                        filesStat.getCountByteFiles().add(difference);
                    }

                    indexMap.put(newFile.path(), newFile);

                    continue;
                }
                if (fileTask.changeType() == ChangeType.CREATED) {
                    FileInfo fileInfo = FileProcessor.process(fileTask);

                    FileInfo old = indexMap.put(fileInfo.path(), fileInfo);

                    if (old == null) {
                        filesStat.getCountFiles().incrementAndGet();
                        filesStat.getCountByteFiles().add(fileInfo.fileSize());
                    }
                }
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
