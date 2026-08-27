package org.example.pipeline;

import org.example.model.ChangeType;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class FileWorker implements Runnable {

    private final BlockingQueue<FileTask> queue;
    private final FilesStat filesStat;
    private final FileIndex fileIndex;

    public FileWorker(
            BlockingQueue<FileTask> queue,
            FilesStat filesStat,
            FileIndex fileIndex
    ) {
        this.queue = queue;
        this.filesStat = filesStat;
        this.fileIndex = fileIndex;
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

                    FileInfo removed =
                            fileIndex.deleteInMap(fileTask.path());

                    if (removed != null) {
                        filesStat.getCountFiles().decrementAndGet();
                        filesStat.getCountByteFiles()
                                .add(-removed.fileSize());
                    }

                    continue;
                }

                if (fileTask.changeType() == ChangeType.CREATED
                        || fileTask.changeType() == ChangeType.MODIFIED) {

                    FileInfo newFile =
                            FileProcessor.process(fileTask);

                    FileInfo oldFile =
                            fileIndex.addToMap(newFile);

                    if (oldFile == null) {

                        filesStat.getCountFiles()
                                .incrementAndGet();

                        filesStat.getCountByteFiles()
                                .add(newFile.fileSize());

                    } else {

                        long difference =
                                newFile.fileSize()
                                        - oldFile.fileSize();

                        filesStat.getCountByteFiles()
                                .add(difference);
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