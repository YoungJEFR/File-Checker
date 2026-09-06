package org.example.pipeline;

import org.example.model.*;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.recovery.FileRecoveryCoordinator;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.BlockingQueue;

public class FileWorker implements Runnable {
    private final BlockingQueue<WorkerTask> queue;
    private final FilesStat filesStat;
    private final FileIndex fileIndex;
    private final FileRecoveryCoordinator fileRecoveryCoordinator;


    public FileWorker(BlockingQueue<WorkerTask> queue, FilesStat filesStat, FileIndex fileIndex, FileRecoveryCoordinator fileRecoveryCoordinator) {
        this.queue = queue;
        this.filesStat = filesStat;
        this.fileIndex = fileIndex;
        this.fileRecoveryCoordinator = fileRecoveryCoordinator;
    }

    @Override
    public void run() {
        while (true) {
            WorkerTask workerTask;

            try {
                workerTask = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (workerTask instanceof StopTask) {
                return;
            }

            if (!(workerTask instanceof FileTask fileTask)) {
                continue;
            }

            try {
                if (fileTask.changeType() == ChangeType.DELETED) {
                    deleteIndexInMap(fileIndex, fileTask, filesStat, fileRecoveryCoordinator);
                    continue;
                }

                if (fileTask.changeType() == ChangeType.CREATED || fileTask.changeType() == ChangeType.MODIFIED) {
                    FileInfo newFile = FileProcessor.process(fileTask);
                    FileInfo oldFile = fileIndex.addToMap(newFile);
                    if (oldFile == null) {
                        filesStat.getCountFiles().incrementAndGet();
                        filesStat.getCountByteFiles().add(newFile.fileSize());
                    } else {
                        long difference = newFile.fileSize() - oldFile.fileSize();
                        filesStat.getCountByteFiles().add(difference);
                    }
                }

                fileRecoveryCoordinator.onSuccess(fileTask.path());
            } catch (NoSuchFileException e) {
                deleteIndexInMap(
                        fileIndex,
                        fileTask,
                        filesStat,
                        fileRecoveryCoordinator
                );
            } catch (IOException e) {
                boolean recoveryStarted = fileRecoveryCoordinator.onFailure(fileTask, e);

                if (recoveryStarted) {
                    filesStat.getErrorFiles().incrementAndGet();
                }
            }
        }
    }

    private static void deleteIndexInMap(
            FileIndex fileIndex,
            FileTask fileTask,
            FilesStat filesStat,
            FileRecoveryCoordinator fileRecoveryCoordinator
    ) {
        FileInfo removed = fileIndex.deleteInMap(fileTask.path());
        if (removed != null) {
            filesStat.getCountFiles().decrementAndGet();
            filesStat.getCountByteFiles().add(-removed.fileSize());
        }

        fileRecoveryCoordinator.onSuccess(fileTask.path());
    }
}