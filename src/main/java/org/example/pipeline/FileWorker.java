package org.example.pipeline;

import org.example.model.*;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.processor.FileTaskProcessor;
import org.example.recovery.FileRecoveryCoordinator;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

public class FileWorker implements Runnable {
    private final BlockingQueue<WorkerTask> queue;
    private final FilesStat filesStat;
    private final FileIndex fileIndex;
    private final FileRecoveryCoordinator fileRecoveryCoordinator;
    private final FileTaskProcessor fileTaskProcessor;
    private final Object indexStateLock;

    public FileWorker(
            BlockingQueue<WorkerTask> queue,
            FilesStat filesStat,
            FileIndex fileIndex,
            FileRecoveryCoordinator fileRecoveryCoordinator,
            FileTaskProcessor fileTaskProcessor,
            Object indexStateLock
    ) {
        this.queue = queue;
        this.filesStat = filesStat;
        this.fileIndex = fileIndex;
        this.fileRecoveryCoordinator = fileRecoveryCoordinator;
        this.fileTaskProcessor = Objects.requireNonNull(
                fileTaskProcessor,
                "fileTaskProcessor must not be null"
        );
        this.indexStateLock = Objects.requireNonNull(indexStateLock);
    }


    public FileWorker(
            BlockingQueue<WorkerTask> queue,
            FilesStat filesStat,
            FileIndex fileIndex,
            FileRecoveryCoordinator fileRecoveryCoordinator,
            Object indexStateLock
    ) {
        this(
                queue,
                filesStat,
                fileIndex,
                fileRecoveryCoordinator,
                FileProcessor::process,
                indexStateLock
        );
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

            if (workerTask instanceof BarrierTask(
                    java.util.concurrent.CountDownLatch latch
            )) {
                latch.countDown();
                continue;
            }

            if (!(workerTask instanceof FileTask fileTask)) {
                continue;
            }

            try {
                if (fileTask.changeType() == ChangeType.DELETED) {
                    deleteIndexInMap(
                            fileIndex,
                            fileTask,
                            filesStat,
                            fileRecoveryCoordinator,
                            indexStateLock
                    );

                    continue;
                }

                if (fileTask.changeType() == ChangeType.CREATED || fileTask.changeType() == ChangeType.MODIFIED) {
                    FileInfo newFile = fileTaskProcessor.process(fileTask);
                    synchronized (indexStateLock) {
                        FileInfo oldFile = fileIndex.addToMap(newFile);
                        if (oldFile == null) {
                            filesStat.fileAdded(newFile.fileSize());
                        } else {
                            long difference = newFile.fileSize() - oldFile.fileSize();
                            filesStat.fileSizeChanged(difference);
                        }
                    }
                }

                fileRecoveryCoordinator.onSuccess(fileTask.path());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (NoSuchFileException e) {
                deleteIndexInMap(
                        fileIndex,
                        fileTask,
                        filesStat,
                        fileRecoveryCoordinator,
                        indexStateLock
                );

            } catch (IOException e) {
                boolean recoveryStarted = fileRecoveryCoordinator.onFailure(fileTask, e);

                if (recoveryStarted) {
                    synchronized (indexStateLock) {
                        filesStat.recordError();
                    }
                }
            }
        }
    }


    private static void deleteIndexInMap(
            FileIndex fileIndex,
            FileTask fileTask,
            FilesStat filesStat,
            FileRecoveryCoordinator fileRecoveryCoordinator,
            Object  indexStateLock
    ) {
        synchronized (indexStateLock){
            FileInfo removed = fileIndex.deleteInMap(fileTask.path());
            if (removed != null) {
                filesStat.fileRemoved(removed.fileSize());
            }
        }

        fileRecoveryCoordinator.onSuccess(fileTask.path());
    }
}

