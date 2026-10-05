package example;

import org.example.model.FilesStat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilesStatTest {
    @Test
    void fileAddedShouldIncreaseFileCountAndTotalBytes() {
        FilesStat filesStat = new FilesStat();

        filesStat.fileAdded(10);

        assertEquals(1, filesStat.countFiles());
        assertEquals(10, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
    }

    @Test
    void fileRemovedShouldDecreaseFileCountAndTotalBytes() {
        FilesStat filesStat = new FilesStat();

        filesStat.fileAdded(10);
        filesStat.fileRemoved(10);

        assertEquals(0, filesStat.countFiles());
        assertEquals(0, filesStat.totalBytes());
        assertEquals(0, filesStat.countError());
    }

    @Test
    void fileSizeChangedShouldUpdateBytesWithoutChangingFileCount() {
        FilesStat filesStat = new FilesStat();
        filesStat.fileAdded(10);

        filesStat.fileSizeChanged(5);
        assertEquals(1, filesStat.countFiles());
        assertEquals(15, filesStat.totalBytes());

        filesStat.fileSizeChanged(-7);
        assertEquals(8, filesStat.totalBytes());
        assertEquals(1, filesStat.countFiles());
    }

    @Timeout(5)
    @Test
    void concurrentFileAddedShouldNotLoseFileCountOrBytes()
            throws InterruptedException {
        FilesStat filesStat = new FilesStat();
        CountDownLatch readyLatch = new CountDownLatch(3);
        CountDownLatch startLatch = new CountDownLatch(1);
        Runnable runnable = () -> {
            readyLatch.countDown();
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            for (int i = 0; i < 1000; i++) {
                filesStat.fileAdded(10);
            }
        };
        Thread[] threads = {
                new Thread(runnable, "FilesStatTest-1"),
                new Thread(runnable, "FilesStatTest-2"),
                new Thread(runnable, "FilesStatTest-3")
        };
        try {
            for (Thread thread : threads) {
                thread.start();
            }

            readyLatch.await();
            startLatch.countDown();

            for (Thread thread : threads) {
                thread.join();
            }

            assertEquals(3000, filesStat.countFiles());
            assertEquals(30000, filesStat.totalBytes());
            assertEquals(0, filesStat.countError());
        } finally {
            startLatch.countDown();
            for (Thread thread : threads) {
                thread.interrupt();
            }

            boolean wasInterrupted = Thread.interrupted();
            for (Thread thread : threads) {
                while (thread.isAlive()) {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        wasInterrupted = true;
                    }
                }
            }
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void recordErrorShouldIncreaseErrorCountWithoutChangingFileStats() {
        FilesStat filesStat = new FilesStat();
        filesStat.recordError();
        filesStat.recordError();

        assertEquals(0, filesStat.countFiles());
        assertEquals(0, filesStat.totalBytes());
        assertEquals(2, filesStat.countError());
    }
}
