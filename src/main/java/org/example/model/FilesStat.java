package org.example.model;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class FilesStat {
    private final LongAdder countByteFiles;
    private final AtomicInteger countFiles;
    private final AtomicInteger errorFiles;

    public FilesStat() {
        countByteFiles = new LongAdder();
        countFiles = new AtomicInteger(0);
        errorFiles = new AtomicInteger(0);
    }

    public void fileAdded(long fileSize) {
        countFiles.incrementAndGet();
        countByteFiles.add(fileSize);
    }

    public void fileSizeChanged(long difference) {
        countByteFiles.add(difference);
    }

    public void fileRemoved(long fileSize) {
        countFiles.decrementAndGet();
        countByteFiles.add(-fileSize);
    }

    public void recordError() {
        errorFiles.incrementAndGet();
    }

    public long totalBytes() {
        return countByteFiles.longValue();
    }

    public int countFiles() {
        return countFiles.intValue();
    }

    public int countError() {
        return errorFiles.intValue();
    }
}

