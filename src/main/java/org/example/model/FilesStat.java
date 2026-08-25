package org.example.model;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class FilesStat  {
    private final LongAdder countByteFiles;
    private final AtomicInteger countFiles;
    private final AtomicInteger errorFiles;

    public FilesStat( AtomicInteger countFiles, AtomicInteger errorFiles, LongAdder countByteFiles) {
        this.countByteFiles = countByteFiles;
        this.countFiles = countFiles;
        this.errorFiles = errorFiles;
    }

    public LongAdder getCountByteFiles() {
        return countByteFiles;
    }

    public AtomicInteger getCountFiles() {
        return countFiles;
    }

    public AtomicInteger getErrorFiles() {
        return errorFiles;
    }
}

