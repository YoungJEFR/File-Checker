package org.example.model;

import java.nio.file.Path;
import java.util.Map;

public record FileIndexerSnapshot(
        Map<Path, FileInfo> files,
        int countFiles,
        long totalBytes,
        int errorCount
) {
}
