package org.example.processor;

import org.example.model.FileInfo;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileIndex {
    private final ConcurrentHashMap<Path, FileInfo> indexMap;

    public FileIndex() {
        indexMap = new ConcurrentHashMap<>();
    }

    public FileInfo getFileInfo(Path path) {
        return indexMap.get(path);
    }

    public Set<Path> snapshotPaths() {
        return Set.copyOf(indexMap.keySet());
    }

    public Map<Path, FileInfo> snapshotMap() {
        return Map.copyOf(indexMap);
    }

    public FileInfo addToMap(FileInfo fileInfo) {
        return indexMap.put(fileInfo.path(), fileInfo);
    }

    public FileInfo deleteInMap(Path path) {
        return indexMap.remove(path);
    }
}
