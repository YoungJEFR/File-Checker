package org.example.processor;

import org.example.model.FileInfo;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class FileIndex {
    private final ConcurrentHashMap<Path, FileInfo> indexMap;

    public FileIndex(ConcurrentHashMap<Path, FileInfo> indexMap) {
        this.indexMap = indexMap;
    }

    public FileInfo getFileInfo(Path path) {
        return indexMap.get(path);
    }

    public FileInfo addToMap(FileInfo fileInfo) {
        return indexMap.put(fileInfo.path(), fileInfo);
    }

    public FileInfo deleteInMap(Path path) {
        return indexMap.remove(path);
    }
}
