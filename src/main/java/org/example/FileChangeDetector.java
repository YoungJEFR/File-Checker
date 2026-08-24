package org.example;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileChangeDetector {

    public List<FileChange> detectChanges(
            List<FileInfo> oldFiles,
            List<FileInfo> newFiles
    ) {
        List<FileChange> fileChanges = new ArrayList<>();

        Map<Path, FileInfo> oldFilesMap = new HashMap<>();
        Map<Path, FileInfo> newFilesMap = new HashMap<>();
        for (FileInfo oldFile : oldFiles) {
            oldFilesMap.putIfAbsent(oldFile.path(), oldFile);
        }
        for (FileInfo newFile : newFiles) {
            newFilesMap.putIfAbsent(newFile.path(), newFile);
        }

        for (FileInfo newFile : newFilesMap.values())  {
            if (!oldFilesMap.containsKey(newFile.path())) {
                fileChanges.add(new FileChange(newFile.path(), ChangeType.CREATED));
            } else if ((newFile.fileSize() != oldFilesMap.get(newFile.path()).fileSize()) ||
                    (!newFile.
                            lastModified().
                            equals(oldFilesMap.
                                    get(newFile.path()).lastModified()))) {
                fileChanges.add(new FileChange(newFile.path(), ChangeType.MODIFIED));
            }
        }

        for (FileInfo oldFile : oldFilesMap.values()) {
            if (!newFilesMap.containsKey(oldFile.path())) {
                fileChanges.add(new FileChange(oldFile.path(), ChangeType.DELETED));
            }
        }

        return fileChanges;
    }
}
