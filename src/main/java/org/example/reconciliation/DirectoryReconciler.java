package org.example.reconciliation;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.processor.FileIndex;
import org.example.route.TaskRouter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class DirectoryReconciler {
    private final FileScanner scanner;
    private final FileIndex index;
    private final TaskRouter router;

    public DirectoryReconciler(
            FileScanner scanner,
            FileIndex index,
            TaskRouter router
    ) {
        this.scanner = scanner;
        this.index = index;
        this.router = router;
    }

    public void reconcile(Path directory)
            throws IOException, InterruptedException {
        Set<Path> pathsToReconcile = new HashSet<>();
        scanner.scanFile(directory, pathsToReconcile::add);

        Set<Path> indexedPaths = index.snapshotPaths();

        for (Path indexedPath : indexedPaths) {
            if (indexedPath.startsWith(directory)) {
                pathsToReconcile.add(indexedPath);
            }
        }

        for (Path newSnapshotPath : pathsToReconcile) {
            router.route(new FileTask(
                    newSnapshotPath,
                    ChangeType.MODIFIED,
                    TaskSource.RECONCILIATION
            ));
        }

    }

}
