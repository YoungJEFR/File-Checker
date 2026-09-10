package example;

import org.example.filescanner.FileScanner;
import org.example.model.ChangeType;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.TaskSource;
import org.example.model.WorkerTask;
import org.example.processor.FileIndex;
import org.example.reconciliation.DirectoryReconciler;
import org.example.route.TaskRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectoryReconcilerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRouteUnionOfDiskAndIndexedPathsInsideDirectory()
            throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("target"));
        Path nestedDirectory = Files.createDirectory(
                directory.resolve("nested")
        );
        Path commonPath = Files.write(
                directory.resolve("common.md"),
                new byte[10]
        );
        Path diskOnlyPath = Files.write(
                nestedDirectory.resolve("disk-only.md"),
                new byte[20]
        );
        Path indexOnlyPath = directory.resolve("index-only.md");
        Path outsidePath = tempDir.resolve("outside.md");

        ConcurrentHashMap<Path, FileInfo> indexMap =
                new ConcurrentHashMap<>();
        FileIndex fileIndex = new FileIndex(indexMap);
        fileIndex.addToMap(fileInfo(commonPath));
        fileIndex.addToMap(fileInfo(indexOnlyPath));
        fileIndex.addToMap(fileInfo(outsidePath));

        BlockingQueue<WorkerTask> queue = new ArrayBlockingQueue<>(20);
        TaskRouter router = new TaskRouter(List.of(queue));
        DirectoryReconciler reconciler = new DirectoryReconciler(
                new FileScanner(),
                fileIndex,
                router
        );

        reconciler.reconcile(directory);

        List<WorkerTask> workerTasks = new ArrayList<>();
        queue.drainTo(workerTasks);
        List<FileTask> routedTasks = workerTasks.stream()
                .map(FileTask.class::cast)
                .toList();
        Set<Path> routedPaths = routedTasks.stream()
                .map(FileTask::path)
                .collect(Collectors.toSet());

        assertEquals(
                Set.of(commonPath, diskOnlyPath, indexOnlyPath),
                routedPaths
        );
        assertEquals(3, routedTasks.size(), "Один путь отправлен дважды");
        assertTrue(routedTasks.stream().allMatch(task ->
                task.changeType() == ChangeType.MODIFIED
                        && task.taskSource() == TaskSource.RECONCILIATION
        ));
    }

    private FileInfo fileInfo(Path path) {
        return new FileInfo(
                FileTime.fromMillis(0),
                path.getFileName().toString(),
                10,
                path
        );
    }
}
