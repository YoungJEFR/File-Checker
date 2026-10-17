package example;

import org.example.Main;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Каждый тест запускает настоящий Main в отдельной JVM, не подменяя System.in/out тестового процесса. */
@Timeout(20)
class MainProcessTest {
    private static final long WAIT_SECONDS = 5;

    @TempDir
    Path tempDir;

    @Test
    void immediateEofShouldExitWithoutStartingService() throws Exception {
        String output = runToEof("");

        assertTrue(output.contains("Введите путь к папке:"));
        assertFalse(output.contains("Directory watcher started"));
        assertFalse(output.contains("Итоговое состояние:"));
    }

    @Test
    void blankLinesShouldRepeatPromptWithoutIndexingWorkingDirectory() throws Exception {
        String output = runToEof("\n   \n");

        assertEquals(3, occurrences(output, "Введите путь к папке:"));
        assertFalse(output.contains("Directory watcher started"));
    }

    @Test
    void invalidPathShouldNotPreventStartingNextValidDirectory() throws Exception {
        Path root = directoryWithFile("valid", "valid.md");
        // NUL недопустим в Path и на Windows, и на Unix.
        String output = runToEof("bad\u0000path\n" + root + "\n");

        assertTrue(occurrences(output, "Введите путь к папке:") >= 2);
        assertEquals(1, occurrences(output, "Directory watcher started"));
        assertTrue(output.contains("valid.md"));
        assertEquals(1, occurrences(output, "Итоговое состояние:"));
    }

    @Test
    void missingDirectoryShouldNotPreventStartingNextValidDirectory() throws Exception {
        Path root = directoryWithFile("valid", "found.md");
        String output = runToEof(tempDir.resolve("missing") + "\n" + root + "\n");

        assertTrue(output.contains("Папка не найдена"));
        assertEquals(1, occurrences(output, "Directory watcher started"));
        assertTrue(output.contains("found.md"));
        assertEquals(1, occurrences(output, "Итоговое состояние:"));
    }

    @Test
    void eofWhileServiceIsRunningShouldShutdownAndExit() throws Exception {
        Path root = directoryWithFile("watched", "note.md");
        try (MainProcess main = new MainProcess()) {
            main.awaitLine("Введите путь к папке:");
            main.sendLine(root.toString());
            main.awaitLine("=====================");
            assertTrue(main.process.isAlive(), "Main должен ждать команды после initial scan");

            main.closeInput();
            String output = main.awaitExit();

            assertTrue(output.contains("note.md"));
            assertEquals(1, occurrences(output, "Итоговое состояние:"));
        }
    }

    @Test
    void enterShouldStopSessionAndAllowAnotherDirectory() throws Exception {
        Path first = directoryWithFile("first", "first.md");
        Path second = directoryWithFile("second", "second.md");
        try (MainProcess main = new MainProcess()) {
            main.awaitLine("Введите путь к папке:");
            main.sendLine(first.toString());
            main.awaitLine("=====================");

            main.sendLine("");
            main.awaitLine("Итоговое состояние:");
            main.awaitLine("Введите путь к папке:");
            main.sendLine(second.toString());
            main.awaitLine("second.md");
            main.awaitLine("=====================");
            assertTrue(main.process.isAlive(), "Второй сервис должен продолжать работать");

            main.closeInput();
            String output = main.awaitExit();

            assertEquals(2, occurrences(output, "Directory watcher started"));
            assertEquals(2, occurrences(output, "Итоговое состояние:"));
            assertFalse(output.contains("IllegalThreadStateException"),
                    "Поток консоли нельзя запускать повторно");
        }
    }

    @Test
    void losingRootShouldPromptForNewPathWithoutWaitingForEnter() throws Exception {
        Path lost = Files.createDirectory(tempDir.resolve("lost"));
        Path next = directoryWithFile("next", "next.md");
        try (MainProcess main = new MainProcess()) {
            main.awaitLine("Введите путь к папке:");
            main.sendLine(lost.toString());
            main.awaitLine("=====================");

            Files.delete(lost);
            // После удаления ничего не вводим: Main должен проснуться от события сервиса.
            main.awaitLine("Сервис заверш");
            main.awaitLine("Введите путь к папке:");
            assertTrue(main.process.isAlive());
            main.sendLine(next.toString());
            main.awaitLine("next.md");
            main.awaitLine("=====================");

            main.closeInput();
            String output = main.awaitExit();

            assertTrue(output.contains("RootWatchLostException"));
            assertEquals(2, occurrences(output, "Directory watcher started"));
            assertEquals(2, occurrences(output, "Итоговое состояние:"));
        }
    }

    private Path directoryWithFile(String directoryName, String fileName) throws IOException {
        Path root = Files.createDirectory(tempDir.resolve(directoryName));
        Files.write(root.resolve(fileName), new byte[10]);
        return root;
    }

    private static String runToEof(String input) throws Exception {
        try (MainProcess main = new MainProcess()) {
            main.input.write(input);
            main.closeInput();
            return main.awaitExit();
        }
    }

    private static int occurrences(String text, String fragment) {
        return text.split(java.util.regex.Pattern.quote(fragment), -1).length - 1;
    }

    private static final class MainProcess implements AutoCloseable {
        private final Process process;
        private final BufferedWriter input;
        private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        private final StringBuffer output = new StringBuffer();
        private final AtomicReference<Throwable> readFailure = new AtomicReference<>();
        private final Thread outputReader;

        private MainProcess() throws Exception {
            String javaName = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
            Path java = Path.of(System.getProperty("java.home"), "bin", javaName);
            Path classes = Path.of(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            process = new ProcessBuilder(java.toString(),
                    "-Dfile.encoding=UTF-8", "-Dstdin.encoding=UTF-8",
                    "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8",
                    "-cp", classes.toString(), Main.class.getName())
                    .redirectErrorStream(true).start();
            input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            outputReader = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append('\n');
                        lines.add(line);
                    }
                } catch (Throwable failure) {
                    readFailure.set(failure);
                }
            }, "MainTestOutputReader");
            outputReader.setDaemon(true);
            outputReader.start();
        }

        private void sendLine(String line) throws IOException {
            input.write(line);
            input.newLine();
            input.flush();
        }

        private void closeInput() throws IOException {
            input.close(); // Для дочернего Main это EOF, а не закрытие System.in JUnit.
        }

        private void awaitLine(String fragment) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    fail("Не получено сообщение: " + fragment + "\n" + output);
                }
                String line = lines.poll(remaining, TimeUnit.NANOSECONDS);
                assertNotNull(line, () -> "Не получено сообщение: " + fragment + "\n" + output);
                if (line.contains(fragment)) {
                    return;
                }
            }
        }

        private String awaitExit() throws InterruptedException {
            assertTrue(process.waitFor(WAIT_SECONDS, TimeUnit.SECONDS),
                    () -> "Main не завершился\n" + output);
            outputReader.join(2_000);
            assertFalse(outputReader.isAlive(), "Не завершилось чтение вывода Main");
            assertNull(readFailure.get(), "Ошибка чтения вывода Main");
            assertEquals(0, process.exitValue(), () -> "Main завершился с ошибкой\n" + output);
            return output.toString();
        }

        @Override
        public void close() throws Exception {
            if (process.isAlive()) {
                // При падении проверки не оставляем дочернюю JVM с живыми worker-потоками.
                process.destroyForcibly();
                assertTrue(process.waitFor(WAIT_SECONDS, TimeUnit.SECONDS));
            }
            input.close();
            outputReader.join(2_000);
            process.getInputStream().close();
        }
    }
}
