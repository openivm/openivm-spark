package org.openivm.spark.compiler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Dedicated, bounded-lifetime JVM for native CLI pipe readers.
 *
 * Capture files are opened only after the native process starts, so neither it
 * nor its descendants can inherit writable capture descriptors. A reader stuck
 * on an inherited pipe dies with this JVM instead of leaking in the Spark driver.
 */
final class OpenIvmCliWorker {
    static final int PROCESS_TIMEOUT_SECONDS = 120;
    static final int CLEANUP_TIMEOUT_SECONDS = 10;
    static final int PROCESS_TIMEOUT = 2;
    static final int OUTPUT_TIMEOUT = 3;
    static final int IO_FAILURE = 4;

    private OpenIvmCliWorker() {}

    public static void main(String[] args) throws IOException {
        int status;
        Trace trace = new Trace(args.length > 6 && "trace-v1".equals(args[6]));
        Trace.Mark total = trace.start();
        boolean success = false;
        try {
            status = run(args, trace);
            success = status == 0;
        } catch (IOException | ExecutionException failure) {
            Files.write(Paths.get(args[4]), failure.toString().getBytes(StandardCharsets.UTF_8));
            status = IO_FAILURE;
        } catch (InterruptedException interrupted) {
            try {
                Files.write(Paths.get(args[4]), interrupted.toString().getBytes(StandardCharsets.UTF_8));
            } finally {
                Thread.currentThread().interrupt();
            }
            status = IO_FAILURE;
        } finally {
            trace.finish("worker_total", total, success);
            trace.publish();
        }
        // This is a standalone helper JVM, never a driver-side entry point.
        System.exit(status);
    }

    private static int run(String[] args, Trace trace)
            throws IOException, InterruptedException, ExecutionException {
        OpenIvmCliLifecycle lifecycle = new OpenIvmCliLifecycle(Paths.get(args[1]).getParent());
        ProcessBuilder builder = new ProcessBuilder(args[0], ":memory:", "-jsonlines", "-unsigned")
                .redirectInput(Paths.get(args[1]).toFile());
        // Apply the native C++ runtime override to neither JVM.
        if (!args[5].isEmpty()) {
            String inherited = builder.environment().getOrDefault("LD_LIBRARY_PATH", "");
            builder.environment().put(
                    "LD_LIBRARY_PATH", inherited.isEmpty() ? args[5] : args[5] + ":" + inherited);
        }
        if (!lifecycle.beginLaunch()) {
            return PROCESS_TIMEOUT;
        }
        Process process;
        Trace.Mark launch = trace.start();
        boolean launched = false;
        try {
            process = builder.start();
            launched = true;
        } catch (IOException launchFailure) {
            lifecycle.acknowledgeExit();
            throw launchFailure;
        } finally {
            trace.finish("native_launch", launch, launched);
        }
        ExecutorService readers = null;
        Trace.Mark nativeLifecycle = trace.start();
        boolean nativeSuccess = false;
        try {
            lifecycle.acknowledgeNative(process.toHandle());
            if (lifecycle.cancellationRequested()) {
                return PROCESS_TIMEOUT;
            }
            readers = Executors.newFixedThreadPool(2, task -> {
                Thread thread = new Thread(task, "openivm-cli-reader");
                thread.setDaemon(true);
                return thread;
            });
            Future<?> stdout = readers.submit(() -> copy(process.getInputStream(), Paths.get(args[2])));
            Future<?> stderr = readers.submit(() -> copy(process.getErrorStream(), Paths.get(args[3])));
            long processDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PROCESS_TIMEOUT_SECONDS);
            while (process.isAlive()) {
                long remaining = processDeadline - System.nanoTime();
                if (lifecycle.cancellationRequested() || remaining <= 0L) {
                    return PROCESS_TIMEOUT;
                }
                process.waitFor(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
            }
            lifecycle.acknowledgeExit();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLEANUP_TIMEOUT_SECONDS);
            Trace.Mark output = trace.start();
            boolean outputSuccess = false;
            try {
                stdout.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                stderr.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                outputSuccess = true;
            } catch (TimeoutException timeout) {
                return OUTPUT_TIMEOUT;
            } finally {
                trace.finish("worker_output", output, outputSuccess);
            }
            nativeSuccess = true;
            return 0;
        } finally {
            trace.finish("native_lifecycle", nativeLifecycle, nativeSuccess);
            Trace.Mark cleanup = trace.start();
            boolean cleanupSuccess = false;
            try {
                if (readers != null) {
                    readers.shutdownNow();
                }
                if (process.isAlive()) {
                    List<ProcessHandle> descendants;
                    try (Stream<ProcessHandle> children = process.descendants()) {
                        descendants = children.collect(Collectors.toList());
                    }
                    for (int index = descendants.size() - 1; index >= 0; index--) {
                        descendants.get(index).destroyForcibly();
                    }
                    // Process.destroyForcibly also closes streams and can block on
                    // an inherited pipe's reader/reaper lock. Signal the handle only.
                    process.toHandle().destroyForcibly();
                    if (!process.waitFor(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IOException("DuckDB CLI did not terminate after being killed");
                    }
                }
                lifecycle.acknowledgeExit();
                cleanupSuccess = true;
            } finally {
                trace.finish("worker_cleanup", cleanup, cleanupSuccess);
            }
        }
    }

    private static final class Trace {
        private final boolean enabled;
        private final StringBuilder output = new StringBuilder("openivm-compiler-trace-v1\n");

        private Trace(boolean enabled) { this.enabled = enabled; }

        private static final class Mark {
            final long epochMs = System.currentTimeMillis();
            final long nanos = System.nanoTime();
        }

        private Mark start() { return enabled ? new Mark() : null; }

        private void finish(String phase, Mark start, boolean success) {
            if (start != null) {
                output.append(phase).append('|').append(start.epochMs).append('|')
                        .append(Math.max(0L, System.nanoTime() - start.nanos)).append('|')
                        .append(success ? "success" : "failed").append('\n');
            }
        }

        private void publish() {
            if (enabled) {
                System.out.print(output.append("end\n").toString());
                System.out.flush();
            }
        }
    }

    private static Void copy(InputStream input, Path destination) throws IOException {
        try (InputStream source = input;
                OutputStream output = Files.newOutputStream(destination)) {
            source.transferTo(output);
        }
        return null;
    }
}
