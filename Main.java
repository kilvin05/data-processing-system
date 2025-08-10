package dps;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Data Processing System (Java)
 * - Shared task queue (BlockingQueue)
 * - Worker threads via ExecutorService
 * - Results aggregated safely and flushed to file
 * - Robust logging and exception handling
 */
public class Main {

    private static final Logger logger = Logger.getLogger("DPS-Java");
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static class Task {
        private final int id;
        private final String payload;

        public Task(int id, String payload) {
            this.id = id;
            this.payload = payload;
        }

        public int getId() {
            return id;
        }

        public String getPayload() {
            return payload;
        }
    }

    public static class Worker implements Callable<Integer> {
        private final int workerId;
        private final BlockingQueue<Task> queue;
        private final List<String> results;
        private final Random rng = new Random();

        public Worker(int workerId, BlockingQueue<Task> queue, List<String> results) {
            this.workerId = workerId;
            this.queue = queue;
            this.results = results;
        }

        @Override
        public Integer call() {
            int processed = 0;
            logger.info(String.format("Worker-%d START at %s", workerId, TS.format(LocalDateTime.now())));
            try {
                while (true) {
                    Task task = queue.poll(500, TimeUnit.MILLISECONDS);
                    if (task == null) {
                        // No tasks recently; check if producer likely done
                        // We'll continue polling until interrupted or timeout logic stops us by poison-pill
                        continue;
                    }
                    // Poison pill: id < 0 means terminate
                    if (task.getId() < 0) {
                        logger.info(String.format("Worker-%d received poison pill. Shutting down.", workerId));
                        break;
                    }

                    try {
                        // Simulate computational delay
                        Thread.sleep(50 + rng.nextInt(150));
                        String result = String.format("Worker-%d processed task-%d payload='%s' at %s",
                                workerId, task.getId(), task.getPayload(), TS.format(LocalDateTime.now()));
                        synchronized (results) {
                            results.add(result);
                        }
                        processed++;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.log(Level.WARNING, String.format("Worker-%d interrupted", workerId), ie);
                        break;
                    } catch (Exception ex) {
                        logger.log(Level.SEVERE, String.format("Worker-%d error processing task-%d",
                                workerId, task.getId()), ex);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING, String.format("Worker-%d interrupted while polling queue", workerId), e);
            } catch (Exception e) {
                logger.log(Level.SEVERE, String.format("Worker-%d unexpected error", workerId), e);
            } finally {
                logger.info(String.format("Worker-%d COMPLETE. Processed=%d at %s",
                        workerId, processed, TS.format(LocalDateTime.now())));
            }
            return processed;
        }
    }

    public static void main(String[] args) {
        int numWorkers = 4;
        int numTasks = 25;
        String outDir = "out";
        String resultFile = outDir + "/java_results.txt";
        String logFile = outDir + "/java_app.log";

        try {
            Files.createDirectories(Paths.get(outDir));
        } catch (IOException e) {
            System.err.println("Failed to create output directory: " + e.getMessage());
            return;
        }

        // Set up logger
        try {
            FileHandler fh = new FileHandler(logFile, true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
            logger.setUseParentHandlers(true);
        } catch (IOException e) {
            System.err.println("Failed to set up logger: " + e.getMessage());
            return;
        }

        BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
        List<String> results = new ArrayList<>(); // guarded by synchronized blocks
        ExecutorService pool = Executors.newFixedThreadPool(numWorkers);
        List<Future<Integer>> futures = new ArrayList<>();

        // Producer: enqueue tasks
        try {
            for (int i = 1; i <= numTasks; i++) {
                queue.put(new Task(i, "data-" + i));
            }
            // Poison pills for clean shutdown (one per worker)
            for (int i = 0; i < numWorkers; i++) {
                queue.put(new Task(-1, "POISON"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, "Producer interrupted while enqueuing tasks", e);
            pool.shutdownNow();
            return;
        }

        // Launch workers
        for (int w = 1; w <= numWorkers; w++) {
            futures.add(pool.submit(new Worker(w, queue, results)));
        }

        // Await completion
        pool.shutdown();
        try {
            boolean finished = pool.awaitTermination(2, TimeUnit.MINUTES);
            if (!finished) {
                logger.warning("Timeout waiting for worker termination. Forcing shutdown.");
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING, "Main thread interrupted while awaiting termination", e);
        }

        // Aggregate results to file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, false))) {
            for (String line : results) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write results to file", e);
        }

        logger.info(String.format("Completed run. Results=%d written to %s", results.size(), resultFile));
        System.out.println("JAVA DPS completed. See " + resultFile + " and " + logFile);
    }
}
