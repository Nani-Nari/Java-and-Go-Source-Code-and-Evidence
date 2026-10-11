import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class DataProcessingSystem {
    private static final Logger LOG = Logger.getLogger(DataProcessingSystem.class.getName());
    private static final int TASKS = 12, WORKERS = 4;
    static class TaskQueue {
        private final Queue<Integer> queue = new ArrayDeque<>();
        public synchronized void addTask(int id) { queue.add(id); }
        public synchronized Integer getTask() { return queue.poll(); }
    }
    static class Result {
        final int id, square; final String worker;
        Result(int id, int square, String worker) { this.id=id; this.square=square; this.worker=worker; }
        String csv() { return id + "," + square + "," + worker; }
    }
    public static void main(String[] args) {
        TaskQueue queue = new TaskQueue();
        for (int i=1; i<=TASKS; i++) queue.addTask(i);
        List<Result> results = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(WORKERS);
        List<Future<?>> futures = new ArrayList<>();
        for (int w=1; w<=WORKERS; w++) {
            final int workerId=w;
            futures.add(pool.submit(() -> {
                String name="Worker-"+workerId;
                LOG.info(name+" started");
                try {
                    Integer id;
                    while ((id=queue.getTask()) != null) {
                        Thread.sleep(20);
                        results.add(new Result(id, id*id, name));
                        LOG.info(name+" processed task "+id);
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    LOG.log(Level.WARNING, name+" interrupted", ex);
                } catch (RuntimeException ex) {
                    LOG.log(Level.SEVERE, name+" processing failure", ex);
                    throw ex;
                } finally { LOG.info(name+" completed"); }
            }));
        }
        pool.shutdown();
        boolean success=true;
        for (Future<?> future : futures) {
            try { future.get(); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); success=false; LOG.log(Level.SEVERE,"Main interrupted",ex); break; }
            catch (ExecutionException ex) { success=false; LOG.log(Level.SEVERE,"Worker failed",ex.getCause()); }
        }
        if (!success) { pool.shutdownNow(); System.exit(1); }
        results.sort(Comparator.comparingInt(r -> r.id));
        Set<Integer> seen = new HashSet<>();
        for (Result r:results) if (!seen.add(r.id) || r.square!=r.id*r.id) throw new IllegalStateException("Invalid result: "+r.id);
        if (results.size()!=TASKS) throw new IllegalStateException("Missing tasks: "+results.size());
        List<String> lines=new ArrayList<>(); lines.add("task_id,square,worker");
        for (Result r:results) lines.add(r.csv());
        try { Files.write(Path.of("java_results.csv"), lines, StandardCharsets.UTF_8); }
        catch (IOException ex) { LOG.log(Level.SEVERE,"Unable to save results",ex); System.exit(1); }
        System.out.println("JAVA SUCCESS: "+results.size()+" unique tasks processed; saved java_results.csv");
        System.out.println("Task 1 -> 1; Task 12 -> 144");
    }
}
