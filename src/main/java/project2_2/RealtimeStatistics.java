package project2_2;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that holds and shows real-time statistics about the load of the server.
 */
public class RealtimeStatistics {
    // All data points
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger bulkDataReceivePerSecond = new AtomicInteger(0);
    private final AtomicInteger dataReceivePerSecond = new AtomicInteger(0);
    private final AtomicInteger queriesPerSecond = new AtomicInteger(0);
    private final AtomicInteger insertsPerSecond = new AtomicInteger(0);

    public RealtimeStatistics() {
    }

    /**
     * Starts the thread which displays the statistics in the console.
     */
    public void startStatsThread() {
        new Thread(this::statsThread).start();
    }

    /**
     * Increases the connection count.
     */
    public void addConnection() {
        synchronized(activeConnections) {
            activeConnections.incrementAndGet();
        }
    }

    /**
     * Decreases the connection count.
     */
    public void removeConnection() {
        synchronized(activeConnections) {
            activeConnections.decrementAndGet();
        }
    }

    /**
     * Increases the bulk data amount.
     */
    public void addBulkData() {
        synchronized(bulkDataReceivePerSecond) {
            bulkDataReceivePerSecond.incrementAndGet();
        }
    }

    /**
     * Increases the data amount.
     *
     * @param amount The amount to increase by
     */
    public void addData(int amount) {
        synchronized(dataReceivePerSecond) {
            dataReceivePerSecond.addAndGet(amount);
        }
    }

    /**
     * Increases the SQL query amount.
     */
    public void addSQLQuery() {
        synchronized(queriesPerSecond) {
            queriesPerSecond.incrementAndGet();
        }
    }

    /**
     * Increases the insertions amount
     *
     * @param amount The amount to increase by
     */
    public void addInsertions(int amount) {
        synchronized(insertsPerSecond) {
            insertsPerSecond.addAndGet(amount);
        }
    }

    /**
     * This is the thread which displays (and resets) data.
     */
    private void statsThread() {
        while(true) {
            // Wait 1000ms, aka 1 second, so we have all values be "per second"
            try{
                Thread.sleep(1000);
            }catch(InterruptedException ignored) {}

            // -- Read the data synchronously, because the data will be adjusted from multiple threads --
            int activeConnectionsValue;
            synchronized(activeConnections) {
                activeConnectionsValue = activeConnections.get();
            }

            int bulkDataReceivePerSecondValue;
            synchronized(bulkDataReceivePerSecond) {
                bulkDataReceivePerSecondValue = bulkDataReceivePerSecond.getAndSet(0);
            }

            int dataReceivePerSecondValue;
            synchronized(dataReceivePerSecond) {
                dataReceivePerSecondValue = dataReceivePerSecond.getAndSet(0);
            }

            int queriesPerSecondValue;
            synchronized(queriesPerSecond) {
                queriesPerSecondValue = queriesPerSecond.getAndSet(0);
            }

            int insertsPerSecondValue;
            synchronized(insertsPerSecond) {
                insertsPerSecondValue = insertsPerSecond.getAndSet(0);
            }

            // Calculate RAM usage in MB
            double usedRam  = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024.0 / 1024.0;
            double totalRam = Runtime.getRuntime().totalMemory() / 1024.0 / 1024.0;
            double maxRam   = Runtime.getRuntime().maxMemory() / 1024.0 / 1024.0;

            // Display all values in the console
            System.out.printf("clients=%4d, bulk_data_ps=%4d, actual_data_ps=%5d, queries_ps=%2d, inserts_ps=%5d used_ram=%.2fmb, total_ram=%.2fmb, max_ram=%.2fmb%n",
                    activeConnectionsValue, bulkDataReceivePerSecondValue, dataReceivePerSecondValue, queriesPerSecondValue, insertsPerSecondValue, usedRam, totalRam, maxRam);
        }
    }
}
