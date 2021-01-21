package project2_2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.SynchronousQueue;

/**
 * A class to help with load-balancing the INSERT queries to the database.
 */
public class DataInsertionQueue {
    // Global objects
    private final Properties properties;
    private final RealtimeStatistics statistics;
    private final Set<Integer> stationIDs;

    /**
     * This map holds the most up-to-date data of the weather stations.
     * When data gets pushed to the server, it will be removed from this map.
     */
    private final Map<Integer, StationWeatherData> latestData = new HashMap<>();

    /**
     * The synchronous queue for data which should be sent to the database.
     */
    private final SynchronousQueue<StationWeatherData> insertQueue = new SynchronousQueue<>();

    /**
     * Constructor of {@link DataInsertionQueue}
     *
     * @param properties The global {@link Properties} object
     * @param statistics The global {@link RealtimeStatistics} object
     * @param stationIDs A list of all available station IDs
     */
    public DataInsertionQueue(Properties properties, RealtimeStatistics statistics, Set<Integer> stationIDs) {
        this.properties = properties;
        this.statistics = statistics;
        this.stationIDs = stationIDs;
    }

    /**
     * Starts all required threads.
     */
    public void startThreads() {
        // Starts the queueData thread.
        new Thread(this::queueDataThread).start();

        // Starts multiple processInsertQueue threads.
        int insertQueryThreads = Integer.parseInt(properties.getProperty("insert_query_threads"));
        System.out.println("Starting " + insertQueryThreads + " insert query threads.");
        for(int i = 0; i < insertQueryThreads; i++) {
            new Thread(this::processInsertQueueThread).start();
        }
    }

    /**
     * Should be called when a client received new data.
     *
     * @param dataList A list of data the client received.
     */
    public void onDataReceive(Collection<StationWeatherData> dataList) {
        synchronized(latestData) {
            for(StationWeatherData data : dataList) {
                // Add each data point, if it has an allowed station ID
                if(stationIDs.contains(data.stationId)) {
                    latestData.put(data.stationId, data);
                }
            }
        }
    }

    /**
     * The data queue thread.
     * This thread is responsible for evenly distributing INSERT queries over all the data that needs to be sent.
     *
     * Ex.:
     * Update interval - 10 seconds (seconds between burst updates)
     * Update division - 100 (the amount of slices to divide this 10 seconds in, to distribute the load)
     *
     * Every 10(interval) / 100(division) = 0.1 seconds the thread will push 1 / 100(division) = 1% of
     * the station's (evenly distributed) data to the synchronous queue insertQueue.
     * Once 10 seconds has elapsed, the thread will have pushed 1% * 100(division) = 100% of the data to the database server.
     */
    private void queueDataThread() {
        // Get some values from the properties file
        final int updateIntervalMs = Integer.parseInt(properties.getProperty("station_update_interval_ms"));
        final int updateDivisionMs = updateIntervalMs / Integer.parseInt(properties.getProperty("bulk_update_interval_ms"));

        // Keep track of which stage we're at.
        // This timer will loop from 0 to division-1, back to 0 (etc. etc)
        int updateTimer = 0;

        while(true) {
            // Sleep for the required time
            try{
                Thread.sleep(updateIntervalMs / updateDivisionMs);
            }catch(InterruptedException ignored) {}

            // Update the timer
            updateTimer++;
            if(updateTimer >= updateDivisionMs) {
                updateTimer = 0;
            }

            // Get a copy of the station IDs, which is indexable
            List<Integer> stationIDsList;
            synchronized(stationIDs) {
                stationIDsList = new ArrayList<>(stationIDs);
            }

            // Figure out which data needs to be sent!
            List<StationWeatherData> dataToSend = new ArrayList<>();
            synchronized(latestData) {
                // Get a copy of the keys, because we will be modifying the map as we loop over it
                List<Integer> latestDataKeys = new ArrayList<>(latestData.keySet());
                for(Integer stationId : latestDataKeys) {
                    int index = stationIDsList.indexOf(stationId);
                    if(index == -1) {
                        index = 0;
                    }

                    if(index % updateDivisionMs == updateTimer) {
                        // This data should be sent right now!
                        dataToSend.add(latestData.get(stationId));

                        // To prevent this data from being sent multiple times, remove it from the list
                        latestData.remove(stationId);
                    }
                }
            }

            // Insert all data that needs to be sent in the insertQueue synchronous queue.
            // When we do this, the multiple processInsertQueue threads will take care of
            // pushing the data to the database.
            for(StationWeatherData data : dataToSend) {
                try{
                    insertQueue.put(data);
                }catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * This thread will be executed multiple times, simultaneously.
     *
     * These threads are responsible for taking data off of the insertQueue synchronous queue, and actually inserting it to the database.
     */
    private void processInsertQueueThread() {
        // Set up a database connection that we can use in the thread.
        Connection con;
        try{
            con = DriverManager.getConnection(properties.getProperty("db_url"));
        }catch(SQLException e) {
            e.printStackTrace();
            System.exit(0);
            return;
        }

        while(true) {
            StationWeatherData data;
            try{
                // Take data off of the insertQueue queue.
                // This method is blocking, so we halt code execution until we actually receive some data.
                data = insertQueue.take();
            }catch(InterruptedException e) {
                e.printStackTrace();
                continue;
            }

            try{
                // Call the StationWeatherData#insertInTable method, which will execute the INSERT statement.
                data.insertInTable(con, "data");

                // Update statistics!
                statistics.addSQLQuery();
            }catch(SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
