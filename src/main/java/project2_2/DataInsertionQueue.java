package project2_2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
    private final Set<Integer> stationIDs = new HashSet<>();

    /**
     * This map holds the most up-to-date data of the weather stations.
     * When data gets pushed to the server, it will be removed from this map.
     */
    private final Map<Integer, StationWeatherData> latestData = new HashMap<>();

    /**
     * The synchronous queue for data which should be sent to the database.
     */
    private final SynchronousQueue<List<StationWeatherData>> insertQueue = new SynchronousQueue<>();

    /**
     * Constructor of {@link DataInsertionQueue}
     *
     * @param properties The global {@link Properties} object
     * @param statistics The global {@link RealtimeStatistics} object
     */
    public DataInsertionQueue(Properties properties, RealtimeStatistics statistics) {
        this.properties = properties;
        this.statistics = statistics;
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
        synchronized(stationIDs) {
            for(StationWeatherData data : dataList) {
                // Make sure each ID is in the list
                // It's a set, so duplicates won't get added.
                stationIDs.add(data.stationId);
            }
        }

        synchronized(latestData) {
            for(StationWeatherData data : dataList) {
                // Add each data point, and update the missing data with data from the previous data
                StationWeatherData prevData = latestData.get(data.stationId);
                if(prevData != null) {
                    data.updateMissingFrom(prevData);
                }

                latestData.put(data.stationId, data);
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

        int insertQueryThreads = Integer.parseInt(properties.getProperty("insert_query_threads"));
        final int insertsPerQuery = Integer.parseInt(properties.getProperty("inserts_per_query"));

        // Keep track of which stage we're at.
        // This timer will loop from 0 to division-1, back to 0 (etc. etc)
        int updateTimer = 0;

        long start, sleepMs;

        while(true) {
            start = System.currentTimeMillis();

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
            List<List<StationWeatherData>> dataToSendChunks = new ArrayList<>();
            dataToSendChunks.add(new ArrayList<>(insertsPerQuery));

            int chunkIndex = 0;

            synchronized(latestData) {
                for(Integer stationId : latestData.keySet()) {
                    StationWeatherData data = latestData.get(stationId);
                    if(!data.isNew) {
                        continue;
                    }

                    int index = stationIDsList.indexOf(stationId);
                    if(index == -1) index = 0;
                    if(index % updateDivisionMs != updateTimer) {
                        continue;
                    }

                    // To prevent this data from being sent multiple times, set the isNew flag to false
                    // Don't remove it! We use it in onDataReceive to fix broken values with previous data
                    data.isNew = false;

                    if(!data.isComplete()) {
                        System.out.println("Warning! A datapoint for station " + stationId + " is not complete, so we're not inserting it into the database.");
                        continue;
                    }

                    // This data should be sent right now!

                    List<StationWeatherData> currentChunk = dataToSendChunks.get(chunkIndex);
                    if(currentChunk.size() >= insertsPerQuery) {
                        chunkIndex++;

                        currentChunk = new ArrayList<>(insertsPerQuery);
                        dataToSendChunks.add(currentChunk);
                    }

                    currentChunk.add(data);
                }
            }

            // Insert all data that needs to be sent in the insertQueue synchronous queue.
            // When we do this, the multiple processInsertQueue threads will take care of
            // pushing the data to the database.
            for(List<StationWeatherData> chunk : dataToSendChunks) {
                if(chunk.isEmpty()) {
                    continue;
                }

                try{
                    insertQueue.put(chunk);
                }catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Sleep for the required time
            sleepMs = updateIntervalMs / updateDivisionMs - (System.currentTimeMillis() - start);
            if(sleepMs > 0) {
                try{
                    Thread.sleep(sleepMs);
                }catch(InterruptedException ignored) {}
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
            List<StationWeatherData> chunk;
            try{
                // Take data off of the insertQueue queue.
                // This method is blocking, so we halt code execution until we actually receive some data.
                chunk = insertQueue.take();
            }catch(InterruptedException e) {
                e.printStackTrace();
                continue;
            }

            if(chunk.isEmpty()) {
                continue;
            }

            try{
                StringBuilder query = new StringBuilder("INSERT INTO data(station_id,date,temperature,dew_point,station_air_pressure,sea_air_pressure,visibility,wind_speed,precipitation,snow_height,overcast,wind_direction,has_frozen,has_rained,has_snowed,has_hailed,has_thundered,has_whirlwinded)VALUES");
                for(int i = 0; i < chunk.size(); i++) {
                    if(i != 0) {
                        query.append(',');
                    }

                    query.append("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                }

                // Prepare the statement
                PreparedStatement stmt = con.prepareStatement(query.toString());

                // Set the values of the statement
                for(int i = 0; i < chunk.size(); i++) {
                    StationWeatherData data = chunk.get(i);

                    int offset = i * 18;
                    stmt.setInt    (offset + 1,  data.stationId);
                    stmt.setInt    (offset + 2,  data.date);
                    stmt.setFloat  (offset + 3,  data.temperature);
                    stmt.setFloat  (offset + 4,  data.dewPoint);
                    stmt.setFloat  (offset + 5,  data.stationAirPressure);
                    stmt.setFloat  (offset + 6,  data.seaAirPressure);
                    stmt.setFloat  (offset + 7,  data.visibility);
                    stmt.setFloat  (offset + 8,  data.windSpeed);
                    stmt.setFloat  (offset + 9,  data.precipitation);
                    stmt.setFloat  (offset + 10, data.snowHeight);
                    stmt.setFloat  (offset + 11, data.overcast);
                    stmt.setShort  (offset + 12, data.windDirection);
                    stmt.setBoolean(offset + 13, data.hasFrozen);
                    stmt.setBoolean(offset + 14, data.hasRained);
                    stmt.setBoolean(offset + 15, data.hasSnowed);
                    stmt.setBoolean(offset + 16, data.hasHailed);
                    stmt.setBoolean(offset + 17, data.hasThundered);
                    stmt.setBoolean(offset + 18, data.hasWhirlwinded);
                }

                // EXECUTE!
                stmt.executeUpdate();

                // Update statistics!
                statistics.addSQLQuery();
            }catch(SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
