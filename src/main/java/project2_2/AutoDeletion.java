package project2_2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class AutoDeletion {
    private static final long QUERY_INTERVAL_MS = 1000 * 60 * 30; // Every 30 minutes
    private static final String DELETE_QUERY = "DELETE FROM `data` WHERE date < UNIX_TIMESTAMP() - ?";

    private final Properties properties;
    private final RealtimeStatistics statistics;

    public AutoDeletion(Properties properties, RealtimeStatistics statistics) {
        this.properties = properties;
        this.statistics = statistics;
    }

    public void startThread() {
        System.out.println("Deleting all data points when they are older than " + properties.getProperty("auto_deletion_max_age_seconds") + " seconds.");

        new Thread(this::autoDeletionThread).start();
    }

    private void autoDeletionThread() {
        while(true) {
            executeDeletionQuery();

            try{
                Thread.sleep(QUERY_INTERVAL_MS);
            }catch(InterruptedException ignored) {}
        }
    }

    private void executeDeletionQuery() {
        int maxAgeSeconds = Integer.parseInt(properties.getProperty("auto_deletion_max_age_seconds"));

        try{
            Connection con = DriverManager.getConnection(properties.getProperty("db_url"));

            PreparedStatement stmt = con.prepareStatement(DELETE_QUERY);
            stmt.setInt(1, maxAgeSeconds);

            int rowsUpdated = stmt.executeUpdate();
            System.out.println("Deleted " + rowsUpdated + " old data points.");

            con.close();

            statistics.addSQLQuery();
        }catch(SQLException e) {
            e.printStackTrace();
        }
    }
}
