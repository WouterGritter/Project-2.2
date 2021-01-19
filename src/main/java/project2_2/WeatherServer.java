package project2_2;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class WeatherServer {
    /**
     * CREATE TABLE `weatherdata`. ( `id` INT NOT NULL AUTO_INCREMENT , `station_id` INT NOT NULL , `date` INT NOT NULL , `dew_point` DOUBLE NOT NULL , `station_air_pressure` DOUBLE NOT NULL , `sea_air_pressure` DOUBLE NOT NULL , `visibility` DOUBLE NOT NULL , `wind_speed` DOUBLE NOT NULL , `precipitation` DOUBLE NOT NULL , `snow_height` DOUBLE NOT NULL , `overcast` DOUBLE NOT NULL , `wind_direction` INT NOT NULL , `has_frozen` BOOLEAN NOT NULL , `has_rained` BOOLEAN NOT NULL , `has_snowed` BOOLEAN NOT NULL , `has_hailed` BOOLEAN NOT NULL , `has_thundered` BOOLEAN NOT NULL , `has_whirlwinded` BOOLEAN NOT NULL , PRIMARY KEY (`id`), INDEX `stations.stn` (`station_id`)) ENGINE = InnoDB;
     */
    private static final String DATABASE_URL = "jdbc:mysql://localhost/weatherdata?user=root&password=";

    private static final int CONNECTION_THREADS = 50;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private final int port;

    private ServerSocket serverSocket;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger dataProcessPerSecond = new AtomicInteger(0);
    private final AtomicInteger queriesPerSecond = new AtomicInteger(0);

    private final SynchronousQueue<String> dataQueue = new SynchronousQueue<>();

    public WeatherServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);

        // Start accepting clients
        new Thread(this::acceptClients).start();

        // Start processing the queue in multiple threads
        for(int i = 0; i < CONNECTION_THREADS; i++) {
            new Thread(this::processQueue).start();
        }

        // Keep track of ups (updates per second)
        new Thread(this::updatesPerSecondMonitor).start();

        System.out.println("Started on *:" + port + "!");
    }

    private void updatesPerSecondMonitor() {
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int dataProcessPerSecondValue;
            synchronized(dataProcessPerSecond) {
                dataProcessPerSecondValue = dataProcessPerSecond.getAndSet(0);
            }

            int queriesPerSecondValue;
            synchronized(queriesPerSecond) {
                queriesPerSecondValue = queriesPerSecond.getAndSet(0);
            }

            System.out.printf("data ups = %4d, queries per second = %5d%n", dataProcessPerSecondValue, queriesPerSecondValue);
        }
    }

    private void acceptClients() {
        while(true) {
            try{
                Socket client = serverSocket.accept();
                synchronized(activeConnections) {
                    int nConnections = activeConnections.incrementAndGet();
                    System.out.println("Client connected. Connections: " + nConnections);
                }

                new Thread(() -> handleClient(client)).start();
            }catch(IOException e) {
                System.out.println("Error while accepting clients: " + e.toString());
                e.printStackTrace();
            }
        }
    }

    private void handleClient(Socket client) {
        try{
            InputStream in = client.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            StringBuilder currentWeatherData = new StringBuilder();

            String line;
            while((line = br.readLine()) != null) {
                currentWeatherData.append(line);
                currentWeatherData.append('\n');

                if(line.equals("</WEATHERDATA>")) {
                    // We've reached the end bois.
                    String weatherData = currentWeatherData.toString();
                    currentWeatherData = new StringBuilder();

                    dataQueue.put(weatherData);
                }
            }

            synchronized(activeConnections) {
                int nConnections = activeConnections.decrementAndGet();
                System.out.println("Client disconnected. Connections: " + nConnections);
            }
        }catch(IOException | InterruptedException e) {
            System.out.println("Error while handling client: " + e.toString());
            e.printStackTrace();
        }
    }

    private void processWeatherData(Connection connection, String weatherDataString) {
        synchronized(dataProcessPerSecond) {
            dataProcessPerSecond.incrementAndGet();
        }

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            ByteArrayInputStream input = new ByteArrayInputStream(
                    weatherDataString.getBytes(StandardCharsets.UTF_8));
            Document doc = builder.parse(input);

            doc.getDocumentElement().normalize();

            Element weatherData = (Element) doc.getElementsByTagName("WEATHERDATA").item(0);

            NodeList measurementList = weatherData.getElementsByTagName("MEASUREMENT");
            for(int i = 0; i < measurementList.getLength(); i++) {
                try{
                    Element measurement = (Element) measurementList.item(i);

                    int stationId = Integer.parseInt(getNode(measurement, "STN", "-1"));

                    String dateStr = getNode(measurement, "DATE", "0000-00-00");
                    int year = Integer.parseInt(dateStr.substring(0, 4));
                    int month = Integer.parseInt(dateStr.substring(5, 7));
                    int day = Integer.parseInt(dateStr.substring(8, 10));

                    String timeStr = getNode(measurement, "TIME", "00:00:00");
                    int hour = Integer.parseInt(timeStr.substring(0, 2));
                    int minute = Integer.parseInt(timeStr.substring(3, 5));
                    int second = Integer.parseInt(timeStr.substring(6, 8));

                    Date date = new Date(year - 1900, month - 1, day, hour, minute, second);

                    double dewPoint = Double.parseDouble(getNode(measurement, "DEWP", "-1")); // Dew point in degrees Celsius
                    double stationAirPressure = Double.parseDouble(getNode(measurement, "STP", "-1")); // Air pressure at station level in millibar
                    double seaAirPressure = Double.parseDouble(getNode(measurement, "SLP", "-1")); // Air pressure at sea level in millibar
                    double visibility = Double.parseDouble(getNode(measurement, "VISIB", "-1")); // Visibility in KM
                    double windSpeed = Double.parseDouble(getNode(measurement, "WDSP", "-1")); // Wind speed in kilometers per hour
                    double precipitation = Double.parseDouble(getNode(measurement, "PRCP", "-1")); // Precipitation in centimeters
                    double snowHeight = Double.parseDouble(getNode(measurement, "SNDP", "-1")); // Show height in centimeters
                    double overcast = Double.parseDouble(getNode(measurement, "CLDC", "-1")); // Overcast percentage
                    int windDirection = Integer.parseInt(getNode(measurement, "WNDDIR", "-1")); // Wind direction in degrees

                    String events = getNode(measurement, "FRSHTT", "000000"); // Events
                    boolean hasFrozen      = events.charAt(0) != '0';
                    boolean hasRained      = events.charAt(1) != '0';
                    boolean hasSnowed      = events.charAt(2) != '0';
                    boolean hasHailed      = events.charAt(3) != '0';
                    boolean hasThundered   = events.charAt(4) != '0';
                    boolean hasWhirlwinded = events.charAt(5) != '0';

                    try{
                        storeInDatabase(connection,
                                stationId, date, dewPoint, stationAirPressure, seaAirPressure, visibility,
                                windSpeed, precipitation, snowHeight, overcast, windDirection, hasFrozen, hasRained,
                                hasSnowed, hasHailed, hasThundered, hasWhirlwinded);
                    }catch(SQLException e) {
                        e.printStackTrace();
                    }
                }catch(Exception e) {
                    System.out.println("Could not parse weather measurement data! " + e.toString());
                    e.printStackTrace();
                }
            }
        }catch (Exception e) {
            System.out.println("Could not parse weather data! " + e.toString());
            e.printStackTrace();
        }
    }

    private static String getNode(Element element, String name, String def) {
        NodeList nodeList = element.getElementsByTagName(name);
        if(nodeList.getLength() < 1) {
            return def;
        }

        String textContent = nodeList.item(0).getTextContent();
        if(textContent.isEmpty()) {
            return def;
        }

        return textContent;
    }

    private void storeInDatabase(Connection connection,
                                 int stationId, Date date, double dewPoint, double stationAirPressure,
                                 double seaAirPressure, double visibility, double windSpeed, double precipitation,
                                 double snowHeight, double overcast, int windDirection, boolean hasFrozen,
                                 boolean hasRained, boolean hasSnowed, boolean hasHailed, boolean hasThundered,
                                 boolean hasWhirlwinded) throws SQLException { // total 17
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO `data`(`station_id`, `date`, `dew_point`, `station_air_pressure`, `sea_air_pressure`, `visibility`, `wind_speed`, `precipitation`, `snow_height`, `overcast`, `wind_direction`, `has_frozen`, `has_rained`, `has_snowed`, `has_hailed`, `has_thundered`, `has_whirlwinded`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setInt    (1,  stationId);
        stmt.setInt    (2,  (int) date.toInstant().getEpochSecond());
        stmt.setDouble (3,  dewPoint);
        stmt.setDouble (4,  stationAirPressure);
        stmt.setDouble (5,  seaAirPressure);
        stmt.setDouble (6,  visibility);
        stmt.setDouble (7,  windSpeed);
        stmt.setDouble (8,  precipitation);
        stmt.setDouble (9,  snowHeight);
        stmt.setDouble (10, overcast);
        stmt.setInt    (11, windDirection);
        stmt.setBoolean(12, hasFrozen);
        stmt.setBoolean(13, hasRained);
        stmt.setBoolean(14, hasSnowed);
        stmt.setBoolean(15, hasHailed);
        stmt.setBoolean(16, hasThundered);
        stmt.setBoolean(17, hasWhirlwinded);

        stmt.executeUpdate();

        synchronized(queriesPerSecond) {
            queriesPerSecond.incrementAndGet();
        }
    }

    private void processQueue() {
        Connection con;
        try {
            con = DriverManager.getConnection(DATABASE_URL);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
            return;
        }

        while(true) {
            String data = null;
            try {
                data = dataQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            processWeatherData(con, data);
        }
    }
}
