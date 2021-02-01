package project2_2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * This is the actual server, and handles stuff like clients connecting and disconnecting.
 */
public class WeatherServer {
    private final Properties properties;

    private ServerSocket server;
    private RealtimeStatistics statistics;
    private DataInsertionQueue dataInsertionQueue;
    private AutoDeletion autoDeletion;

    /**
     * The WeatherServer constructor.
     *
     * @param properties The global properties object containing configuration data
     */
    public WeatherServer(Properties properties) {
        this.properties = properties;
    }

    /**
     * Starts the server socket, instantiates some objects and spin up some threads.
     *
     * @throws IOException If the server socket could not be made.
     */
    public void start() throws IOException {
        // Create a RealtimeStatistics object
        statistics = new RealtimeStatistics();
        statistics.startStatsThread();

        // Create a DataInsertionQueue object
        dataInsertionQueue = new DataInsertionQueue(properties, statistics);
        dataInsertionQueue.startThreads();

        // Auto-delete old data!
        autoDeletion = new AutoDeletion(properties, statistics);
        autoDeletion.startThread();

        // Start the server socket!
        int port = Integer.parseInt(properties.getProperty("server_port"));
        System.out.println("Starting server at *:" + port);
        server = new ServerSocket(port);

        // Call acceptClientsThread() on a different thread, because it is a blocking method.
        new Thread(this::acceptClientsThread).start();
    }

    /**
     * A thread that accepts clients which are trying to connect to the server.
     * After a client has connected, a new thread will be made.
     */
    private void acceptClientsThread() {
        try{
            while(true) {
                // Get a client - this is a blocking function. Code execution will halt until we actually get a client
                Socket clientSocket = server.accept();

                // Create a new thread for the client
                new Thread(() -> handleClientThread(clientSocket)).start();
            }
        }catch(IOException e) {
            // Could not accept clients anymore :(
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * This thread will handle one single client, and will listen for incoming data.
     * This thread also parses the data.
     *
     * @param client The client.
     */
    private void handleClientThread(Socket client) {
        // Update statistics!
        statistics.addConnection();

        try{
            // Set up the input stream reader
            InputStream in = client.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            // We will keep track of the incoming xml data in this object
            StationWeatherData currentMeasurement = new StationWeatherData();
            boolean inMeasurement = false;

            // Loop for as long as the client is connected
            String line;
            while((line = br.readLine()) != null) {
                try{
                    line = line.trim();
                    if(line.equals("<MEASUREMENT>")) {
                        // The beginning of  a new measurement!
                        inMeasurement = true;
                    }else if(line.equals("</MEASUREMENT>")) {
                        // The end of a measurement!
                        inMeasurement = false;

                        dataInsertionQueue.onDataReceive(currentMeasurement);

                        currentMeasurement = new StationWeatherData();

                        // Update statistics!
                        statistics.addDataReceived();
                    }else if(inMeasurement) {
                        String key = line.substring(1, line.indexOf('>'));
                        String value = line.substring(key.length() + 2, line.length() - key.length() - 3);

                        if(!value.isEmpty()) {
                            currentMeasurement.insertData(key, value);
                        }
                    }
                }catch(Exception e) {
                    System.out.println("Could not parse a data line: " + e.toString());
                    System.out.println("Line: " + line);
                }
            }

            // The client disconnected (because we exited from the loop)
        }catch(IOException e) {
            // There was an error while handling the client.. Don't exit the process, just throw the exception.
            System.out.println("Error while handling client: " + e.toString());
            e.printStackTrace();
        }

        // Update statistics!
        statistics.removeConnection();
    }
}
