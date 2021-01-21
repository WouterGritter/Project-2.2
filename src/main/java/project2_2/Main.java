package project2_2;

import java.io.*;
import java.util.Properties;

/**
 * The main class, containing some utility bits and is the main entry to the program.
 */
public class Main {
    /**
     * The properties file location. The name of this file will be used to look for the
     * default properties file in the resources of the program.
     */
    private static final File PROPERTIES_FILE = new File("server.properties");

    public static void main(String[] args) throws Exception {
        // Make sure the properties file exists
        saveDefaultProperties();

        // Load the properties
        Properties properties = new Properties();
        properties.load(new FileInputStream(PROPERTIES_FILE));

        // Load the database driver
        String dbDriverClass = properties.getProperty("db_driver_class");
        System.out.println("Trying to load database driver class " + dbDriverClass + "!");
        Class.forName(dbDriverClass);

        // START THE SERVER!
        WeatherServer server = new WeatherServer(properties);
        server.start();
    }

    /**
     * Saves the default server properties file, if there is currently none present on the disk.
     */
    private static void saveDefaultProperties() {
        if(PROPERTIES_FILE.exists()) {
            // The file already exists, do nothing!
            return;
        }

        try{
            // Set up input and output streams
            InputStream in = Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE.getName());
            FileOutputStream out = new FileOutputStream(PROPERTIES_FILE);

            // Copy all the data from in to out (in is the default file, out is the file that we're creating)
            byte[] buf = new byte[512];
            int read;
            while((read = in.read(buf)) > 0) {
                out.write(buf, 0, read);
            }
        }catch(IOException e) {
            // Couldn't write the server properties file, so exit.
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
