package project2_2;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This class represents some data which is sent by a single weather station.
 */
public class StationWeatherData {
    // The unique station ID
    public final int stationId;

    // The time at which this event occurred
    public final int date;

    // ~ Weather data ~
    public final float temperature; // Temperature in degrees Celsius
    public final float dewPoint; // Dew point in degrees Celsius
    public final float stationAirPressure; // Air pressure at station level in millibar
    public final float seaAirPressure; // Air pressure at sea level in millibar
    public final float visibility; // Visibility in KM
    public final float windSpeed; // Wind speed in kilometers per hour
    public final float precipitation; // Precipitation in centimeters
    public final float snowHeight; // Show height in centimeters
    public final float overcast; // Overcast percentage
    public final short windDirection; // Wind direction in degrees

    // Some flags
    public final boolean hasFrozen;
    public final boolean hasRained;
    public final boolean hasSnowed;
    public final boolean hasHailed;
    public final boolean hasThundered;
    public final boolean hasWhirlwinded;

    /**
     * The constructor, which sets every field in this class.
     */
    public StationWeatherData(int stationId, int date, float temperature, float dewPoint, float stationAirPressure,
                              float seaAirPressure, float visibility, float windSpeed, float precipitation,
                              float snowHeight, float overcast, short windDirection, boolean hasFrozen, boolean hasRained,
                              boolean hasSnowed, boolean hasHailed, boolean hasThundered, boolean hasWhirlwinded) {
        this.stationId = stationId;
        this.date = date;
        this.temperature = temperature;
        this.dewPoint = dewPoint;
        this.stationAirPressure = stationAirPressure;
        this.seaAirPressure = seaAirPressure;
        this.visibility = visibility;
        this.windSpeed = windSpeed;
        this.precipitation = precipitation;
        this.snowHeight = snowHeight;
        this.overcast = overcast;
        this.windDirection = windDirection;
        this.hasFrozen = hasFrozen;
        this.hasRained = hasRained;
        this.hasSnowed = hasSnowed;
        this.hasHailed = hasHailed;
        this.hasThundered = hasThundered;
        this.hasWhirlwinded = hasWhirlwinded;
    }

    /**
     * Parses an XML object into a {@link StationWeatherData} object.
     *
     * Adds support for a set of allowed station IDs. The station ID is parsed first,
     * and if this ID does not match any ID in the allowed ID list, execution stops and null is returned.
     * If the allowed IDs list is null or empty, all IDs will be valid.
     *
     * @param measurement The XML data
     * @return The parsed StationWeatherData, or NULL if the ID was not allowed
     */
    public static StationWeatherData parseSingleFromXML(Element measurement) {
        // Parse the ID
        int stationId = Integer.parseInt(getNode(measurement, "STN", "-1"));

        // Parse date and time
        String dateStr = getNode(measurement, "DATE", "0000-00-00");
        int year = Integer.parseInt(dateStr.substring(0, 4));
        int month = Integer.parseInt(dateStr.substring(5, 7));
        int day = Integer.parseInt(dateStr.substring(8, 10));

        String timeStr = getNode(measurement, "TIME", "00:00:00");
        int hour = Integer.parseInt(timeStr.substring(0, 2));
        int minute = Integer.parseInt(timeStr.substring(3, 5));
        int second = Integer.parseInt(timeStr.substring(6, 8));

        // Put the measurement date in a java Date object
        Date dateObj = new Date(year - 1900, month - 1, day, hour, minute, second);
        int date = (int) dateObj.toInstant().getEpochSecond();

        // Parse weather data points
        float temperature = Float.parseFloat(getNode(measurement, "TEMP", "-1"));
        float dewPoint = Float.parseFloat(getNode(measurement, "DEWP", "-1"));
        float stationAirPressure = Float.parseFloat(getNode(measurement, "STP", "-1"));
        float seaAirPressure = Float.parseFloat(getNode(measurement, "SLP", "-1"));
        float visibility = Float.parseFloat(getNode(measurement, "VISIB", "-1"));
        float windSpeed = Float.parseFloat(getNode(measurement, "WDSP", "-1"));
        float precipitation = Float.parseFloat(getNode(measurement, "PRCP", "-1"));
        float snowHeight = Float.parseFloat(getNode(measurement, "SNDP", "-1"));
        float overcast = Float.parseFloat(getNode(measurement, "CLDC", "-1"));
        short windDirection = Short.parseShort(getNode(measurement, "WNDDIR", "-1"));

        // Parse events
        String events = getNode(measurement, "FRSHTT", "000000"); // Events
        boolean hasFrozen      = events.charAt(0) != '0';
        boolean hasRained      = events.charAt(1) != '0';
        boolean hasSnowed      = events.charAt(2) != '0';
        boolean hasHailed      = events.charAt(3) != '0';
        boolean hasThundered   = events.charAt(4) != '0';
        boolean hasWhirlwinded = events.charAt(5) != '0';

        // Return the parsed data
        return new StationWeatherData(stationId, date, temperature, dewPoint, stationAirPressure, seaAirPressure,
                visibility, windSpeed, precipitation, snowHeight, overcast, windDirection, hasFrozen, hasRained,
                hasSnowed, hasHailed, hasThundered, hasWhirlwinded);
    }

    /**
     * Parses a large XML string, which contains multiple measurements.
     *
     * @param xmlData The large XML data
     * @return A list of all parsed data
     */
    public static List<StationWeatherData> parseListFromXML(String xmlData) {
        List<StationWeatherData> result = new ArrayList<>();

        try{
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            ByteArrayInputStream input = new ByteArrayInputStream(
                    xmlData.getBytes(StandardCharsets.UTF_8));
            Document doc = builder.parse(input);

            doc.getDocumentElement().normalize();

            Element weatherData = (Element) doc.getElementsByTagName("WEATHERDATA").item(0);

            NodeList measurementList = weatherData.getElementsByTagName("MEASUREMENT");
            for(int i = 0; i < measurementList.getLength(); i++) {
                try{
                    Element measurement = (Element) measurementList.item(i);

                    StationWeatherData entry = parseSingleFromXML(measurement);
                    if(entry != null) {
                        result.add(entry);
                    }
                }catch(Exception e) {
                    System.out.println("Could not parse weather measurement data! " + e.toString());
                    e.printStackTrace();
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Utility function to get a certain node from XML data, with the ability to set a default value if the node does not exist.
     */
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
}
