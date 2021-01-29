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
import java.util.function.Function;

/**
 * This class represents some data which is sent by a single weather station.
 */
public class StationWeatherData {
    // The unique station ID
    public final int stationId;

    // The time at which this event occurred
    public int date;

    // ~ Weather data ~
    public Float temperature; // Temperature in degrees Celsius
    public Float dewPoint; // Dew point in degrees Celsius
    public Float stationAirPressure; // Air pressure at station level in millibar
    public Float seaAirPressure; // Air pressure at sea level in millibar
    public Float visibility; // Visibility in KM
    public Float windSpeed; // Wind speed in kilometers per hour
    public Float precipitation; // Precipitation in centimeters
    public Float snowHeight; // Show height in centimeters
    public Float overcast; // Overcast percentage
    public Short windDirection; // Wind direction in degrees

    // Some flags
    public Boolean hasFrozen;
    public Boolean hasRained;
    public Boolean hasSnowed;
    public Boolean hasHailed;
    public Boolean hasThundered;
    public Boolean hasWhirlwinded;

    // Whether this datapoint is 'new' or not
    public boolean isNew = true;

    /**
     * The constructor, which sets every field in this class.
     */
    public StationWeatherData(int stationId, int date, Float temperature, Float dewPoint, Float stationAirPressure,
                              Float seaAirPressure, Float visibility, Float windSpeed, Float precipitation,
                              Float snowHeight, Float overcast, Short windDirection, Boolean hasFrozen, Boolean hasRained,
                              Boolean hasSnowed, Boolean hasHailed, Boolean hasThundered, Boolean hasWhirlwinded) {
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
     * Checks weather this data point is complete or not. If it isn't, it's
     * not safe to insert it in a database.
     *
     * @return Whether this data point is complete or not
     */
    public boolean isComplete() {
        return temperature != null && dewPoint != null && stationAirPressure != null && seaAirPressure != null &&
                visibility != null && windSpeed != null && precipitation != null && snowHeight != null &&
                overcast != null && windDirection != null && hasFrozen != null && hasRained != null &&
                hasSnowed != null && hasHailed != null && hasThundered != null && hasWhirlwinded != null;
    }

    /**
     * Updates the values that are missing (aka null) in `this` with the values in `other`.
     *
     * @param other The object to copy values from, if the values from this object are missing
     */
    public void updateMissingFrom(StationWeatherData other) {
        if(this.temperature == null) this.temperature = other.temperature;
        if(this.dewPoint == null) this.dewPoint = other.dewPoint;
        if(this.stationAirPressure == null) this.stationAirPressure = other.stationAirPressure;
        if(this.seaAirPressure == null) this.seaAirPressure = other.seaAirPressure;
        if(this.visibility == null) this.visibility = other.visibility;
        if(this.windSpeed == null) this.windSpeed = other.windSpeed;
        if(this.precipitation == null) this.precipitation = other.precipitation;
        if(this.snowHeight == null) this.snowHeight = other.snowHeight;
        if(this.overcast == null) this.overcast = other.overcast;
        if(this.windDirection == null) this.windDirection = other.windDirection;
        if(this.hasFrozen == null) this.hasFrozen = other.hasFrozen;
        if(this.hasRained == null) this.hasRained = other.hasRained;
        if(this.hasSnowed == null) this.hasSnowed = other.hasSnowed;
        if(this.hasHailed == null) this.hasHailed = other.hasHailed;
        if(this.hasThundered == null) this.hasThundered = other.hasThundered;
        if(this.hasWhirlwinded == null) this.hasWhirlwinded = other.hasWhirlwinded;
    }

    /**
     * Updates all values that are not missing from `other` to `this`.
     * Includes the date!
     *
     * @param other The object to copy values from
     */
    public void updateAllFrom(StationWeatherData other) {
        this.date = other.date;

        if(other.temperature != null) this.temperature = other.temperature;
        if(other.dewPoint != null) this.dewPoint = other.dewPoint;
        if(other.stationAirPressure != null) this.stationAirPressure = other.stationAirPressure;
        if(other.seaAirPressure != null) this.seaAirPressure = other.seaAirPressure;
        if(other.visibility != null) this.visibility = other.visibility;
        if(other.windSpeed != null) this.windSpeed = other.windSpeed;
        if(other.precipitation != null) this.precipitation = other.precipitation;
        if(other.snowHeight != null) this.snowHeight = other.snowHeight;
        if(other.overcast != null) this.overcast = other.overcast;
        if(other.windDirection != null) this.windDirection = other.windDirection;
        if(other.hasFrozen != null) this.hasFrozen = other.hasFrozen;
        if(other.hasRained != null) this.hasRained = other.hasRained;
        if(other.hasSnowed != null) this.hasSnowed = other.hasSnowed;
        if(other.hasHailed != null) this.hasHailed = other.hasHailed;
        if(other.hasThundered != null) this.hasThundered = other.hasThundered;
        if(other.hasWhirlwinded != null) this.hasWhirlwinded = other.hasWhirlwinded;
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
        Float temperature = parseNode(measurement, "TEMP", null, Float::parseFloat);
        Float dewPoint = parseNode(measurement, "DEWP", null, Float::parseFloat);
        Float stationAirPressure = parseNode(measurement, "STP", null, Float::parseFloat);
        Float seaAirPressure = parseNode(measurement, "SLP", null, Float::parseFloat);
        Float visibility = parseNode(measurement, "VISIB", null, Float::parseFloat);
        Float windSpeed = parseNode(measurement, "WDSP", null, Float::parseFloat);
        Float precipitation = parseNode(measurement, "PRCP", null, Float::parseFloat);
        Float snowHeight = parseNode(measurement, "SNDP", null, Float::parseFloat);
        Float overcast = parseNode(measurement, "CLDC", null, Float::parseFloat);
        Short windDirection = parseNode(measurement, "WNDDIR", null, Short::parseShort);

        // Parse events
        Boolean hasFrozen = null, hasRained = null, hasSnowed = null, hasHailed = null, hasThundered = null, hasWhirlwinded = null;
        String events = getNode(measurement, "FRSHTT", null);
        if(events != null && events.length() == 6) {
            hasFrozen      = events.charAt(0) != '0';
            hasRained      = events.charAt(1) != '0';
            hasSnowed      = events.charAt(2) != '0';
            hasHailed      = events.charAt(3) != '0';
            hasThundered   = events.charAt(4) != '0';
            hasWhirlwinded = events.charAt(5) != '0';
        }

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
                    result.add(entry);
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

    /**
     * Utility function to parse a certain node from XML data, with the ability to set a default value if the node does not exist.
     */
    private static <R> R parseNode(Element element, String name, R def, Function<String, R> parser) {
        NodeList nodeList = element.getElementsByTagName(name);
        if(nodeList.getLength() < 1) {
            return def;
        }

        String textContent = nodeList.item(0).getTextContent();
        if(textContent.isEmpty()) {
            return def;
        }

        return parser.apply(textContent);
    }
}
