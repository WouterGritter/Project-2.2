package project2_2;

import java.util.Date;

/**
 * This class represents some data which is sent by a single weather station.
 */
public class StationWeatherData {
    // The unique station ID
    public int stationId = -1;

    // The time at which this event occurred
    public int date_year = -1, date_month = -1, date_day = -1;
    public int time_hour = -1, time_minute = -1, time_second = -1;

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

    public StationWeatherData() {
    }

    /**
     * Checks weather this data point is complete or not. If it isn't, it's
     * not safe to insert it in a database.
     *
     * @return Whether this data point is complete or not
     */
    public boolean isComplete() {
        return stationId != -1 &&
                date_year != -1 && date_month != -1 && date_day != -1 &&
                time_hour != -1 && time_minute != -1 && time_second != -1 &&
                temperature != null && dewPoint != null && stationAirPressure != null && seaAirPressure != null &&
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
        this.date_year = other.date_year;
        this.date_month = other.date_month;
        this.date_day = other.date_day;

        this.time_hour = other.time_hour;
        this.time_minute = other.time_minute;
        this.time_second = other.time_second;

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

    public int calculateUnixTime() {
        // Put the measurement date in a java Date object
        Date dateObj = new Date(date_year - 1900, date_month - 1, date_day, time_hour, time_minute, time_second);
        return (int) dateObj.toInstant().getEpochSecond();
    }

    public void insertData(String key, String value) {
        switch(key) {
            case "STN":
                stationId = Integer.parseInt(value);
                break;
            case "DATE":
                date_year = Integer.parseInt(value.substring(0, 4));
                date_month = Integer.parseInt(value.substring(5, 7));
                date_day = Integer.parseInt(value.substring(8, 10));
                break;
            case "TIME":
                time_hour = Integer.parseInt(value.substring(0, 2));
                time_minute = Integer.parseInt(value.substring(3, 5));
                time_second = Integer.parseInt(value.substring(6, 8));
                break;
            case "TEMP":
                temperature = Float.parseFloat(value);
                break;
            case "DEWP":
                dewPoint = Float.parseFloat(value);
                break;
            case "STP":
                stationAirPressure = Float.parseFloat(value);
                break;
            case "SLP":
                seaAirPressure = Float.parseFloat(value);
                break;
            case "VISIB":
                visibility = Float.parseFloat(value);
                break;
            case "WDSP":
                windSpeed = Float.parseFloat(value);
                break;
            case "PRCP":
                precipitation = Float.parseFloat(value);
                break;
            case "SNDP":
                snowHeight = Float.parseFloat(value);
                break;
            case "CLDC":
                overcast = Float.parseFloat(value);
                break;
            case "WNDDIR":
                windDirection = Short.parseShort(value);
                break;
            case "FRSHTT":
                hasFrozen = value.charAt(0) != '0';
                hasRained = value.charAt(1) != '0';
                hasSnowed = value.charAt(2) != '0';
                hasHailed = value.charAt(3) != '0';
                hasThundered = value.charAt(4) != '0';
                hasWhirlwinded = value.charAt(5) != '0';
                break;
        }
    }
}
