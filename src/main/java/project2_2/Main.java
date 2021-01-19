package project2_2;

public class Main {
    public static void main(String[] args) throws Exception {
        WeatherServer weatherServer = new WeatherServer(7789);
        weatherServer.start();
    }
}
