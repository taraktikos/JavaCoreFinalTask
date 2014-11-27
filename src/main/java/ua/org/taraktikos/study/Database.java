package ua.org.taraktikos.study;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Database {
    private static Database ourInstance = new Database();

    public static Database getInstance() {
        return ourInstance;
    }

    private Database() {
    }

    Connection getConnection() throws Exception {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        Connection connection = null;
        try {
            String url = "jdbc:postgresql://localhost/" + properties.getProperty("database");
            Class.forName(properties.getProperty("driver"));
            connection = DriverManager.getConnection(url, properties.getProperty("user"), properties.getProperty("password"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return connection;
    }

    boolean importData(InputStream inputStream) {
        return false;
    }

    Map<String, String> parseLine(String line) {
        String[] array = line.split("\t");
        Map<String, String> map = new HashMap<>();
        map.put("countryCode", array[0]);
        map.put("postalCode", array[1]);
        map.put("cityName", array[2]);
        map.put("countryName", array[3]);
        map.put("longCountryCode", array[4]);
        map.put("provinceName", array[5]);
        map.put("provinceCode", array[6]);
        map.put("regionName", array[7]);
        map.put("regionCode", array[8]);
        map.put("latitude", array[9]);
        map.put("longitude", array[10]);
        map.put("accuracy", array[11]);
        return map;
    }
}
