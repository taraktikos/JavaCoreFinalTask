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

}
