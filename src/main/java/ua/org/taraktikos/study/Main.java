package ua.org.taraktikos.study;

import java.io.InputStream;
import java.sql.*;

public class Main {

    public static void main(String[] args) {
        Connection connection = Database.getInstance().getConnection();
        if (connection == null) {
            System.out.println("Database connection fail");
            return;
        }
        int countRows = 0;
        long startTime = System.currentTimeMillis();
        DataLoader dataLoader = new DataLoader(connection);
        try (
                InputStream input = DataLoader.class.getResourceAsStream("/GB.txt")
        ) {
            countRows = dataLoader.load(input, false);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long stopTime = System.currentTimeMillis();
        long runTime = (stopTime - startTime) / 100;
        System.out.println("Done. Count = " + countRows);
        System.out.println("Run time: " + runTime);
    }

}
