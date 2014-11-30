package ua.org.taraktikos.study;

import java.io.InputStream;
import java.sql.*;

public class Main {

    public static void main(String[] args) {
        Connection connection = Database.getInstance().getConnection();
        if (connection == null) {
            return;
        }
        int countRows = 0;
        DataLoader dataLoader = new DataLoader(connection);
        try (
                InputStream input = DataLoader.class.getResourceAsStream("/GB.txt")
        ) {
            countRows = dataLoader.load(input, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Done. Count = " + countRows);
    }

}
