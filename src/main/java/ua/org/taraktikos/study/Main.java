package ua.org.taraktikos.study;

import java.sql.*;

public class Main {

    public static void main(String[] args) {
        String driver = "org.postgresql.Driver";
        String url = "jdbc:postgresql://localhost/study00";
        String user = "study";
        String password = "study";
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery("SELECT VERSION()");
            while (result.next()) {
                System.out.println(result.toString());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        System.out.println("Done");
    }

}
