import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.postgresql.Driver;

public class Main {

    public static void main(String[] args) {
        Driver driver = new Driver();
        Properties properties = new Properties();
        properties.setProperty("user", "study");
        properties.setProperty("password", "study");
        try {
            Connection connection = driver.connect("jdbc:postgresql://localhost/study00", properties);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Done");
    }

}
