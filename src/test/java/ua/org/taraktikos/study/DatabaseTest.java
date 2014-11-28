package ua.org.taraktikos.study;

import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.*;

public class DatabaseTest {

    Database db;

    @Before
    public void setUp() throws Exception {
        db = Database.getInstance();
    }

    @Test
    public void testGetConnection() throws Exception {
        assertNotNull(db.getConnection());
    }

}