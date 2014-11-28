package ua.org.taraktikos.study;

import org.junit.Before;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.*;

public class DataLoaderTest {

    DataLoader loader;

    @Before
    public void setUp() throws Exception {
        loader = new DataLoader(Database.getInstance().getConnection());
    }

    @Test
    public void testImportData() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("GB\tB99\tBirmingham\tEngland\tENG\t\t\tBirmingham District (B)\tE08000025\t52.4814\t-1.8998\t4".getBytes());
        assertEquals(1, loader.load(inputStream));
    }

    @Test
    public void testParseLine() throws Exception {
        Map<String, String> map = loader.parseLine("GB\tB99\tBirmingham\tEngland\tENG\t\t\tBirmingham District (B)\tE08000025\t52.4814\t-1.8998\t4");
        assertEquals("GB", map.get("countryCode"));
        assertEquals("B99", map.get("postalCode"));
        assertEquals("Birmingham", map.get("cityName"));
        assertEquals("England", map.get("countryName"));
        assertEquals("ENG", map.get("longCountryCode"));
        assertEquals("", map.get("provinceName"));
        assertEquals("", map.get("provinceCode"));
        assertEquals("Birmingham District (B)", map.get("regionName"));
        assertEquals("E08000025", map.get("regionCode"));
        assertEquals("52.4814", map.get("latitude"));
        assertEquals("-1.8998", map.get("longitude"));
        assertEquals("4", map.get("accuracy"));
    }
}