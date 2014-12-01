package ua.org.taraktikos.study;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DataLoader {

    Connection connection;
    final int batchSize = 1000;

    public DataLoader(Connection connection) {
        this.connection = connection;
    }

    int load(InputStream inputStream) throws Exception {
        return load(inputStream, false);
    }

    int getPostCodeId(Map<String, String> map) throws SQLException {
        String query = "SELECT id FROM postcode WHERE name = ?";
        PreparedStatement ps = connection.prepareStatement(query);
        ps.setString(1, map.get("postalCode"));
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return rs.getInt("id");
        }
        query = "INSERT INTO postcode (name) values (?)";
        ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, map.get("postalCode"));
        int affectedRows = ps.executeUpdate();
        if (affectedRows == 0) {
            throw new SQLException("Creating postcode fail, no rows affected.");
        }
        rs = ps.getGeneratedKeys();
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new SQLException("Creating postcode fail, no id obtained.");
    }

    int getCountryId(Map<String, String> map) throws SQLException {
        String query = "SELECT id FROM country WHERE name = ? AND code = ?";
        PreparedStatement ps = connection.prepareStatement(query);
        ps.setString(1, map.get("countryName"));
        ps.setString(2, map.get("countryCode"));
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return rs.getInt("id");
        }
        query = "INSERT INTO country (name, code, long_code) values (?, ?, ?)";
        ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, map.get("countryName"));
        ps.setString(2, map.get("countryCode"));
        ps.setString(3, map.get("longCountryCode"));
        int affectedRows = ps.executeUpdate();
        if (affectedRows == 0) {
            throw new SQLException("Creating region fail, no rows affected.");
        }
        rs = ps.getGeneratedKeys();
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new SQLException("Creating region fail, no id obtained.");
    }

    int getRegionId(Map<String, String> map) throws SQLException {
        String query = "SELECT id FROM region WHERE name = ? AND code = ?";
        PreparedStatement ps = connection.prepareStatement(query);
        ps.setString(1, map.get("regionName"));
        ps.setString(2, map.get("regionCode"));
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return rs.getInt("id");
        }
        query = "INSERT INTO region (name, code, country_id) values (?, ?, ?)";
        ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, map.get("regionName"));
        ps.setString(2, map.get("regionCode"));
        ps.setInt(3, getCountryId(map));
        int affectedRows = ps.executeUpdate();
        if (affectedRows == 0) {
            throw new SQLException("Creating region fail, no rows affected.");
        }
        rs = ps.getGeneratedKeys();
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new SQLException("Creating region fail, no id obtained.");
    }

    int load(InputStream input, boolean truncateBeforeLoad) throws Exception {
        if(null == connection) {
            throw new Exception("Not a valid connection.");
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
        String line;
        String query = "INSERT INTO city (name, latitude, longitude, accuracy, region_id, postcode_id) values (?, ?, ?, ?, ?, ?)";
        int count = 0;
        PreparedStatement ps = null;
        try {
            if (truncateBeforeLoad) {
                Statement statement = connection.createStatement();
                statement.executeUpdate("TRUNCATE TABLE city");
                statement.executeUpdate("TRUNCATE TABLE region");
                statement.executeUpdate("TRUNCATE TABLE country");
                statement.executeUpdate("TRUNCATE TABLE postcode");
                statement.close();
            }
            ps = connection.prepareStatement(query);
            Map<String, String> map;
            while ((line = bufferedReader.readLine()) != null) {
                try {
                    map = parseLine(line);
                } catch (Exception e) {
                    System.out.println("Incorrect line '" + line + "'");
                    continue;
                }
                int regionId = getRegionId(map);
                int postCodeId = getPostCodeId(map);
                String selectQuery = "SELECT id FROM city WHERE name = ? AND region_id = ? AND postcode_id = ?";
                PreparedStatement statement = connection.prepareStatement(selectQuery);
                statement.setString(1, map.get("cityName"));
                statement.setInt(2, regionId);
                statement.setInt(3, postCodeId);
                ResultSet rs = statement.executeQuery();
                if (rs.next()) {
                    System.out.println("City " + map.get("cityName") + " already exist and skip.");
                    continue;
                }
                ps.setString(1, map.get("cityName"));
                ps.setDouble(2, Double.parseDouble(map.get("latitude")));
                ps.setDouble(3, Double.parseDouble(map.get("longitude")));
                ps.setInt(4, Integer.parseInt(map.get("accuracy")));
                ps.setInt(5, regionId);
                ps.setInt(6, postCodeId);
                ps.addBatch();
                count ++;
                if (count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
            if (ps != null) {
                ps.close();
            }
        }
        return count;
    }

    Map<String, String> parseLine(String line) throws IllegalArgumentException {
        String[] array = line.split("\t");
        if (array.length < 12) {
            throw new IllegalArgumentException("Incorrect data");
        }
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
