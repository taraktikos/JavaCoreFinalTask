package ua.org.taraktikos.study;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taras on 28.11.14.
 */
public class DataLoader {

    Connection connection;
    final int batchSize = 1000;

    public DataLoader(Connection connection) {
        this.connection = connection;
    }

    int load(InputStream inputStream) throws Exception {
        return load(inputStream, false);
    }

    int getPostCodeId(String name) {
        String query = "SELECT id FROM postcode WHERE name = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setString(1, name);
            ps.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return -1;
    }

    int load(InputStream input, boolean truncateBeforeLoad) throws Exception {
        if(null == connection) {
            throw new Exception("Not a valid connection.");
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
        String line;
        String query = "INSERT INTO postcode (name) values (?)";
        int count = 0;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(query);
            while ((line = bufferedReader.readLine()) != null) {
                Map<String, String> map = parseLine(line);
                ps.setString(1, map.get("postalCode"));
                ps.addBatch();
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.getStackTrace();
        } finally {
            connection.close();
            if (ps != null) {
                ps.close();
            }
        }
        return count;
        /*CSVReader csvReader = null;


        String[] headerRow = csvReader.readNext();

        if (null == headerRow) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file." +
                            "Please check the CSV file format.");
        }

        String questionmarks = StringUtils.repeat("?,", headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
        query = query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
        query = query.replaceFirst(VALUES_REGEX, questionmarks);

        System.out.println("Query: " + query);

        String[] nextLine;
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = this.connection;
            con.setAutoCommit(false);
            ps = con.prepareStatement(query);

            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                con.createStatement().execute("DELETE FROM " + tableName);
            }

            final int batchSize = 1000;
            int count = 0;
            Date date = null;
            while ((nextLine = csvReader.readNext()) != null) {

                if (null != nextLine) {
                    int index = 1;
                    for (String string : nextLine) {
                        date = DateUtil.convertToDate(string);
                        if (null != date) {
                            ps.setDate(index++, new java.sql.Date(date
                                    .getTime()));
                        } else {
                            ps.setString(index++, string);
                        }
                    }
                    ps.addBatch();
                }
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch(); // insert remaining records
            con.commit();
        } catch (Exception e) {
            con.rollback();
            e.printStackTrace();
            throw new Exception(
                    "Error occured while loading data from file to database."
                            + e.getMessage());
        } finally {
            if (null != ps)
                ps.close();
            if (null != con)
                con.close();

            csvReader.close();
        }*/
        //return false;
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
