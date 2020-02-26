package com.vertica.aws;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Properties;

public class VerticaUtil {
    public static String aliveQuery = "select /*+label(proxytestquery)*/ version();\n";
    public static String activeQuery = "select /*+label(proxytestquery)*/ * from query_requests where request_label <> 'proxytestquery' AND request <> 'select 1' AND (end_timestamp > (current_timestamp - interval '15 minutes') or end_timestamp is null) order by end_timestamp desc;";
    private static String DBUSER = "dbadmin", DBPASS = "", DBNAME = "hibernate";
    final static Logger LOG = LogManager.getLogger(VerticaUtil.class);

    public static void init() {

    }

    public static boolean checkIfAlive(String verticaNode) {
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            LOG.error("Could not find the JDBC driver class.", e);
            return false; // Exit. Cannot do anything further.
        }
        Properties myProp = new Properties();
        myProp.put("user", DBUSER);
        myProp.put("password", DBPASS);
        myProp.put("loginTimeout", "60");
        Connection conn;
        try {
            conn = DriverManager.getConnection("jdbc:vertica://"+verticaNode+":5433/"+DBNAME, myProp);
            LOG.error("Connected!");
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(aliveQuery);
            while (rs.next()) {
                LOG.error("Result set row = " + rs.getRow() + ", content = " + rs.getString(1));
            }
            conn.close();
            return true;
        } catch (SQLTransientConnectionException connException) {
            // There was a potentially temporary network error
            // Could automatically retry a number of times here, but
            // instead just report error and exit.
            LOG.error("Network connection issue: ");
            LOG.error(connException.getMessage());
            LOG.error(" Try again later!");
            return false;
        } catch (SQLInvalidAuthorizationSpecException authException) {
            // Either the username or password was wrong
            LOG.error("Could not log into database: ");
            LOG.error(authException.getMessage());
            LOG.error(" Check the login credentials and try again.");
            return false;
        } catch (Exception e) {
            // Catch-all for other exceptions
            LOG.error(e);
            e.printStackTrace();
            return false;
        }
    }

    public static boolean checkIfActive(String verticaNode) {
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            LOG.error("Could not find the JDBC driver class.", e);
            return false; // Exit. Cannot do anything further.
        }
        Properties myProp = new Properties();
        myProp.put("user", DBUSER);
        myProp.put("password", DBPASS);
        myProp.put("loginTimeout", "60");
        Connection conn;
        try {
            conn = DriverManager.getConnection("jdbc:vertica://"+verticaNode+":5433/"+DBNAME, myProp);
            LOG.error("Connected!");
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(activeQuery);
            int count = 0;
            while (rs.next()) {
                count++;
                LOG.error("Result set row = " + rs.getRow() + ", content = " + rs.getString(1));
            }
            conn.close();
            if (count > 0) {
                return true;
            }
            return false;
        } catch (SQLTransientConnectionException connException) {
            // There was a potentially temporary network error
            // Could automatically retry a number of times here, but
            // instead just report error and exit.
            LOG.error("Network connection issue: ");
            LOG.error(connException.getMessage());
            LOG.error(" Try again later!");
            return false;
        } catch (SQLInvalidAuthorizationSpecException authException) {
            // Either the username or password was wrong
            LOG.error("Could not log into database: ");
            LOG.error(authException.getMessage());
            LOG.error(" Check the login credentials and try again.");
            return false;
        } catch (Exception e) {
            // Catch-all for other exceptions
            LOG.error(e);
            e.printStackTrace();
            return false;
        }
    }

}
