package com.vertica.aws;

import com.vertica.devops.CloudServiceInterface;
import com.vertica.devops.SshUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AwsVerticaService implements CloudServiceInterface {
    final static Logger LOG = LogManager.getLogger(AwsVerticaService.class);
    public static String aliveQuery = "select /*+label(proxytestquery)*/ version();\n";
    public static String activeQuery = "select /*+label(proxytestquery)*/ * from query_requests where request_label <> 'proxytestquery' AND request <> 'select 1' AND (end_timestamp > (current_timestamp - interval '15 minutes') or end_timestamp is null) order by end_timestamp desc limit 25;";
    public static String nodesQuery = "select /*+label(proxytestquery)*/ * from nodes;\n";
    public static String storageLocationsQuery = "select /*+label(proxytestquery)*/ * from storage_locations;\n";

    public static String getVerticaAmiId() {
        // this currently gets AMI's with Amazon Linux and v9.3.1-2.  You may have to subscribe to the AMI first via the AWS Marketplace.
        //return "ami-0e8b1767863a67aa7"; // Vertica by the hour
        return "ami-05c08427801571a43"; // Vertica BYOL
    }

    @Override
    public boolean createServices(Properties targets) {
        // create or revive (from hibernate) EE, or create or revive (from S3) Eon mode
        if (targets.containsKey("eonMode")) {
            return createEonServices(targets);
        } else {
            return createEEServices(targets);
        }
    }

    private boolean createEEServices(Properties targets) {
        return false;
    }

    private boolean createEonServices(Properties targets) {
        return false;
    }

    @Override
    public String checkState(Properties targets) {
        boolean c1 = this.runQuery(targets, aliveQuery);
        boolean c2 = this.runQuery(targets, activeQuery);
        boolean c3 = this.runQuery(targets, nodesQuery);
        this.runQuery(targets, storageLocationsQuery);
        if (c1 && c2 && c3) { return "Vertica is OK"; }
        return null;
    }

    @Override
    public boolean alterServices(Properties targets) {
        return false;
    }

    @Override
    public boolean destroyServices(Properties targets) {
        // call appropriate functions to flush catalog and data...
        String stopDb = "sudo -u dbadmin /opt/vertica/bin/admintools -t stop_db -d "+targets.getProperty("DBNAME")+" -p "+targets.getProperty("DBPASS");
        SshUtil ssh = new SshUtil();
        try {
            ssh.ssh(targets, stopDb);
        } catch (Exception e) {
            LOG.error(e);
        }
        return false;
    }

    public boolean runQuery(Properties target, String query) {
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            LOG.error("Could not find the JDBC driver class.", e);
            return false; // Exit. Cannot do anything further.
        }
        Properties myProp = new Properties();
        myProp.put("user", target.getProperty("DBUSER","dbadmin"));
        myProp.put("password", target.getProperty("DBPASS",""));
        myProp.put("loginTimeout", "60");
        Connection conn;
        try {
            conn = DriverManager.getConnection("jdbc:vertica://"+target.getProperty("node")+":"+target.getProperty("DBPORT","5433")+"/"+target.getProperty("DBNAME","vertica"), myProp);
            LOG.info("Connected! Query = "+query);
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                List<String> contents = new ArrayList<>();
                for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
                    contents.add(rs.getString(i));
                }
                LOG.info("RS[" + rs.getRow() + "] = " + String.join("|", contents));
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
}
