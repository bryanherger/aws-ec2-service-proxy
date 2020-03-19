package com.vertica.aws;

import com.vertica.devops.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;

// BH: creating an interface seemed like a good idea at the time, but I don't seem to be using it here
public class AwsVerticaService implements CloudServiceInterface {
    final static Logger LOG = LogManager.getLogger(AwsVerticaService.class);
    public static String aliveQuery = "select /*+label(proxytestquery)*/ version();\n";
    public static String activeQuery = "select /*+label(proxytestquery)*/ * from query_requests where request_label <> 'proxytestquery' AND request <> 'select 1' AND (end_timestamp > (current_timestamp - interval '15 minutes') or end_timestamp is null) order by end_timestamp desc limit 25;";
    public static String nodesQuery = "select /*+label(proxytestquery)*/ * from v_catalog.nodes;\n";
    public static String storageLocationsQuery = "select /*+label(proxytestquery)*/ * from storage_locations;\n";
    public static String subclustersQuery = "select /*+label(proxytestquery)*/ DISTINCT subcluster_oid,subcluster_name,parent_oid,parent_name,is_default,is_primary from v_catalog.subclusters;\n";

    public Map<String,VerticaNodeGroup> subclusters = new HashMap<>();
    public List<VerticaNode> nodes = new ArrayList<>();

    public static String getVerticaAmiId() {
        // this currently gets AMI's with Amazon Linux and v9.3.1-2.  You may have to subscribe to the AMI first via the AWS Marketplace.
        //return "ami-0e8b1767863a67aa7"; // Vertica by the hour
        return "ami-05c08427801571a43"; // Vertica BYOL
    }

    // read cluster described in "params", populate nodes and subclusters lists, return ... ? status ?
    public String getClusterState(Properties params) {
        List<String> resC = runQueryWithResult(params, subclustersQuery);
        for (String res : resC) {
            VerticaNodeGroup vng = new VerticaNodeGroup(res);
            subclusters.put(vng.subcluster_name,vng);
        }
        List<String> resS = runQueryWithResult(params, nodesQuery);
        for (String res : resS) {
            VerticaNode vn = new VerticaNode(res);
            nodes.add(vn);
        }
        return null;
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

    // install Vertica.  Should be primary/default subcluster only
    public void installVertica(Properties params) throws Exception {
        LOG.info("params:");
        params.list(System.out);
        String allIp = params.getProperty("allNodes");
        // for Eon mode, we also need to upload a credential file
        LOG.info("Will use node "+params.getProperty("node")+" to install Vertica on "+allIp);
        SshUtil ssh = new SshUtil();
        ssh.sftp(params, params.getProperty("VERTICA_PEM_KEYFILE"), "/tmp/keyfile.pem");
        // exec command(s)
        List<String> commands = new ArrayList<String>();
        // only Eon mode is supported here
        commands.add("sudo /opt/vertica/sbin/install_vertica -i /tmp/keyfile.pem --debug --license CE --accept-eula --hosts "+allIp+" --dba-user-password-disabled --failure-threshold NONE -d "+params.getProperty("DBDATADIR"));
        commands.add("sudo -u dbadmin /opt/vertica/bin/admintools -t revive_db --force -s "+allIp+" -d "+params.getProperty("DBNAME")+" -x /tmp/eonaws.conf --communal-storage-location="+params.getProperty("DBS3BUCKET"));
        commands.add("sudo -u dbadmin /opt/vertica/bin/admintools -t start_db -i -d "+params.getProperty("DBNAME")+" -p "+params.getProperty("DBPASS"));
        // if selected, create and upload a credential file for Eon mode
        File tf = File.createTempFile("eonaws",".cnf");
        BufferedWriter bw = new BufferedWriter(new FileWriter(tf));
        bw.write("awsauth = "+params.getProperty("awsAccessKeyID")+":"+params.getProperty("awsSecretAccessKey")); bw.newLine();
        bw.write("awsregion = "+params.getProperty("awsRegion")); bw.newLine();
        bw.flush(); bw.close();
        LOG.info("Uploading "+tf.getCanonicalPath());
        ssh.sftp(params, tf.getCanonicalPath(), "/tmp/eonaws.conf");
        //commands.add("cat /opt/vertica/log/adminTools.log");
        //String command = install_vertica;
        for (String command : commands) {
            ssh.ssh(params, command);
        }
        // test Vertica
        LOG.info(checkState(params));
        // populate node and subcluster list
        getClusterState(params);
    }

    // manage subclusters
    public boolean eonAddSubcluster(Properties targets, String scName, List<String> hostIps) {
        SshUtil ssh = new SshUtil();
        // ask the cloud provider class to create instances
        if (hostIps == null) {
            AwsCloudProvider acp = new AwsCloudProvider();
            acp.init(targets);
            acp.createVerticaNodes(targets, scName, 3, "i3.4xlarge");
            hostIps = new ArrayList<>(acp.instanceIdIpMap.keySet());
            List<String> publicIps = new ArrayList<>();
            for (AwsInstance i : acp.instances) {
                publicIps.add(i.publicDns);
            }
            acp.configureInstances(targets, publicIps);
        }
        String addHosts = String.join(",",hostIps);
        LOG.info("Add subcluster "+scName+" with instances at "+addHosts);
        // install_vertica to add nodes
        String addNodes = "sudo /opt/vertica/sbin/install_vertica -i /tmp/keyfile.pem -d /vertica --add-hosts "+addHosts+" --failure-threshold NONE";
        try {
            ssh.ssh(targets, addNodes);
        } catch (Exception e) {
            LOG.error(e);
            return false;
        }
        // example from docs.  TODO: read parameters and substitute
        String addSubcluster = "sudo -u dbadmin /opt/vertica/bin/admintools -t db_add_subcluster -s "+addHosts+" --is-secondary -c "+scName+" -d "+targets.getProperty("DBNAME")+" -p "+targets.getProperty("DBPASS");
        try {
            ssh.ssh(targets, addSubcluster);
        } catch (Exception e) {
            LOG.error(e);
            return false;
        }
        // update cluster data
        getClusterState(targets);

        return true;
    }

    // manage subclusters: provision instances and Vertica on new secondary subcluster to be used for proxy queries
    public boolean eonAddProxySubcluster(Properties targets, String scName, int nodeCount, String nodeType) {
        SshUtil ssh = new SshUtil();
        // ask the cloud provider class to create instances
        AwsCloudProvider acp = new AwsCloudProvider();
        acp.init(targets);
        acp.createVerticaNodes(targets, scName, nodeCount, nodeType);
        List<String> hostIps = new ArrayList<>();
        List<String> publicIps = new ArrayList<>();
        List<String> toTag = new ArrayList<>();
        for (AwsInstance i : acp.instances) {
            hostIps.add(i.privateIp);
            publicIps.add(i.publicDns);
            toTag.add(i.instanceId);
            // this will choose the last one...
            targets.setProperty("scnode",i.publicDns);
        }
        acp.tagInstances(toTag, "Name", "Vertica-"+scName);
        acp.configureInstances(targets, publicIps);
        String addHosts = String.join(",",hostIps);
        LOG.info("Add subcluster "+scName+" with instances at "+addHosts);
        // install_vertica to add nodes
        //String dbuser = targets.getProperty("DBUSER");
        //targets.setProperty("DBUSER","ec2-user");
        String addNodes = "sudo /opt/vertica/sbin/install_vertica -i /tmp/keyfile.pem -d /vertica --add-hosts "+addHosts+" --failure-threshold NONE";
        LOG.info(addNodes);
        try {
            ssh.ssh(targets, addNodes);
        } catch (Exception e) {
            LOG.error(e);
            return false;
        }
        // example from docs.  TODO: read parameters and substitute
        String addSubcluster = "sudo -u dbadmin /opt/vertica/bin/admintools -t db_add_subcluster -s "+addHosts+" --is-secondary -c "+scName+" -d "+targets.getProperty("DBNAME")+" -p "+targets.getProperty("DBPASS");
        LOG.info(addSubcluster);
        try {
            ssh.ssh(targets, addSubcluster);
        } catch (Exception e) {
            LOG.error(e);
            return false;
        }
        // update cluster data
        getClusterState(targets);
        //targets.setProperty("DBUSER",dbuser);
        return true;
    }

    public boolean eonStartSubcluster(Properties targets, String scName) {
        // example from docs.  TODO: read parameters and substitute
        String stopDb = "sudo -u dbadmin /opt/vertica/bin/admintools -t restart_subcluster -c "+scName+" -d "+targets.getProperty("DBNAME")+" -p "+targets.getProperty("DBPASS");
        SshUtil ssh = new SshUtil();
        try {
            ssh.ssh(targets, stopDb);
        } catch (Exception e) {
            LOG.error(e);
        }
        return false;
    }

    public boolean eonStopSubcluster(Properties targets, String scName) {
        // example from docs.  TODO: read parameters and substitute
        String stopDb = "sudo -u dbadmin /opt/vertica/bin/admintools -t stop_subcluster -c "+scName+" -d "+targets.getProperty("DBNAME")+" -p "+targets.getProperty("DBPASS");
        SshUtil ssh = new SshUtil();
        try {
            ssh.ssh(targets, stopDb);
        } catch (Exception e) {
            LOG.error(e);
        }
        return false;
    }

    public boolean eonRemoveSubcluster(Properties targets, String scName) {
        // example from docs.  TODO: read parameters and substitute
        String stopDb = "sudo -u dbadmin /opt/vertica/bin/admintools -t db_remove_subcluster -c "+scName+" -d "+targets.getProperty("DBNAME")+" -p "+targets.getProperty("DBPASS");
        SshUtil ssh = new SshUtil();
        try {
            ssh.ssh(targets, stopDb);
        } catch (Exception e) {
            LOG.error(e);
        }
        // remove nodes from cluster with install_vertica

        // ask the cloud provider class to terminate instances

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
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
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

    public List<String> runQueryWithResult(Properties target, String query) {
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            LOG.error("Could not find the JDBC driver class.", e);
            return null; // Exit. Cannot do anything further.
        }
        Properties myProp = new Properties();
        myProp.put("user", target.getProperty("DBUSER","dbadmin"));
        myProp.put("password", target.getProperty("DBPASS",""));
        myProp.put("loginTimeout", "60");
        Connection conn;
        try {
            List<String> results = new ArrayList<>();
            conn = DriverManager.getConnection("jdbc:vertica://"+target.getProperty("node")+":"+target.getProperty("DBPORT","5433")+"/"+target.getProperty("DBNAME","vertica"), myProp);
            LOG.info("Connected! Query = "+query);
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                List<String> contents = new ArrayList<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    contents.add(rs.getString(i));
                }
                LOG.info("RS[" + rs.getRow() + "] = " + String.join("|", contents));
                results.add(String.join("|", contents));
            }
            conn.close();
            return results;
        } catch (SQLTransientConnectionException connException) {
            // There was a potentially temporary network error
            // Could automatically retry a number of times here, but
            // instead just report error and exit.
            LOG.error("Network connection issue: ");
            LOG.error(connException.getMessage());
            LOG.error(" Try again later!");
            return null;
        } catch (SQLInvalidAuthorizationSpecException authException) {
            // Either the username or password was wrong
            LOG.error("Could not log into database: ");
            LOG.error(authException.getMessage());
            LOG.error(" Check the login credentials and try again.");
            return null;
        } catch (Exception e) {
            // Catch-all for other exceptions
            LOG.error(e);
            e.printStackTrace();
            return null;
        }
    }
}
