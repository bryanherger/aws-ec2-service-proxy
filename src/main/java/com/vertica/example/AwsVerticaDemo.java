package com.vertica.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.vertica.aws.AwsCloudProvider;
import com.vertica.aws.AwsVerticaService;
import com.vertica.devops.AwsInstance;
import com.vertica.devops.SshUtil;
import org.apache.log4j.*;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import software.amazon.awssdk.utils.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.ServerSocket;
import java.util.*;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class AwsVerticaDemo {
    final static Logger LOG = LogManager.getLogger(AwsVerticaDemo.class);
    private static String awsAccessKeyID = "X"
            , awsSecretAccessKey = "X"
            , awsRegion = "X", awsKeyPairName = "X"
            , awsS3Bucket = null;
    public static String DBUSER = "X", DBPASS = "X", DBNAME = "X", DBPORT = "5433", VERTICA_PEM_KEYFILE = "X"
            , DBS3BUCKET = "s3://X/Y", DBDATADIR = "/X", DBPROXY = "35433:5433";

    public static void main(String[] argv) throws Exception {
        // parse command line args
        Args args = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(args)
                .build();
        jc.parse(argv);
        //This is the root logger provided by log4j
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);

//Define log pattern layout
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

//Add console appender to root logger
        rootLogger.addAppender(new ConsoleAppender(layout));

        // run test(s)
        Properties params = new Properties();
        LOG.info("AwsVerticaDemo: args " + argv.length);
        if (args.help) {
            LOG.error("(help option?)");
            jc.usage();
            System.exit(0);
        }
        if (args.spot) {
            LOG.error("!$ using spot instances");
            params.setProperty("spotInstances","true");
        } else {
            LOG.error("!$ using on-demand instances");
        }
        // Eon mode for spot currently assumes we are reviving an existing database
        if (args.eonMode) {
            LOG.error("!V using Eon mode DB");
            params.setProperty("eonMode","true");
        } else {
            LOG.error("!V using EE mode DB");
        }
        // load config file if specified and override
        if (args.propertiesFile != null) {
            LOG.info("Reading properties from "+args.propertiesFile);
            params.load(new FileReader(args.propertiesFile));
        }
        // set from command line
        /*params.setProperty("awsAccessKeyID", awsAccessKeyID);
        params.setProperty("awsSecretAccessKey", awsSecretAccessKey);
        params.setProperty("awsRegion", awsRegion);
        params.setProperty("awsKeyPairName", awsKeyPairName);*/
        if (StringUtils.isNotBlank(args.communalStorage)) { params.setProperty("awsS3Bucket", args.communalStorage); }
        if (StringUtils.isNotBlank(args.clusterSize)) { params.setProperty("clusterSize", args.clusterSize); }
        if (StringUtils.isNotBlank(args.primarySubcluster)) { params.setProperty("DBPRIMARY", args.primarySubcluster); }
        if (StringUtils.isNotBlank(args.secondarySubcluster)) { params.setProperty("DBSECONDARY", args.secondarySubcluster); }
        if (StringUtils.isNotBlank(args.sshIdentityFile)) { params.setProperty("VERTICA_PEM_KEYFILE", args.sshIdentityFile); }
        if (StringUtils.isNotBlank(args.sshNode)) { params.setProperty("DBCONTROLNODE", args.sshNode); }
        if (StringUtils.isNotBlank(args.dbLicense)) { params.setProperty("DBLICENSE", args.dbLicense); }
        if (StringUtils.isNotBlank(args.dbName)) { params.setProperty("DBNAME", args.dbName); }
        if (StringUtils.isNotBlank(args.dbCluster)) { params.setProperty("DBCLUSTER", args.dbCluster); }
        if (StringUtils.isNotBlank(args.dbPassword)) { params.setProperty("DBPASS", args.dbPassword); }
        if (StringUtils.isNotBlank(args.dbUser)) { params.setProperty("DBUSER", args.dbUser); }
        //params.setProperty("DBPORT", DBPORT);
        //params.setProperty("DBS3BUCKET",DBS3BUCKET);
        //params.setProperty("DBDATADIR",DBDATADIR);
        if (StringUtils.isNotBlank(args.tagBaseName)) {
            params.setProperty("tagBaseName",args.tagBaseName);
        }
        // if -t/--task (admintools style) flag, run the specified task
        if (StringUtils.isNotBlank(args.dbTask)) {
            params.setProperty("DBTASK", args.dbTask);
            VerticaTasks vt = new VerticaTasks(params);
            try {
                if (args.dbTask.equalsIgnoreCase("create_db")) {
                    vt.createDatabase(params);
                }
                if (args.dbTask.equalsIgnoreCase("revive_db")) {
                    vt.reviveDatabase(params);
                }
                if (args.dbTask.equalsIgnoreCase("create_subcluster")) {
                    vt.createSubcluster(params);
                }
                if (args.dbTask.equalsIgnoreCase("get_status")) {
                    //vt.getStatus(params);
                    // do nothing here since we'll always print status at end of run.
                }
                if (args.dbTask.equalsIgnoreCase("db_proxy")) {
                    vt.proxyServer(params);
                }
                if (args.dbTask.equalsIgnoreCase("remove_subcluster")) {
                    vt.removeSubcluster(params);
                }
                if (args.dbTask.equalsIgnoreCase("stop_db")) {
                    vt.stopDatabase(params);
                }
                vt.getStatus(params);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            System.exit(0);
        }
        if (args.proxyPorts != null && args.proxyPorts.contains(":")) {
            params.setProperty("DBPROXY", args.proxyPorts);
            proxyDemoEon(params);
            System.exit(0);
        }
        if ("proxy".equalsIgnoreCase(args.demoMode)) {
            params.setProperty("DBPROXY", DBPROXY);
            proxyDemo(params);
        } else if ("proxydemo".equalsIgnoreCase(args.demoMode)) {
            params.setProperty("DBPROXY", DBPROXY);
            proxyDemo(params);
        } else {
            spotInstanceDemo(params);
        }
        System.exit(0);
    }

    public static void spotInstanceDemo(Properties params) {
        // demo spot requests: create then destroy
        AwsCloudProvider acp = new AwsCloudProvider();
        acp.init(params);
        params.setProperty("serviceTag","VerticaSpotDemo");
        //acp.createInstances(params);
        acp.createVerticaNodes(params, "default", 3, /*"c5.large"*/"i3.large");
        LOG.info("First instance (out of "+acp.instances.size()+") of first cluster: "+acp.instances.get(0).toString());
        String publicIp = acp.instances.get(0).publicDns;
        String asimss = acp.checkState(params);
        LOG.info("spot state: "+asimss);
        List<String> ips = new ArrayList<>();
        List<String> publicIps = new ArrayList<>();
        for (String ip : asimss.split(";;")) {
            String findDns[] = ip.split("\\|");
            if (findDns.length == 5 && !StringUtils.isEmpty(findDns[4])) {
                LOG.info("private IP: "+findDns[4]);
                publicIps.add(findDns[3]);
                ips.add(findDns[4]);
            }
        }
        try { Thread.sleep(10000L); } catch (Exception e) { }
        LOG.info("Using public IP or DNS: "+publicIp);
        try {
            params.setProperty("node", publicIp);
            params.setProperty("allNodes", String.join(",", ips));
            // configure instances
            acp.configureInstances(params, publicIps);
            // install and start DB
            /* */
            AwsVerticaService avs = new AwsVerticaService();
            avs.installVertica(params);
            // how fast can we add a subcluster?
            LOG.error("Start adding ondemand-subcluster");
            avs.eonAddSubcluster(params, "ondemand-subcluster", null);
            LOG.error("Finish adding ondemand-subcluster");
            // tests
            avs.runQuery(params, "SELECT REBALANCE_SHARDS();");
            avs.runQuery(params, "SELECT * FROM NODES;");
            // remove
            avs.eonRemoveSubcluster(params, "ondemand-subcluster");
            avs.runQuery(params, "SELECT REBALANCE_SHARDS();");
            // stop DB
            avs.destroyServices(params);
            /* */
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        // let the scheduler kill the instances
        params.setProperty("stopBehavior","terminate");
        acp.destroyInstances(params);
        //scheduleInit(params);
        //try { LOG.error("Waiting 600 seconds before exiting!"); Thread.sleep(600L*1000L); } catch (Exception e) { }
    }

    public static void proxyDemo(Properties params) throws Exception {
        AwsCloudProvider acp = new AwsCloudProvider();
        acp.init(params);
        params.setProperty("instanceTag","Service=VHibernateDemo");
        String instances = acp.getInstancesByTag(params);
        LOG.info("instances: "+instances);
        params.setProperty("instances", instances);
        acp.startInstances(params);
        String cs = acp.checkState(params);
        LOG.info(cs);
        params.setProperty("DBNAME", DBNAME);
        params.setProperty("DBPORT", DBPORT);
        params.setProperty("DBPASS", DBPASS);
        params.setProperty("DBUSER", DBUSER);
        AwsVerticaService avs = new AwsVerticaService();
        String[] findDns = cs.split(";;");
        for (String findDns1 : findDns) {
            String findDns1a[] = findDns1.split("\\|");
            if (findDns1a.length == 5 && !StringUtils.isEmpty(findDns1a[3])) {
                LOG.info("using node "+findDns1a[3]);
                params.setProperty("node", findDns1a[3]);
                break;
            }
        }
        if (!StringUtils.isEmpty(params.getProperty("node"))) {
            avs.checkState(params);
            scheduleInit(params);
            // TODO: convert to parameters
            String[] proxyPorts = params.getProperty("DBPROXY").split(":");
            int localport = Integer.parseInt(proxyPorts[0]);
            int remoteport = Integer.parseInt(proxyPorts[1]);
            // Print a start-up message
            System.out.println("Starting proxy for " + params.getProperty("node") + ":" + remoteport
                    + " on local port " + localport);
            ServerSocket server = new ServerSocket(localport);
            while (true) {
                new ThreadProxy(server.accept(), params.getProperty("node"), remoteport, acp, params);
            }
        } else {
            LOG.error("No running Vertica node found!");
        }
        acp.stopInstances(params);
        LOG.info(acp.checkState(params));
    }

    // Eon mode proxy demo:
    public static void proxyDemoEon(Properties params) throws Exception {
        if (!StringUtils.isEmpty(params.getProperty("DBPRIMARY"))) {
            String dbPrimary = params.getProperty("DBPRIMARY");
            LOG.info("Contacting AWS API for info on: "+dbPrimary);
            String[] pargs = dbPrimary.split(":");
            AwsCloudProvider aws = new AwsCloudProvider(); aws.init(params);
            // revive primary cluster if needed
            boolean revive = false;
            do {
                revive = false;
                aws.getInstancesById(new HashSet<String>(Arrays.asList(pargs[1].split("\\,"))));
                for (AwsInstance a : aws.instances) {
                    if (a.publicIp == null) {
                        LOG.error("DOWN (" + a + "): " + a.state);
                        revive = true;
                        break;
                    }
                }
                if (revive) {
                    aws.startInstances(Arrays.asList(pargs[1].split("\\,")));
                    try { Thread.sleep(5000L); } catch (Exception e) { }
                }
            } while (revive);
            String dbControlNode = aws.instances.get(0).publicDns;
            params.setProperty("DBCONTROLNODE", dbControlNode);
            LOG.info("Primary subcluster is ready and we will use node: "+dbControlNode);
        }
        // check required settings
        if (StringUtils.isEmpty(params.getProperty("DBCONTROLNODE"))) {
            LOG.error("Need to specify a control node in an existing primary subcluster");
            return;
        }
        params.setProperty("node", params.getProperty("DBCONTROLNODE"));
        AwsVerticaService avs = new AwsVerticaService();
        if (!StringUtils.isEmpty(params.getProperty("DBSECONDARY"))) {
            LOG.warn("We will create and manage a secondary subcluster on spot instances to handle queries");
            // name:nodeCount:instanceType
            String[] secondary = params.getProperty("DBSECONDARY").split(":");
            LOG.info("subcluster "+secondary[0]+" will have "+secondary[1]+"x "+secondary[2]);
            // TODO: add secondary subcluster metadata so monitor job will have it
            params.setProperty("secondarySubcluster",secondary[0]);
            params.setProperty("instanceTag", "SecondarySC-SpotInstance");
            params.setProperty("serviceTag", "SecondarySC-Spot-Vertica");
            avs.eonAddProxySubcluster(params, secondary[0], Integer.parseInt(secondary[1]), secondary[2]);
            // TODO: now set the control node (proxy target) to the new secondary subcluster
            params.setProperty("node", params.getProperty("scnode"));
            LOG.info("ACP selected proxy destination node: "+params.getProperty("node"));
            avs.checkState(params);
            // TODO: init scheduler so monitor threads and stop subcluster as needed
            // scheduleInit(params);
            // well, just stop here for now
            avs.eonRemoveSubcluster(params, secondary[0]);
            AwsCloudProvider aws = new AwsCloudProvider(); aws.init(params); aws.destroyInstances(params);
            System.exit(34);
        }
        if (!StringUtils.isEmpty(params.getProperty("node"))) {
            avs.checkState(params);
            //scheduleInit(params);
            String[] proxyPorts = params.getProperty("DBPROXY").split(":");
            int localport = Integer.parseInt(proxyPorts[0]);
            int remoteport = Integer.parseInt(proxyPorts[1]);
            // Print a start-up message
            System.out.println("Starting proxy for " + params.getProperty("node") + ":" + remoteport
                    + " on local port " + localport);
            ServerSocket server = new ServerSocket(localport);
            while (true) {
                new ThreadProxy(server.accept(), params.getProperty("node"), remoteport);
            }
        } else {
            LOG.error("No running Vertica node found!");
        }
    }

    public static void scheduleInit(Properties params) {
        try {
            SchedulerFactory sf = new StdSchedulerFactory();
            Scheduler sched = sf.getScheduler();
            JobDataMap jdm = new JobDataMap();
            jdm.put("params", params);
            JobDetail job = newJob(MonitorJob.class).withIdentity("job1", "group1").usingJobData(jdm).build();
            // TODO: make the schedule configurable
            Trigger trigger = newTrigger()
                    .withIdentity("trigger1", "group1")
                    .withSchedule(cronSchedule("0 0/2 * * * ?"))
                    .build();
            sched.scheduleJob(job, trigger);
            sched.start();
            LOG.info("--- Started Scheduler");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Deprecated
    public static void verticaOnSpot(Properties params) throws Exception {
        LOG.info("params:");
        params.list(System.out);
        String allIp = params.getProperty("allNodes");
        // for Eon mode, we also need to upload a credential file
        LOG.info("Will use node "+params.getProperty("node")+" to install Vertica on "+allIp);
        SshUtil ssh = new SshUtil();
        ssh.sftp(params, params.getProperty("VERTICA_PEM_KEYFILE"), "/tmp/keyfile.pem");
        // exec command(s)
        List<String> commands = new ArrayList<String>();
        if (params.containsKey("eonMode")) {
            // Eon mode
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
        } else {
            // EE mode
            commands.add("sudo /opt/vertica/sbin/install_vertica -i /tmp/keyfile.pem --debug --license CE --accept-eula --hosts "+allIp+" --dba-user-password-disabled --failure-threshold NONE --no-system-configuration");
            commands.add("sudo -u dbadmin /opt/vertica/bin/admintools -t create_db --skip-fs-checks -s "+allIp+" -d "+params.getProperty("DBNAME")+" -p "+params.getProperty("DBPASS"));
        }
        //commands.add("cat /opt/vertica/log/adminTools.log");
        //String command = install_vertica;
        for (String command : commands) {
            ssh.ssh(params, command);
        }
        // test Vertica
        AwsVerticaService avs = new AwsVerticaService();
        LOG.info(avs.checkState(params));
        // populate node and subcluster list
        avs.getClusterState(params);
        // let's try adding a new secondary subcluster
        avs.runQuery(params, "SELECT REBALANCE_SHARDS();");
        // destroy all the things!

    }
}

class Args {
    // command line parsing: see http://jcommander.org/#_overview
    // in the help output from usage(), it looks like options are printed in order of long option name, regardless of order here
    @Parameter(names = {"--properties"}, description = "Properties file (java.util.Properties format) (if omitted, use command line settings, or fall back to defaults)")
    public String propertiesFile = "d:\\temp\\github\\eonaws.properties";
    @Parameter(names = {"--tag"}, description = "Tag name for resources (if omitted, use AvsVerticaDemo)")
    public String tagBaseName = "";
    @Parameter(names = {"--demomode"}, description = "Which demo mode to run (if omitted or invalid, demo spot instances and exit)")
    public String demoMode = null;
    @Parameter(names = {"-d","--database"}, description = "Vertica database name")
    public String dbName = "";
    @Parameter(names = {"-c","--cluster"}, description = "Vertica database subcluster name")
    public String dbCluster = "";
    @Parameter(names = {"-u","--dbuser"}, description = "Vertica database user")
    public String dbUser = "";
    @Parameter(names = {"-p","--dbpassword"}, description = "Vertica database password")
    public String dbPassword = "";
    @Parameter(names = {"-P","--ports"}, description = "For proxy service, <local port>:<remote port> (implicitly sets proxy mode.  if omitted or invalid, no proxy service)")
    public String proxyPorts = null;
    @Parameter(names = {"--communal-storage"}, description = "Communal storage location (S3 bucket) (if omitted or invalid, no default, error will occur with Eon mode. This setting is ignored for EE mode)")
    public String communalStorage = "";
    @Parameter(names = {"-n","--node"}, description = "Node to use to manage Vertica - primary subcluster node with SSH keys (-p/--primary is checked first. if omitted or invalid, no default, we will try to look up from AWS and error out if we can't find a node)")
    public String sshNode = "";
    @Parameter(names = {"-i","--identity"}, description = "SSH identity file (PEM private key, usually) (required in most cases, unless you set a default in the code)")
    public String sshIdentityFile = "";
    @Parameter(names = {"-l","--license"}, description = "Vertica license file (default: use CE)")
    public String dbLicense = "CE";
    @Parameter(names = {"--primary-subcluster"}, description = "CSV list of primary subcluster nodes as <id type>:node0,...,nodeX (where <id type> is what is in the CSV, supported: instanceId, privateIp. if omitted, try to discover from AWS API or implied from other settings like -n/--node")
    public String primarySubcluster = "";
    @Parameter(names = {"--clustersize"}, description = "Create nodes with name:nodeCount:instanceType (default: create 1x i3.large nodes named \"VerticaNode\")")
    public String clusterSize = "";
    @Parameter(names = {"-s","--secondary-subcluster"}, description = "Create a secondary subcluster with name:nodeCount:instanceType (if omitted or invalid, no default, no secondary subcluster will be created, may error out)")
    public String secondarySubcluster = "";
    @Parameter(names = {"-t","--task"}, description = "Run a task: create_db,revive_db,create_subcluster,get_status,db_proxy,remove_subcluster,stop_db")
    public String dbTask = "";
    @Parameter(names = {"-S","--spot"}, description = "Create spot instances (default: on-demand)")
    public boolean spot = false;
    @Parameter(names = {"-e","--eonmode"}, description = "Create Eon mode DB")
    public boolean eonMode = true;
    @Parameter(names = {"-h","-?","--help"}, help = true, description = "Print this message")
    public boolean help;
}