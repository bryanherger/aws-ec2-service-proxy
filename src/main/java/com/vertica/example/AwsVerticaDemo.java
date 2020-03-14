package com.vertica.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.jcraft.jsch.*;
import com.vertica.aws.AwsCloudProvider;
import com.vertica.aws.AwsSpotInstanceManager;
import com.vertica.aws.AwsVerticaService;
import com.vertica.devops.SshUtil;
import org.apache.log4j.*;
import org.apache.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import software.amazon.awssdk.utils.StringUtils;

import java.io.*;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class AwsVerticaDemo {
    final static Logger LOG = LogManager.getLogger(AwsVerticaDemo.class);
    private static String awsAccessKeyID = "0"
            , awsSecretAccessKey = "0"
            , awsRegion = "0";
    private static String DBUSER = "X", DBPASS = "X", DBNAME = "X", DBPORT = "0";

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
            LOG.error("!V using EE ode DB");
        }
        // set defaults
        params.setProperty("awsAccessKeyID", awsAccessKeyID);
        params.setProperty("awsSecretAccessKey", awsSecretAccessKey);
        params.setProperty("awsRegion", awsRegion);
        params.setProperty("awsKeyPairName", awsKeyPairName);
        params.setProperty("awsS3Bucket", args.communalStorage);
        params.setProperty("DBSECONDARY", args.secondarySubcluster);
        params.setProperty("VERTICA_PEM_KEYFILE", VERTICA_PEM_KEYFILE);
        params.setProperty("DBNAME", DBNAME);
        params.setProperty("DBPORT", DBPORT);
        params.setProperty("DBPASS", DBPASS);
        params.setProperty("DBUSER", DBUSER);
        params.setProperty("DBS3BUCKET",DBS3BUCKET);
        params.setProperty("DBDATADIR",DBDATADIR);
        if (args.tagBaseName != null) {
            params.setProperty("tagBaseName",args.tagBaseName);
        }
        // load config file if specified and override
        if (args.propertiesFile != null) {
            LOG.info("Reading properties from "+args.propertiesFile);
            params.load(new FileReader(args.propertiesFile));
        }
        if (args.proxyPorts != null && args.proxyPorts.contains(":")) {
            params.setProperty("DBPROXY", args.proxyPorts);
        } else {
            params.setProperty("DBPROXY", DBPROXY);
        }
        if ("proxy".equalsIgnoreCase(args.demoMode)) {
            proxyDemo(params);
        } else if ("proxydemo".equalsIgnoreCase(args.demoMode)) {
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
        acp.createVerticaNodes(params, "default", 6, "c5.large");
        String asimss = acp.checkState(params);
        LOG.info("spot state: "+asimss);
        String publicIp = null;
        List<String> ips = new ArrayList<>();
        for (String ip : asimss.split(";;")) {
            String findDns[] = ip.split("\\|");
            if (findDns.length == 5 && !StringUtils.isEmpty(findDns[4])) {
                LOG.info("private IP: "+findDns[4]);
                ips.add(findDns[4]);
                publicIp = findDns[3];
            }
        }
        try { Thread.sleep(10000L); } catch (Exception e) { }
        LOG.info("Using public IP (last node): "+publicIp);
        try {
            params.setProperty("node", publicIp);
            params.setProperty("allNodes", String.join(",", ips));
            AwsVerticaService avs = new AwsVerticaService();
            avs.installVertica(params);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        // let the scheduler kill the instances
        params.setProperty("stopBehavior","terminate");
        //scheduleInit(params);
        //try { LOG.error("Waiting 600 seconds before exiting!"); Thread.sleep(600L*1000L); } catch (Exception e) { }
    }

    public static void proxyDemo(Properties params) throws Exception {
        AwsCloudProvider acp = new AwsCloudProvider();
        acp.init(params);
        params.setProperty("instanceTag","Service=VHibernateDemo");
        String instances = acp.getInstances(params);
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
            e.printStackTrace();
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
    @Parameter(names = {"-p","--properties"}, description = "Properties file (java.util.Properties format) (if omitted, use defaults for all settings not listed here)")
    public String propertiesFile = null;
    @Parameter(names = {"-t","--tagname"}, description = "Tag name for resources (if omitted, use a string derived from current timestamp)")
    public String tagBaseName = "AwsVerticaDemo-" + System.currentTimeMillis();
    @Parameter(names = {"-d","--demomode"}, description = "Which demo mode to run (if omitted or invalid, demo spot instances and exit)")
    public String demoMode = null;
    @Parameter(names = {"-P","--ports"}, description = "For proxy service, <local port>:<remote port> (if omitted or invalid, 35433:5433, or forward local port 35433 to remote port 5433)")
    public String proxyPorts = null;
    @Parameter(names = {"-c","--communal-storage"}, description = "Communal storage location (S3 bucket) (if omitted or invalid, no default, error will occur with Eon mode. This setting is ignored for EE mode)")
    public String communalStorage = "";
    @Parameter(names = {"-S","--secondary-subcluster"}, description = "Create a secondary subcluster with name:nodeCount:instanceType (if omitted or invalid, no default, no secondary subcluster will be created. This setting is ignored for EE mode)")
    public String secondarySubcluster = "";
    @Parameter(names = {"-s","--spot"}, description = "Create spot instances (default: on-demand)")
    public boolean spot = false;
    @Parameter(names = {"-e","--eonmode"}, description = "Create Eon mode DB (default: EE)")
    public boolean eonMode = false;
    @Parameter(names = {"-?","--help"}, help = true, description = "Print this message")
    public boolean help;
}