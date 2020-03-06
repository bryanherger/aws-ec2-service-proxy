package com.vertica.example;

import com.jcraft.jsch.*;
import com.vertica.aws.AwsCloudProvider;
import com.vertica.aws.AwsSpotInstanceManager;
import com.vertica.aws.AwsVerticaService;
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

    public static void main(String[] args) throws Exception {
        //This is the root logger provided by log4j
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);

//Define log pattern layout
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

//Add console appender to root logger
        rootLogger.addAppender(new ConsoleAppender(layout));

        // run test(s)
        LOG.info("AwsVerticaDemo: args "+args.length);
        Properties params = new Properties();
        // set defaults
        params.setProperty("awsAccessKeyID", awsAccessKeyID);
        params.setProperty("awsSecretAccessKey", awsSecretAccessKey);
        params.setProperty("awsRegion", awsRegion);
        params.setProperty("VERTICA_PEM_KEYFILE", VERTICA_PEM_KEYFILE);
        params.setProperty("DBNAME", DBNAME);
        params.setProperty("DBPORT", DBPORT);
        params.setProperty("DBPASS", DBPASS);
        params.setProperty("DBUSER", DBUSER);
        // load config file if specified and override
        if (args.length > 0) {
            params.load(new FileReader(args[0]));
        }
        // demo spot requests: create then destroy
        AwsSpotInstanceManager asim = new AwsSpotInstanceManager();
        asim.submitSpotRequest(params);
        String asimss = asim.checkSpotState(params);
        LOG.info("spot state: "+asim.checkSpotState(params));
        String publicIp = null;
        List<String> ips = new ArrayList<>();
        for (String ip : asimss.split(";;")) {
            String findDns[] = ip.split("\\|");
            if (findDns.length == 3 && !StringUtils.isEmpty(findDns[1])) {
                LOG.info("private IP: "+findDns[1]);
                ips.add(findDns[1]);
                publicIp = findDns[2];
            }
        }
        try { Thread.sleep(10000L); } catch (Exception e) { }
        LOG.info("Using public IP (last node): "+publicIp);
        try {
            params.setProperty("eonMode","true");
            params.setProperty("node", publicIp);
            verticaOnSpot(params, String.join(",", ips));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        // let the scheduler kill the instances
        //asim.terminateSpotInstances(params);
        scheduleInit(params);
        try { Thread.sleep(10000000L); } catch (Exception e) { }
        System.exit(0);
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
            if (findDns1a.length == 4 && !StringUtils.isEmpty(findDns1a[3])) {
                LOG.info("using node "+findDns1a[3]);
                params.setProperty("node", findDns1a[3]);
                break;
            }
        }
        if (!StringUtils.isEmpty(params.getProperty("node"))) {
            avs.checkState(params);
            scheduleInit(params);
            int remoteport = 5433;
            int localport = 35433;
            // Print a start-up message
            System.out.println("Starting proxy for " + params.getProperty("node") + ":" + remoteport
                    + " on port " + localport);
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
            // First we must get a reference to a scheduler
            SchedulerFactory sf = new StdSchedulerFactory();
            Scheduler sched = sf.getScheduler();
            // define the job and tie it to our HelloJob class
            JobDataMap jdm = new JobDataMap();
            jdm.put("params", params);
            JobDetail job = newJob(MonitorJob.class).withIdentity("job1", "group1").usingJobData(jdm).build();
            // Trigger the job to run on the next round minute
            //Trigger trigger = newTrigger().withIdentity("trigger1", "group1").startAt(runTime).build();
            Trigger trigger = newTrigger()
                    .withIdentity("trigger1", "group1")
                    .withSchedule(cronSchedule("0 0/3 * * * ?"))
                    .build();
            // Tell quartz to schedule the job using our trigger
            sched.scheduleJob(job, trigger);
            // Start up the scheduler (nothing can actually run until the
            // scheduler has been started)
            sched.start();
            LOG.info("------- Started Scheduler -----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void verticaOnSpot(Properties params, String allIp) throws Exception {
        LOG.info("params:");
        params.list(System.out);
        String install_vertica = "sudo /opt/vertica/sbin/install_vertica -i /tmp/keyfile.pem --debug --license CE --accept-eula --hosts "+allIp+" --dba-user-password-disabled --failure-threshold NONE --no-system-configuration";
        String create_db = "sudo -u dbadmin /opt/vertica/bin/admintools -t create_db --skip-fs-checks -s "+allIp+" -d "+params.getProperty("DBNAME")+" -p "+params.getProperty("DBPASS");
        // for Eon mode, we also need to upload a credential file
        LOG.info("Will use node "+params.getProperty("node")+" to install Vertica on "+allIp);
        JSch jsch = new JSch();
        Session jschSession = jsch.getSession(params.getProperty("DBUSER"), params.getProperty("node"));
        java.util.Properties config = new java.util.Properties();
        // ignore host key since it changes for each new AWS instance
        config.put("StrictHostKeyChecking", "no");
        jschSession.setConfig(config);
        // set key file
        String localFile = params.getProperty("VERTICA_PEM_KEYFILE");
        jsch.addIdentity(localFile);
        jschSession.connect();
        ChannelSftp csftp = (ChannelSftp) jschSession.openChannel("sftp");
        csftp.connect();
        String remoteFile = "/tmp/keyfile.pem";
        csftp.put(localFile, remoteFile);
        // if selected, create and upload a credential file for Eon mode
        if (params.containsKey("eonMode")) {
            String create_db_eon = "sudo -u dbadmin /opt/vertica/bin/admintools -t create_db --skip-fs-checks -s "+allIp+" -d "+params.getProperty("DBNAME")+" -p "+params.getProperty("DBPASS");
            create_db = create_db_eon;
            File tf = File.createTempFile("eonaws",".cnf");
            BufferedWriter bw = new BufferedWriter(new FileWriter(tf));
            bw.write("awsauth = "+params.getProperty("awsAccessKeyID")+":"+params.getProperty("awsSecretAccessKey")); bw.newLine();
            bw.write("awsregion = "+params.getProperty("awsRegion")); bw.newLine();
            bw.flush(); bw.close();
            LOG.info("Uploading "+tf.getCanonicalPath());
            csftp.put(tf.getCanonicalPath(), "/tmp/eonaws.conf");
        }
        csftp.exit();
        // exec command(s)
        List<String> commands = new ArrayList<String>();
        commands.add(install_vertica);
        commands.add(create_db);
        commands.add("cat /tmp/eonaws.conf");
        //String command = install_vertica;
        for (String command : commands) {
            LOG.info("SSH EXEC: "+command);
            Channel channel=jschSession.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);
            InputStream in=channel.getInputStream();
            channel.connect();
            byte[] tmp=new byte[1024];
            while(true){
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    System.out.print(new String(tmp, 0, i));
                }
                if(channel.isClosed()){
                    if(in.available()>0) continue;
                    System.out.println("exit-status: "+channel.getExitStatus());
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){}
            }
            channel.disconnect();
        }
        /*
        command = create_db;//"ls -ltr /tmp";
        channel=jschSession.openChannel("exec");
        ((ChannelExec)channel).setCommand(command);
        channel.setInputStream(null);
        ((ChannelExec)channel).setErrStream(System.err);
        in=channel.getInputStream();
        channel.connect();
        tmp=new byte[1024];
        while(true){
            while(in.available()>0){
                int i=in.read(tmp, 0, 1024);
                if(i<0)break;
                System.out.print(new String(tmp, 0, i));
            }
            if(channel.isClosed()){
                if(in.available()>0) continue;
                System.out.println("exit-status: "+channel.getExitStatus());
                break;
            }
            try{Thread.sleep(1000);}catch(Exception ee){}
        }
        channel.disconnect();
        */
        jschSession.disconnect();
        // test Vertica
        AwsVerticaService avs = new AwsVerticaService();
        LOG.info(avs.checkState(params));
    }
}
