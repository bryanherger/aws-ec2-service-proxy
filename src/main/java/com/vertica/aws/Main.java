package com.vertica.aws;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.net.ServerSocket;

public class Main {
    final static Logger LOG = LogManager.getLogger(LogUtil.class);

    public static void main(String[] args) throws Exception {
        //This is the root logger provided by log4j
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);

//Define log pattern layout
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

//Add console appender to root logger
        rootLogger.addAppender(new ConsoleAppender(layout));

        // run test(s)
        proxyTestSuite();
    }

    public static void awsTestSuite() throws Exception {
        AwsUtil.init();
        //AwsUtil.describeEC2Instances();
        String instanceId = "i-0496825e4f6bfcfb4";
        AwsUtil.describeInstance(instanceId);
        LOG.info("Current state: "+AwsUtil.getInstanceState(instanceId));
        testInstanceState(instanceId, "stopped");
        testStartInstance(instanceId);
        testVertica(AwsUtil.getInstancePublicDns(instanceId), 30);
        //testInstanceState(instanceId, "running");
        testHibernateInstance(instanceId);
        //testInstanceState(instanceId, "hibernate");
    }

    public static void proxyTestSuite() throws Exception {
        AwsUtil.init();
        ScheduleUtil.scheduleInit();
        String host = "192.168.1.206";
        int remoteport = 5433;
        int localport = 35433;
        // Print a start-up message
        System.out.println("Starting proxy for " + host + ":" + remoteport
                + " on port " + localport);
        ServerSocket server = new ServerSocket(localport);
        while (true) {
            new ThreadProxy(server.accept(), host, remoteport);
        }
    }

    public static void testStartInstance(String instanceId) {
        AwsUtil.startInstance(instanceId);
        try { testInstanceState(instanceId, "running"); } catch (Exception e) { }
    }

    public static void testHibernateInstance(String instanceId) {
        AwsUtil.hibernateInstance(instanceId);
        try { testInstanceState(instanceId, "hibernate"); } catch (Exception e) { }
        try { testInstanceState(instanceId, "stopped"); } catch (Exception e) { }
    }

    public static void testInstanceState(String instanceId, String targetState) throws InterruptedException {
        testInstanceState(instanceId, targetState, 30);
    }

    public static void testInstanceState(String instanceId, String targetState, int count) throws InterruptedException {
        long stm = System.currentTimeMillis();
        Thread.sleep(5000);
        while (count > 0) {
            String sts = AwsUtil.getInstanceState(instanceId);
            System.out.println(">>> count: "+count+" state: "+sts);
            if (sts.contains(targetState)) {
                break;
            }
            count--;
            Thread.sleep(5000);
        }
        System.out.println("Time (ms) to transition to "+targetState+": "+(System.currentTimeMillis()-stm));
    }

    public static void testVertica(String hostname, int count) throws InterruptedException {
        long stm = System.currentTimeMillis();
        Thread.sleep(5000);
        while (count > 0) {
            System.out.println(">>> count: "+count+"");
            if (VerticaUtil.checkIfAlive(hostname)) {
                break;
            }
            count--;
            Thread.sleep(5000);
        }
        System.out.println("Time (ms) to test Vertica: "+(System.currentTimeMillis()-stm));
    }

}
