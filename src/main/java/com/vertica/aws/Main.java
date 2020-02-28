package com.vertica.aws;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

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
        //awsTestSuite();
        awsClusterTestSuite();
        //proxyTestSuite();
    }

    public static void awsTestSuite() throws Exception {
        AwsUtil.init();
        //AwsUtil.describeEC2Instances();
        String instanceId = "X";
        AwsUtil.describeInstance(instanceId);
        LOG.info("Current state: "+AwsUtil.getInstanceState(instanceId));
        testInstanceState(instanceId, "stopped");
        testStartInstance(instanceId);
        testVertica(AwsUtil.getInstancePublicDns(instanceId), 30);
        //testInstanceState(instanceId, "running");
        testHibernateInstance(instanceId);
        //testInstanceState(instanceId, "hibernate");
    }

    public static void awsClusterTestSuite() throws Exception {
        AwsUtil.init();
        List<String> instanceIds = new ArrayList<>();
        instanceIds.add("X");
        instanceIds.add("X");
        instanceIds.add("X");
        //AwsUtil.describeInstances(instanceIds);
        testStartInstances(instanceIds);
        testVerticaNodes(AwsUtil.getInstancePublicDns(instanceIds), 30);
        testHibernateInstances(instanceIds);
        //AwsUtil.hibernateInstances(instanceIds);
        //AwsUtil.describeInstances(instanceIds);
        //LOG.info("Current state: "+AwsUtil.getInstanceState(instanceId));
        //testInstanceState(instanceId, "stopped");
        //testStartInstance(instanceIds);
        //testVertica(AwsUtil.getInstancePublicDns(instanceIds.get(0)), 30);
        //testInstanceState(instanceId, "running");
        //testHibernateInstance(instanceIds);
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

    public static void testStartInstances(List<String> instanceIds) {
        AwsUtil.startInstances(instanceIds);
        try { testInstanceState(instanceIds, "running", 30); } catch (Exception e) { }
    }

    public static void testHibernateInstances(List<String> instanceIds) {
        AwsUtil.hibernateInstances(instanceIds);
        try { testInstanceState(instanceIds, "hibernate", 30); } catch (Exception e) { }
        try { testInstanceState(instanceIds, "stopped", 30); } catch (Exception e) { }
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

    public static void testInstanceState(List<String> instanceIds, String targetState, int count) throws InterruptedException {
        long stm = System.currentTimeMillis();
        Thread.sleep(5000);
        while (count > 0) {
            List<String> sts = AwsUtil.getInstanceState(instanceIds);
            System.out.println(">>> count: "+count+" states: "+sts.toString()+", target: "+targetState);
            int stc = 0;
            for (String st : sts) {
                if (st.contains(targetState)) {
                    stc++;
                }
            }
            System.out.println(""+stc+"=="+instanceIds.size());
            if (stc == instanceIds.size()) {
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

    public static void testVerticaNodes(String hostname, int count) throws InterruptedException {
        long stm = System.currentTimeMillis();
        Thread.sleep(5000);
        while (count > 0) {
            System.out.println(">>> count: "+count+"");
            if (VerticaUtil.checkIfAlive(hostname) && VerticaUtil.checkNodes(hostname)) {
                break;
            }
            count--;
            Thread.sleep(5000);
        }
        System.out.println("Time (ms) to test Vertica: "+(System.currentTimeMillis()-stm));
    }

}
