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
    final static String SINGLE_INSTANCE_ID = "i-X"
                        ,CLUSTER_INSTANCE_ID_1 = "i-1"
                        ,CLUSTER_INSTANCE_ID_2 = "i-2"
                        ,CLUSTER_INSTANCE_ID_3 = "i-3";
    private static List<String> instanceIds = new ArrayList<>();

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
        /*
        AwsUtil.init();
        //AwsUtil.describeEC2Instances();
        String instanceId = SINGLE_INSTANCE_ID;
        AwsUtil.describeInstance(instanceId);
        LOG.info("Current state: "+AwsUtil.getInstanceState(instanceId));
        testInstanceState(instanceId, "stopped");
        testStartInstance(instanceId);
        testVertica(AwsUtil.getInstancePublicDns(instanceId), 30);
        //testInstanceState(instanceId, "running");
        testHibernateInstance(instanceId);
        //testInstanceState(instanceId, "hibernate");
        */
    }

    public static void awsClusterTestSuite() throws Exception {
        // cluster health tests
        AwsUtil.init();
        instanceIds.add(CLUSTER_INSTANCE_ID_1);
        instanceIds.add(CLUSTER_INSTANCE_ID_2);
        instanceIds.add(CLUSTER_INSTANCE_ID_3);
        AwsUtil.setInstanceIds(instanceIds);
        AwsUtil.testStartInstances();
        VerticaUtil.testVerticaNodes(AwsUtil.getInstancePublicDns(instanceIds), 30);
        AwsUtil.testHibernateInstances();
        // proxy tests
        ScheduleUtil.scheduleInit();
        String host = instanceIds.get(0);
        int remoteport = 5433;
        int localport = 35433;
        // Print a start-up message
        System.out.println("Starting proxy for " + host + ":" + remoteport
                + " on port " + localport);
        ServerSocket server = new ServerSocket(localport);
        while (true) {
            new ThreadProxy(server.accept(), host, remoteport);
        }
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

    public static void clusterProxyTestSuite() throws Exception {
        AwsUtil.init();
    }

    public static void proxyTestSuite() throws Exception {
        AwsUtil.init();
        ScheduleUtil.scheduleInit();
        String host = SINGLE_INSTANCE_ID;
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

}
