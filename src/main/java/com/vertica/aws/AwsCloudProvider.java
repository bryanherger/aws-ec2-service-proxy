package com.vertica.aws;

import com.vertica.devops.CloudProviderInterface;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AwsCloudProvider implements CloudProviderInterface {
    final static Logger LOG = LogManager.getLogger(AwsCloudProvider.class);
    private static Ec2Client ec2 = null;

    @Override
    public boolean init(Properties params) {
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        ec2 = Ec2Client.builder().build();
        return true;
    }

    @Override
    public boolean createInstances(Properties targets) {
        if (targets.containsKey("spotInstances")) {
            return createSpotInstances(targets);
        } else {
            return createOnDemandInstances(targets);
        }
    }

    public boolean createSpotInstances(Properties params) {
        return false;
    }

    public boolean createOnDemandInstances(Properties params) {
        return false;
    }

    @Override
    public boolean startInstances(Properties targets) {
        String targetState = "running";
        List<String> instanceIds = Arrays.asList(targets.getProperty("instances").split(";;"));
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceIds).build();
        ec2.startInstances(request);
        targets.setProperty("targetState",targetState);
        try {
            int count = 30;
            while (count > 0) {
                if (targetState.equalsIgnoreCase(checkState(targets))) {
                    return true;
                }
                Thread.sleep(5000L);
                count--;
            }
        } catch (Exception e) {
            e.printStackTrace(); return false;
        }
        return true;
    }

    @Override
    public String getInstances(Properties targets) {
        String nextToken = null;
        String[] filterData = targets.getProperty("instanceTag").split("=");
        Filter filter = Filter.builder().name("tag:"+filterData[0]).values(filterData[1]).build();
        List<String> instances = new ArrayList<>();
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    LOG.info(instance.instanceId()+","+instance.tags());
                    instances.add(instance.instanceId());
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return String.join(";;", instances);
    }

    @Override
    public String checkState(Properties targets) {
        List<String> instanceIds = Arrays.asList(targets.getProperty("instances").split(";;"));
        String targetState = null;
        if (targets.contains("targetState")) {
            targetState = targets.getProperty("targetState");
            targets.remove("targetState");
        }
        String nextToken = null;
        List<String> fresponse = new ArrayList<>();
        int count = 0, match = 0;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceIds).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    count++;
                    String isr = instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason();
                    fresponse.add(instance.instanceId()+"|"+instance.state().name()+"|"+isr+"|"+instance.publicDnsName());
                    if (targetState != null) {
                        if (isr.contains(targetState)) { match++; }
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        if (targetState != null) {
            if (count == match) { return targetState; } else { return ""; }
        }
        return String.join(";;", fresponse);
    }

    @Override
    public boolean alterInstances(Properties targets) {
        return false;
    }

    @Override
    public boolean stopInstances(Properties targets) {
        String targetState = "stopped";
        List<String> instanceIds = Arrays.asList(targets.getProperty("instances").split(";;"));
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceIds).hibernate(true).build();
        ec2.stopInstances(request);
        targets.setProperty("targetState",targetState);
        try {
            int count = 30;
            while (count > 0) {
                if (targetState.equalsIgnoreCase(checkState(targets))) {
                    return true;
                }
                Thread.sleep(5000L);
                count--;
            }
        } catch (Exception e) {
            e.printStackTrace(); return false;
        }
        return true;
    }

    @Override
    public boolean destroyInstances(Properties targets) {
        return false;
    }
}
