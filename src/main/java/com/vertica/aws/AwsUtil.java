package com.vertica.aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class AwsUtil {
    private static String awsAccessKeyID = ""
            , awsSecretAccessKey = ""
            , awsRegion = "";
    private static Ec2Client ec2 = null;

    public static void init() {
        System.setProperty("aws.accessKeyId", awsAccessKeyID);
        System.setProperty("aws.secretAccessKey", awsSecretAccessKey);
        System.setProperty("aws.region", awsRegion);
        ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
    }

    public static void describeEC2Instances() {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    /*System.out.printf(
                            "describeEC2Instances: Found reservation with id %s, " +
                                    "AMI %s, " +
                                    "type %s, " +
                                    "state %s, reason %s, " +
                                    "hibernation %s, " +
                                    "and monitoring state %s",
                            instance.instanceId(),
                            instance.imageId(),
                            instance.instanceType(),
                            instance.state().name(),
                            (instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason()),
                            instance.hibernationOptions().toString(),
                            instance.monitoring().state());
                    System.out.println("");*/
                    System.out.println(instance.toString().replace(",",",\n"));
                }
            }
            nextToken = response.nextToken();


        } while (nextToken != null);
    }

    public static void describeInstance(String instanceId) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceId).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    System.out.println(instance.toString().replace(",",",\n"));
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
    }

    public static void describeInstances(List<String> instanceIds) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceIds).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    System.out.println(instance.toString().replace(",",",\n"));
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
    }

    public static String getInstanceState(String instanceId) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceId).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    System.out.printf(
                            "getInstanceState: Found reservation with id %s, " +
                                    "AMI %s, " +
                                    "type %s, " +
                                    "state %s, reason %s, " +
                                    "hibernation %s, " +
                                    "and monitoring state %s",
                            instance.instanceId(),
                            instance.imageId(),
                            instance.instanceType(),
                            instance.state().name(),
                            (instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason()),
                            instance.hibernationOptions().toString(),
                            instance.monitoring().state());
                    System.out.println("");
                    return instance.state().name()+"|"+(instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason());
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return null;
    }

    public static List<String> getInstanceState(List<String> instanceId) {
        String nextToken = null;
        List<String> fresponse = new ArrayList<>();
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceId).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    System.out.printf(
                            "getInstanceState: Found reservation with id %s, " +
                                    "AMI %s, " +
                                    "type %s, " +
                                    "state %s, reason %s, " +
                                    "hibernation %s, " +
                                    "and monitoring state %s",
                            instance.instanceId(),
                            instance.imageId(),
                            instance.instanceType(),
                            instance.state().name(),
                            (instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason()),
                            instance.hibernationOptions().toString(),
                            instance.monitoring().state());
                    System.out.println("");
                    fresponse.add(instance.state().name()+"|"+(instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason()));
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return fresponse;
    }

    public static String getInstancePublicDns(String instanceId) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceId).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    return instance.publicDnsName();
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return null;
    }

    public static String getInstancePublicDns(List<String> instanceIds) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceIds).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (!StringUtils.isEmpty(instance.publicDnsName())) {
                        return instance.publicDnsName();
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return null;
    }

    public static boolean hibernateInstance(String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId).hibernate(true).build();
        ec2.stopInstances(request);
        return false;
    }

    public static boolean startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId).build();
        ec2.startInstances(request);
        return false;
    }

    public static boolean hibernateInstances(List<String> instanceIds) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceIds).hibernate(true).build();
        ec2.stopInstances(request);
        return false;
    }

    public static boolean startInstances(List<String> instanceIds) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceIds).build();
        ec2.startInstances(request);
        return false;
    }

}
