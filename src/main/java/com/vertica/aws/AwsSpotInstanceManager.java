package com.vertica.aws;

// "steps" from official documentation at https://github.com/awsdocs/aws-java-developer-guide/blob/master/doc_source/tutorial-spot-instances-java.rst

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.util.*;

// TODO: fold back into AwsCloudProvider class
public class AwsSpotInstanceManager {
    final static Logger LOG = LogManager.getLogger(AwsSpotInstanceManager.class);

    @Deprecated
    private boolean submitSpotRequest(Properties params) {
        String tagName = "AwsVerticaDemo-" + System.currentTimeMillis();
        params.setProperty("spotTag", tagName);
        // Step 2: Setting Up a Security Group
        // Create the AmazonEC2 client so we can call various APIs.
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        Ec2Client ec2 = Ec2Client.builder().build();

        // Create a new security group.
        try {
            CreateSecurityGroupRequest securityGroupRequest = CreateSecurityGroupRequest.builder()
                    .groupName(tagName+"-SpotGroup")
                    .description("Spot Security Group")
                    .build();
            ec2.createSecurityGroup(securityGroupRequest);
        } catch (Exception ase) {
            // Likely this means that the group is already created, so ignore.
            System.out.println(ase.getMessage());
        }

        String ipAddr = "0.0.0.0/0";

        // Get the IP of the current host, so that we can limit the Security
        // Group by default to the ip range associated with your subnet.
        /*try {
            InetAddress addr = InetAddress.getLocalHost();

            // Get IP Address
            ipAddr = addr.getHostAddress() + "/10";
        } catch (UnknownHostException e) {
        }*/

        // Create a range that you would like to populate.
        ArrayList<IpRange> ipRanges = new ArrayList<>();
        ipRanges.add(IpRange.builder().cidrIp(ipAddr).build());

        // Open up port 22 for TCP traffic to the associated IP
        // from above (e.g. ssh traffic).
        ArrayList<IpPermission> ipPermissions = new ArrayList<>();
        /*IpPermission ipPermission = new IpPermission();
        ipPermission.setIpProtocol("tcp");
        ipPermission.setFromPort(new Integer(22));
        ipPermission.setToPort(new Integer(22));
        ipPermission.setIpRanges(ipRanges);*/
        IpPermission ipPermission = IpPermission.builder().ipProtocol("tcp").fromPort(22).toPort(22).ipRanges(ipRanges).build();
        ipPermissions.add(ipPermission);
        ipPermission = IpPermission.builder().ipProtocol("tcp").fromPort(5433).toPort(5433).ipRanges(ipRanges).build();
        ipPermissions.add(ipPermission);
        ipPermission = IpPermission.builder().ipProtocol("-1").ipRanges(IpRange.builder().cidrIp("172.31.0.0/16").build()).build();
        ipPermissions.add(ipPermission);

        try {
            // Authorize the ports to the used.
            AuthorizeSecurityGroupIngressRequest ingressRequest =
                    AuthorizeSecurityGroupIngressRequest.builder().groupName(tagName+"-SpotGroup").ipPermissions(ipPermissions).build();
            ec2.authorizeSecurityGroupIngress(ingressRequest);
        } catch (Exception ase) {
            // Ignore because this likely means the zone has
            // already been authorized.
            System.out.println(ase.getMessage());
        }
        // end step 2
        // Step 3: Submitting Your Spot Request
        // Initializes a Spot Instance Request
        // Setup the specifications of the launch. This includes the
        // instance type (e.g. t1.micro) and the latest Amazon Linux
        // AMI id available. Note, you should always use the latest
        // Amazon Linux AMI id or another of your choosing.
        // Add the security group to the request.
        ArrayList<String> securityGroups = new ArrayList<>();
        securityGroups.add(tagName+"-SpotGroup");
        // Add the launch specifications to the request. Use Vertica AMI here...
        String sit = params.getProperty("instanceType");
        InstanceType spotInstanceType = StringUtils.isEmpty(sit)?InstanceType.C5_LARGE:InstanceType.fromValue(sit);
        RequestSpotLaunchSpecification launchSpecification = RequestSpotLaunchSpecification.builder()
                .imageId(AwsVerticaService.getVerticaAmiId())
                .instanceType(spotInstanceType)
                .securityGroups(securityGroups)
                .keyName(params.getProperty("awsKeyPairName")).build();
        // Request instance with a bid price
        RequestSpotInstancesRequest requestRequest = RequestSpotInstancesRequest.builder()/*.spotPrice("0.05")*/.instanceCount(3).launchSpecification(launchSpecification).build();
        // Call the RequestSpotInstance API.
        RequestSpotInstancesResponse requestResult = ec2.requestSpotInstances(requestRequest);
        // end Step 3
        // step 4: Step 4: Determining the State of Your Spot Request
        // BH: I suppose the question is whether to block here waiting for an outcome?  If so, how long?
        // Call the RequestSpotInstance API.
        //RequestSpotInstancesResult requestResult = ec2.requestSpotInstances(requestRequest);
        List<SpotInstanceRequest> requestResponses = requestResult.spotInstanceRequests();
        // Setup an arraylist to collect all of the request ids we want to
        // watch hit the running state.
        ArrayList<String> spotInstanceRequestIds = new ArrayList<>();
        // Add all of the request ids to the hashset, so we can determine when they hit the
        // active state.
        for (SpotInstanceRequest requestResponse : requestResponses) {
            System.out.println("Created Spot Request: "+requestResponse.spotInstanceRequestId());
            spotInstanceRequestIds.add(requestResponse.spotInstanceRequestId());
        }
        params.setProperty("sirs",String.join(";;",spotInstanceRequestIds));
        // Create a variable that will track whether there are any
        // requests still in the open state.
        boolean anyOpen;
        // use an actual HashSet to record unique instance Id's assigned
        Set<String> spotIds = new HashSet<>();
        do {
            // Create the describeRequest object with all of the request ids
            // to monitor (e.g. that we started).
            DescribeSpotInstanceRequestsRequest describeRequest = DescribeSpotInstanceRequestsRequest.builder().spotInstanceRequestIds(spotInstanceRequestIds).build();
            //describeRequest.setSpotInstanceRequestIds(spotInstanceRequestIds);
            // Initialize the anyOpen variable to false - which assumes there
            // are no requests open unless we find one that is still open.
            anyOpen=false;
            try {
                // Retrieve all of the requests we want to monitor.
                DescribeSpotInstanceRequestsResponse describeResult = ec2.describeSpotInstanceRequests(describeRequest);
                List<SpotInstanceRequest> describeResponses = describeResult.spotInstanceRequests();

                // Look through each request and determine if they are all in
                // the active state.
                for (SpotInstanceRequest describeResponse : describeResponses) {
                    // If the state is open, it hasn't changed since we attempted
                    // to request it. There is the potential for it to transition
                    // almost immediately to closed or cancelled so we compare
                    // against open instead of active.
                    LOG.info("Spot state: "+describeResponse.instanceId()+"/"+describeResponse.spotInstanceRequestId()+" is "+describeResponse.stateAsString());
                    if (describeResponse.stateAsString().equalsIgnoreCase("open")) {
                        anyOpen = true;
                        break;
                    } else {
                        spotIds.add(describeResponse.instanceId());
                    }
                }
            } catch (Exception e) {
                // If we have an exception, ensure we don't break out of
                // the loop. This prevents the scenario where there was
                // blip on the wire.
                LOG.error(e.getMessage(), e);
                anyOpen = true;
            }
            try {
                // Sleep for 20 seconds.
                Thread.sleep(20*1000L);
            } catch (Exception e) {
                // Do nothing because it woke up early.
            }
        } while (anyOpen);
        // end step 4
        String ids = String.join(";;", spotIds);
        LOG.error("Got spot IDs: "+ids);
        params.setProperty("spotIds", ids);
        return false;
    }

    @Deprecated
    private String checkSpotState(Properties params) {
        LOG.info("Spot instance request Id's: "+params.getProperty("sirs"));
        LOG.info("Spot instance Id's: "+params.getProperty("spotIds"));
        List<String> instanceIds = Arrays.asList(params.getProperty("spotIds").split(";;"));
        String nextToken = null;
        List<String> fresponse = new ArrayList<>();
        Ec2Client ec2 = Ec2Client.builder().build();
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceIds).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    fresponse.add(instance.instanceId()+"|"+instance.privateDnsName()+"|"+instance.publicDnsName());
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return String.join(";;", fresponse);
    }

    @Deprecated
    private String terminateSpotInstances(Properties params) {
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        Ec2Client ec2 = Ec2Client.builder().build();
        List<String> spotInstanceRequestIds = Arrays.asList(params.getProperty("sirs").split(";;"));
        List<String> spotInstanceIds = Arrays.asList(params.getProperty("spotIds").split(";;"));
        try {
            // Cancel requests.
            CancelSpotInstanceRequestsRequest cancelRequest =
                    CancelSpotInstanceRequestsRequest.builder().spotInstanceRequestIds(spotInstanceRequestIds).build();
            ec2.cancelSpotInstanceRequests(cancelRequest);
            LOG.info("Submitted cancel spot request");
        } catch (Exception e) {
            // Write out any exceptions that may have occurred.
            LOG.error("Error terminating requests: "+e.getMessage(), e);
        }
        try {
            // Terminate instances.
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder().instanceIds(spotInstanceIds).build();
            ec2.terminateInstances(terminateRequest);
            LOG.info("Submitted terminate spot instances");
        } catch (Exception e) {
            // Write out any exceptions that may have occurred.
            LOG.error("Error terminating instances: "+e.getMessage(), e);
        }
        // the question here is, whether to block until instances terminate, or only on error (retry)
        return null;
    }

}
