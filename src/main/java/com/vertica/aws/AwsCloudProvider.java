package com.vertica.aws;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.vertica.devops.AwsInstance;
import com.vertica.devops.CloudProviderInterface;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.util.*;

// BH: creating an interface seemed like a good idea at the time, but I don't seem to be using it here
public class AwsCloudProvider implements CloudProviderInterface {
    final static Logger LOG = LogManager.getLogger(AwsCloudProvider.class);
    private static Ec2Client ec2 = null;
    public List<AwsInstance> instances = new ArrayList<>();
    // quick lookup of instance Id based on private IP.  Allows Vertica functions to map nodes to instances by IP.
    public Map<String,String> instanceIdIpMap = new HashMap<>();

    public AwsCloudProvider() { }

    public AwsCloudProvider(Properties p) { init(p); }

    @Override
    public boolean init(Properties params) {
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        ec2 = Ec2Client.builder().build();
        return true;
    }

    public boolean createVerticaNodes(Properties targets, String clusterName, int instanceCount, String instanceType) {
        boolean finalState = false;
        if (targets.containsKey("spotInstances")) {
            finalState = createVerticaNodesSpot(targets, clusterName, instanceCount, instanceType);
        } else {
            finalState = createVerticaNodesOnDemand(targets, clusterName, instanceCount, instanceType);
        }
        finalState = configureInstances(targets);
        return finalState;
    }

    private boolean createVerticaNodesSpot(Properties params, String clusterName, int instanceCount, String instanceType) {
        String spotTagBaseName = params.getProperty("tagBaseName", "AwsVerticaDemo-" + System.currentTimeMillis());
        params.setProperty("instanceTag", spotTagBaseName + "-SpotInstance");
        params.setProperty("verticaDataTag", "subcluster::"+spotTagBaseName);
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        Ec2Client ec2 = Ec2Client.builder().build();

        try {
            CreateSecurityGroupRequest securityGroupRequest = CreateSecurityGroupRequest.builder()
                    .groupName(spotTagBaseName+"-SpotSecurityGroup")
                    .description(spotTagBaseName+" Spot Security Group")
                    .build();
            ec2.createSecurityGroup(securityGroupRequest);
        } catch (Exception ase) {
            // Likely this means that the group is already created, so ignore.
            System.out.println(ase.getMessage());
        }

        String ipAddr = "0.0.0.0/0";

        // Create a range that you would like to populate.
        ArrayList<IpRange> ipRanges = new ArrayList<>();
        ipRanges.add(IpRange.builder().cidrIp(ipAddr).build());

        // Open up port 22 for TCP traffic to the associated IP
        // from above (e.g. ssh traffic).
        ArrayList<IpPermission> ipPermissions = new ArrayList<>();
        IpPermission ipPermission = IpPermission.builder().ipProtocol("tcp").fromPort(22).toPort(22).ipRanges(ipRanges).build();
        ipPermissions.add(ipPermission);
        ipPermission = IpPermission.builder().ipProtocol("tcp").fromPort(5433).toPort(5433).ipRanges(ipRanges).build();
        ipPermissions.add(ipPermission);
        ipPermission = IpPermission.builder().ipProtocol("-1").ipRanges(IpRange.builder().cidrIp("172.31.0.0/16").build()).build();
        ipPermissions.add(ipPermission);

        try {
            // Authorize the ports to the used.
            AuthorizeSecurityGroupIngressRequest ingressRequest =
                    AuthorizeSecurityGroupIngressRequest.builder().groupName(spotTagBaseName+"-SpotSecurityGroup").ipPermissions(ipPermissions).build();
            ec2.authorizeSecurityGroupIngress(ingressRequest);
        } catch (Exception ase) {
            // Ignore because this likely means the zone has
            // already been authorized.
            System.out.println(ase.getMessage());
        }
        ArrayList<String> securityGroups = new ArrayList<>();
        securityGroups.add(spotTagBaseName+"-SpotSecurityGroup");
        // Add the launch specifications to the request. Use Vertica AMI here...
        InstanceType spotInstanceType = StringUtils.isEmpty(instanceType)?InstanceType.C5_LARGE:InstanceType.fromValue(instanceType);
        RequestSpotLaunchSpecification launchSpecification = RequestSpotLaunchSpecification.builder()
                .imageId(AwsVerticaService.getVerticaAmiId())
                .instanceType(spotInstanceType)
                .securityGroups(securityGroups)
                .keyName(params.getProperty("awsKeyPairName")).build();
        // Request instance with a bid price
        RequestSpotInstancesRequest requestRequest = RequestSpotInstancesRequest.builder().instanceCount(instanceCount).launchSpecification(launchSpecification).build();
        // Call the RequestSpotInstance API.
        RequestSpotInstancesResponse requestResult = ec2.requestSpotInstances(requestRequest);
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
        params.setProperty("spotInstanceRequestIds",String.join(";;",spotInstanceRequestIds));
        // Create a variable that will track whether there are any
        // requests still in the open state.
        boolean anyOpen;
        // use an actual HashSet to record unique instance Id's assigned
        Set<String> spotInstanceIds = new HashSet<>();
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
                        spotInstanceIds.add(describeResponse.instanceId());
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
                // Sleep for 5 seconds.
                Thread.sleep(5*1000L);
            } catch (Exception e) {
                // Do nothing because it woke up early.
            }
        } while (anyOpen);
        // end step 4
        // assuming everything worked, let's tag the instances for later actions
        Tag spotTag = Tag.builder().key("instanceTag").value(params.getProperty("instanceTag")).build();
        Tag spotServiceTag = Tag.builder().key("Service").value(params.getProperty("serviceTag")).build();
        CreateTagsRequest ctr = CreateTagsRequest.builder().resources(spotInstanceIds).tags(spotTag,spotServiceTag).build();
        ec2.createTags(ctr);
        String ids = String.join(";;", spotInstanceIds);
        LOG.error("Got spot IDs: "+ids);
        params.setProperty("spotInstanceIds", ids);
        // update instance data
        getInstances(params);
        return false;
    }

    private boolean createVerticaNodesOnDemand(Properties params, String clusterName, int instanceCount, String instanceType) {
        // check for and revive hibernated instances, otherwise create new
        return false;
    }

    @Override
    public boolean createInstances(Properties targets) {
        boolean finalState = false;
        if (targets.containsKey("spotInstances")) {
            finalState = createSpotInstances(targets);
        } else {
            finalState = createOnDemandInstances(targets);
        }
        finalState = configureInstances(targets);
        return finalState;
    }

    private boolean createSpotInstances(Properties params) {
        String spotTagBaseName = params.getProperty("tagBaseName", "AwsVerticaDemo-" + System.currentTimeMillis());
        params.setProperty("instanceTag", spotTagBaseName + "-SpotInstance");
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        Ec2Client ec2 = Ec2Client.builder().build();

        try {
            CreateSecurityGroupRequest securityGroupRequest = CreateSecurityGroupRequest.builder()
                    .groupName(spotTagBaseName+"-SpotSecurityGroup")
                    .description(spotTagBaseName+" Spot Security Group")
                    .build();
            ec2.createSecurityGroup(securityGroupRequest);
        } catch (Exception ase) {
            // Likely this means that the group is already created, so ignore.
            System.out.println(ase.getMessage());
        }

        String ipAddr = "0.0.0.0/0";

        // Create a range that you would like to populate.
        ArrayList<IpRange> ipRanges = new ArrayList<>();
        ipRanges.add(IpRange.builder().cidrIp(ipAddr).build());

        // Open up port 22 for TCP traffic to the associated IP
        // from above (e.g. ssh traffic).
        ArrayList<IpPermission> ipPermissions = new ArrayList<>();
        IpPermission ipPermission = IpPermission.builder().ipProtocol("tcp").fromPort(22).toPort(22).ipRanges(ipRanges).build();
        ipPermissions.add(ipPermission);
        ipPermission = IpPermission.builder().ipProtocol("tcp").fromPort(5433).toPort(5433).ipRanges(ipRanges).build();
        ipPermissions.add(ipPermission);
        ipPermission = IpPermission.builder().ipProtocol("-1").ipRanges(IpRange.builder().cidrIp("172.31.0.0/16").build()).build();
        ipPermissions.add(ipPermission);

        try {
            // Authorize the ports to the used.
            AuthorizeSecurityGroupIngressRequest ingressRequest =
                    AuthorizeSecurityGroupIngressRequest.builder().groupName(spotTagBaseName+"-SpotSecurityGroup").ipPermissions(ipPermissions).build();
            ec2.authorizeSecurityGroupIngress(ingressRequest);
        } catch (Exception ase) {
            // Ignore because this likely means the zone has
            // already been authorized.
            System.out.println(ase.getMessage());
        }
        ArrayList<String> securityGroups = new ArrayList<>();
        securityGroups.add(spotTagBaseName+"-SpotSecurityGroup");
        // Add the launch specifications to the request. Use Vertica AMI here...
        String sit = params.getProperty("instanceType");
        InstanceType spotInstanceType = StringUtils.isEmpty(sit)?InstanceType.C5_LARGE:InstanceType.fromValue(sit);
        RequestSpotLaunchSpecification launchSpecification = RequestSpotLaunchSpecification.builder()
                .imageId(AwsVerticaService.getVerticaAmiId())
                .instanceType(spotInstanceType)
                .securityGroups(securityGroups)
                .keyName(params.getProperty("awsKeyPairName")).build();
        // Request instance with a bid price
        RequestSpotInstancesRequest requestRequest = RequestSpotInstancesRequest.builder().instanceCount(3).launchSpecification(launchSpecification).build();
        // Call the RequestSpotInstance API.
        RequestSpotInstancesResponse requestResult = ec2.requestSpotInstances(requestRequest);
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
        params.setProperty("spotInstanceRequestIds",String.join(";;",spotInstanceRequestIds));
        // Create a variable that will track whether there are any
        // requests still in the open state.
        boolean anyOpen;
        // use an actual HashSet to record unique instance Id's assigned
        Set<String> spotInstanceIds = new HashSet<>();
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
                        spotInstanceIds.add(describeResponse.instanceId());
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
                // Sleep for 5 seconds.
                Thread.sleep(5*1000L);
            } catch (Exception e) {
                // Do nothing because it woke up early.
            }
        } while (anyOpen);
        // end step 4
        // assuming everything worked, let's tag the instances for later actions
        Tag spotTag = Tag.builder().key("instanceTag").value(params.getProperty("instanceTag")).build();
        Tag spotServiceTag = Tag.builder().key("Service").value(params.getProperty("serviceTag")).build();
        CreateTagsRequest ctr = CreateTagsRequest.builder().resources(spotInstanceIds).tags(spotTag,spotServiceTag).build();
        ec2.createTags(ctr);
        String ids = String.join(";;", spotInstanceIds);
        LOG.error("Got spot IDs: "+ids);
        params.setProperty("spotInstanceIds", ids);
        return false;
    }

    private boolean createOnDemandInstances(Properties params) {
        // check for and revive hibernated instances, otherwise create new
        return false;
    }

    public boolean configureInstances(Properties params) {
        // TODO: this will do stuff like further network config, mount volumes, etc.
        return false;
    }

    // configure LVM for types with Direct Attached Storage, e.g. i3.4xlarge
    public boolean provisionInstanceDAS(Properties params) {
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

    // this also updates the class listing of instances
    @Override
    public String getInstances(Properties targets) {
        String nextToken = null;
        String filterData = targets.getProperty("instanceTag");
        Filter filter = Filter.builder().name("tag:instanceTag").values(filterData).build();
        LOG.info("Filter on "+filter.toString());
        List<String> instances = new ArrayList<>();
        List<AwsInstance> awsInstances = new ArrayList<>();
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    LOG.info(instance.instanceId()+","+instance.privateIpAddress()+","+instance.tags());
                    instances.add(instance.instanceId());
                    awsInstances.add(new AwsInstance(instance));
                    instanceIdIpMap.put(instance.privateIpAddress(), instance.instanceId());
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        // replace existing list
        this.instances = awsInstances;
        return String.join(";;", instances);
    }

    // return pattern: [instanceId,stateName,computedState,publicDns,privateDns]
    @Override
    public String checkState(Properties targets) {
        if (targets.containsKey("spotInstances")) {
            return checkSpotState(targets);
        } else {
            return checkOnDemandState(targets);
        }
    }

    public String checkOnDemandState(Properties targets) {
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
                    fresponse.add(instance.instanceId()+"|"+instance.state().name()+"|"+isr+"|"+instance.publicDnsName()+"|"+instance.privateDnsName());
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

    public String checkSpotState(Properties params) {
        LOG.info("Spot instance request Id's: "+params.getProperty("spotInstanceRequestIds"));
        LOG.info("Spot instance Id's: "+params.getProperty("spotInstanceIds"));
        List<String> instanceIds = Arrays.asList(params.getProperty("spotInstanceIds").split(";;"));
        String nextToken = null;
        List<String> fresponse = new ArrayList<>();
        Ec2Client ec2 = Ec2Client.builder().build();
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceIds).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    String isr = instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason();
                    fresponse.add(instance.instanceId()+"|"+instance.state().name()+"|"+isr+"|"+instance.publicDnsName()+"|"+instance.privateDnsName());
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return String.join(";;", fresponse);
    }

    @Override
    public boolean alterInstances(Properties targets) {
        return false;
    }

    public boolean tagInstance(String instanceId, String tagKey, String tagValue) {
        Tag newTag = Tag.builder().key(tagKey).value(tagValue).build();
        CreateTagsRequest ctr = CreateTagsRequest.builder().resources(instanceId).tags(newTag).build();
        ec2.createTags(ctr);
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
        if (targets.containsKey("spotInstances")) {
            return destroySpotInstances(targets);
        } else {
            return stopInstances(targets);//false;//createOnDemandInstances(targets);
        }
    }

    private boolean destroySpotInstances(Properties params) {
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        Ec2Client ec2 = Ec2Client.builder().build();
        List<String> spotInstanceRequestIds = Arrays.asList(params.getProperty("spotInstanceRequestIds").split(";;"));
        List<String> spotInstanceIds = Arrays.asList(params.getProperty("spotInstanceIds").split(";;"));
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
        return false;
    }

}
