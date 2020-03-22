package com.vertica.example;

import com.vertica.aws.AwsCloudProvider;
import com.vertica.aws.AwsVerticaService;
import com.vertica.devops.AwsInstance;
import com.vertica.devops.SshUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class VerticaTasks {
    final static Logger LOG = LogManager.getLogger(VerticaTasks.class);

    public VerticaTasks(Properties params) { init(params); }

    public void createDatabase(Properties params) throws Exception {
        LOG.info("Adding 'create' flag and using revive method");
        params.setProperty("createDb","true");
        reviveDatabase(params);
        params.remove("createDb");
    }

    public void reviveDatabase(Properties params) throws Exception {
        LOG.warn("We will create and manage a cluster on spot instances to revive the DB");
        // name:nodeCount:instanceType
        String[] secondary = params.getProperty("clusterSize").split(":");
        LOG.info("subcluster "+secondary[0]+" will have "+secondary[1]+"x "+secondary[2]);
        // create the nodes
        init(params);
        List<AwsInstance> nodes = createVerticaNodes(params, secondary[0], Integer.parseInt(secondary[1]), secondary[2]);
        LOG.info("Node count = "+nodes.size());
    }

    public void createSubcluster(Properties params) throws Exception {
        String dbControlNode = params.getProperty("DBCONTROLNODE");
        if (StringUtils.isEmpty(dbControlNode)) {
            throw new Exception("You need to define -n/--node parameter we can connect to get cluster info.");
        }
    }

    public void getStatus(Properties params) throws Exception {
        List<AwsInstance> aList = getInstancesByTag(params, "VerticaDatabase", params.getProperty("DBNAME"));
        for (AwsInstance a : aList) {
            LOG.info("getStatus found:"+a);
        }
    }

    public void removeSubcluster(Properties params) throws Exception {
        String dbControlNode = params.getProperty("DBCONTROLNODE");
        if (StringUtils.isEmpty(dbControlNode)) {
            throw new Exception("You need to define -n/--node parameter we can connect to get cluster info.");
        }
    }

    public void stopDatabase(Properties params) throws Exception {
        String dbControlNode = null;
        List<AwsInstance> aList = getInstancesByTag(params, "VerticaDatabase", params.getProperty("DBNAME"));
        List<String> sirIds = new ArrayList<>();
        List<String> instanceIds = new ArrayList<>();
        for (AwsInstance a : aList) {
            LOG.info("getStatus found:"+a);
            dbControlNode = a.publicDns;
            sirIds.add(a.sirId);
            instanceIds.add(a.instanceId);
        }
        if (StringUtils.isEmpty(dbControlNode)) {
            throw new Exception("Could not stop database: could not determine hostname of any node.");
        }
        params.setProperty("node", dbControlNode);
        AwsVerticaService avs = new AwsVerticaService();
        avs.destroyServices(params);
        AwsCloudProvider acp = new AwsCloudProvider(params);
        params.setProperty("spotInstances", "true");
        //         List<String> spotInstanceRequestIds = Arrays.asList(params.getProperty("spotInstanceRequestIds").split(";;"));
        //        List<String> spotInstanceIds = Arrays.asList(params.getProperty("spotInstanceIds").split(";;"));
        params.setProperty("spotInstanceRequestIds", String.join(";;", sirIds));
        params.setProperty("spotInstanceIds", String.join(";;", instanceIds));
        acp.destroyInstances(params);
    }

    // methods to do the work
    private static Ec2Client ec2 = null;

    public boolean init(Properties params) {
        System.setProperty("aws.accessKeyId", params.getProperty("awsAccessKeyID"));
        System.setProperty("aws.secretAccessKey", params.getProperty("awsSecretAccessKey"));
        System.setProperty("aws.region", params.getProperty("awsRegion"));
        ec2 = Ec2Client.builder().build();
        return true;
    }

    private String createVerticaSecurityGroup(String groupName) {
        try {
            CreateSecurityGroupRequest securityGroupRequest = CreateSecurityGroupRequest.builder()
                    .groupName(groupName)
                    .description(groupName)
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
        ipPermission = IpPermission.builder().ipProtocol("-1").ipRanges(IpRange.builder().cidrIp("10.11.12.0/24").build()).build();
        ipPermissions.add(ipPermission);

        try {
            // Authorize the ports to the used.
            AuthorizeSecurityGroupIngressRequest ingressRequest =
                    AuthorizeSecurityGroupIngressRequest.builder().groupName(groupName).ipPermissions(ipPermissions).build();
            ec2.authorizeSecurityGroupIngress(ingressRequest);
        } catch (Exception ase) {
            // Ignore because this likely means the zone has
            // already been authorized.
            System.out.println(ase.getMessage());
        }

        return groupName;
    }

    private List<AwsInstance> createVerticaNodes(Properties params, String clusterName, int instanceCount, String instanceType) throws Exception {
        String spotTagBaseName = params.getProperty("tagBaseName", "AwsVerticaDemo");
        params.setProperty("instanceTag", spotTagBaseName + "-SpotInstance");
        params.setProperty("serviceTag", spotTagBaseName + "-Vertica");
        params.setProperty("verticaDataTag", "subcluster::"+spotTagBaseName);
        //Ec2Client ec2 = Ec2Client.builder().build();
        String verticaSecurityGroup = params.getProperty("verticaSecurityGroup");
        if (StringUtils.isEmpty(verticaSecurityGroup)) {
            verticaSecurityGroup = createVerticaSecurityGroup(spotTagBaseName + "-SpotSecurityGroup");
        }
        ArrayList<String> securityGroups = new ArrayList<>();
        securityGroups.add(verticaSecurityGroup);
        // Add the launch specifications to the request. Use Vertica AMI here...
        InstanceType spotInstanceType = StringUtils.isEmpty(instanceType)?InstanceType.C5_LARGE:InstanceType.fromValue(instanceType);
        RequestSpotLaunchSpecification launchSpecification = RequestSpotLaunchSpecification.builder()
                .imageId(AwsVerticaService.getVerticaAmiId())
                .instanceType(spotInstanceType)
                .securityGroups(securityGroups)
                .keyName(params.getProperty("awsKeyPairName")).build();
        // Request instance with a bid price
        RequestSpotInstancesRequest requestRequest = RequestSpotInstancesRequest.builder().availabilityZoneGroup("us-east-1d").instanceCount(instanceCount).launchSpecification(launchSpecification).build();
        // Call the RequestSpotInstance API.
        RequestSpotInstancesResponse requestResult = ec2.requestSpotInstances(requestRequest);
        List<SpotInstanceRequest> requestResponses = requestResult.spotInstanceRequests();
        // Setup an arraylist to collect all of the request ids we want to
        // watch hit the running state.
        ArrayList<String> spotInstanceRequestIds = new ArrayList<>();
        // Add all of the request ids to the hashset, so we can determine when they hit the
        // active state.
        for (SpotInstanceRequest requestResponse : requestResponses) {
            LOG.info("Created Spot Request: "+requestResponse.spotInstanceRequestId());
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
                    LOG.info("Spot state: "+describeResponse.instanceId()+"/"+describeResponse.spotInstanceRequestId()+" is "+describeResponse.stateAsString()+"/"+describeResponse.actualBlockHourlyPrice());
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
        // assuming everything worked, let's tag the instances for later actions
        LOG.info("Instances to tag: "+spotInstanceIds.size());
        if (spotInstanceIds.size() != instanceCount) {
            LOG.error("=== expecting "+instanceCount+" instances, but only found "+spotInstanceIds.size());
        }
        Tag nameTag = Tag.builder().key("Name").value(spotTagBaseName+"-"+params.getProperty("DBNAME")+"-"+clusterName).build();
        Tag verticaDbTag = Tag.builder().key("VerticaDatabase").value(params.getProperty("DBNAME")).build();
        Tag verticaDbClusterTag = Tag.builder().key("VerticaDatabaseCluster").value(clusterName).build();
        CreateTagsRequest ctr = CreateTagsRequest.builder().resources(spotInstanceIds).tags(nameTag,verticaDbTag,verticaDbClusterTag).build();
        CreateTagsResponse ctresp = ec2.createTags(ctr);
        LOG.info("create tags response: "+ctresp.toString()+","+ctresp.toBuilder().toString());
        String ids = String.join(";;", spotInstanceIds);
        LOG.info("Got spot IDs: "+ids);
        params.setProperty("spotInstanceIds", ids);
        // configure instances
        AwsCloudProvider acp = new AwsCloudProvider(params);
        acp.getInstancesById(spotInstanceIds);
        List<String> publicIps = new ArrayList<>();
        List<String> privateIps = new ArrayList<>();
        for (AwsInstance i : acp.instances) {
            publicIps.add(i.publicDns);
            privateIps.add(i.privateIp);
            params.setProperty("node",i.publicDns);
        }
        acp.configureInstances(params, publicIps);
        // install Vertica and revive
        String allIp = String.join(",",privateIps);
        // for Eon mode, we also need to upload a credential file
        LOG.info("Will use node "+params.getProperty("node")+" to install Vertica on "+allIp);
        SshUtil ssh = new SshUtil();
        ssh.sftp(params, params.getProperty("VERTICA_PEM_KEYFILE"), "/tmp/keyfile.pem");
        // exec command(s)
        List<String> commands = new ArrayList<String>();
        // only Eon mode is supported here
        String dbLicense = params.getProperty("DBLICENSE","CE");
        if (!"CE".equalsIgnoreCase(dbLicense)) {
            ssh.sftp(params, dbLicense, "/tmp/license.dat");
            dbLicense = "/tmp/license.dat";
        }
        commands.add("sudo /opt/vertica/sbin/install_vertica -i /tmp/keyfile.pem --debug --license "+dbLicense+" --accept-eula --hosts "+allIp+" --dba-user-password-disabled --failure-threshold NONE -d "+params.getProperty("DBDATADIR"));
        // this is really the only difference between create and revive scripting
        if ("true".equalsIgnoreCase(params.getProperty("createDb"))) {
            commands.add("sudo -u dbadmin /opt/vertica/bin/admintools -t create_db --shard-count "+(2*instanceCount)+" --depot-path=/vertica/depot -s " + allIp + " -d " + params.getProperty("DBNAME") + " -x /tmp/eonaws.conf --communal-storage-location=" + params.getProperty("DBS3BUCKET"));
        } else {
            commands.add("sudo -u dbadmin /opt/vertica/bin/admintools -t revive_db --force -s " + allIp + " -d " + params.getProperty("DBNAME") + " -x /tmp/eonaws.conf --communal-storage-location=" + params.getProperty("DBS3BUCKET"));
        }
        commands.add("sudo -u dbadmin /opt/vertica/bin/admintools -t start_db -i -d "+params.getProperty("DBNAME")+" -p "+params.getProperty("DBPASS"));
        // if selected, create and upload a credential file for Eon mode
        File tf = File.createTempFile("eonaws",".cnf");
        BufferedWriter bw = new BufferedWriter(new FileWriter(tf));
        bw.write("awsauth = "+params.getProperty("awsAccessKeyID")+":"+params.getProperty("awsSecretAccessKey")); bw.newLine();
        bw.write("awsregion = "+params.getProperty("awsRegion")); bw.newLine();
        bw.flush(); bw.close();
        LOG.info("Uploading "+tf.getCanonicalPath());
        ssh.sftp(params, tf.getCanonicalPath(), "/tmp/eonaws.conf");
        //commands.add("cat /opt/vertica/log/adminTools.log");
        //String command = install_vertica;
        for (String command : commands) {
            ssh.ssh(params, command);
        }
        // test Vertica
        //LOG.info(checkState(params));
        // populate node and subcluster list
        //getClusterState(params);
        return acp.instances;
    }

    // this also updates the class listing of instances
    public List<AwsInstance> getInstancesByTag(Properties targets, String tagName, String tagValue) {
        String nextToken = null;
        Filter filter = Filter.builder().name("tag:"+tagName).values(tagValue).build();
        LOG.info("Filter on "+filter.toString());
        List<String> instances = new ArrayList<>();
        List<AwsInstance> awsInstances = new ArrayList<>();
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).maxResults(1000)/*.nextToken(nextToken)*/.build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            LOG.info("response.reservations()="+response.reservations().size());
            for (Reservation reservation : response.reservations()) {
                LOG.info("For reservation ["+reservation.toString()+"], reservation.instances()="+reservation.instances().size());
                for (Instance instance : reservation.instances()) {
                    LOG.info(instance.instanceId()+","+instance.privateIpAddress()+","+instance.tags());
                    instances.add(instance.instanceId());
                    awsInstances.add(new AwsInstance(instance));
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return awsInstances;
    }

}
