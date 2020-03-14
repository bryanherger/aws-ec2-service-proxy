package com.vertica.devops;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.utils.StringUtils;

public class AwsInstance {
    public String instanceId, sirId, publicIp, publicDns, privateIp, privateDns, type, state;
    boolean isSpot;

    public AwsInstance() {

    }

    public AwsInstance(Instance instance) {
        update(instance);
    }

    public void update(Instance instance) {
        this.instanceId = instance.instanceId();
        this.sirId = instance.spotInstanceRequestId();
        this.publicIp = instance.publicIpAddress();
        this.publicDns = instance.publicDnsName();
        this.privateIp = instance.privateIpAddress();
        this.privateDns = instance.privateDnsName();
        this.type = instance.instanceTypeAsString();
        this.state = instance.stateReason()!=null?"(msg)"+instance.stateReason().message():"(sTR)"+instance.stateTransitionReason();
        this.isSpot = (StringUtils.isEmpty(instance.spotInstanceRequestId())?false:true);
    }
}
