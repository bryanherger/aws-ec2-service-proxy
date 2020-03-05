package com.vertica.devops;

/* Manage infrastructure lifecycle */

import java.util.Properties;

public interface CloudProviderInterface {
    // set up cloud provider API (credentials, e.g.)
    boolean init(Properties params);

    // create infrastructure (instances, containers) to run services, or start if they already exist
    boolean createInstances(Properties targets);

    // create infrastructure (instances, containers) to run services, or start if they already exist
    boolean startInstances(Properties targets);

    // check for infrastructure status (could also return "does not exist" before creating)
    String getInstances(Properties targets);

    // check for infrastructure status (could also return "does not exist" before creating)
    String checkState(Properties targets);

    // alter infrastructure - update config, add or rempove nodes, etc.
    boolean alterInstances(Properties targets);

    // create infrastructure (instances, containers) to run services, or start if they already exist
    boolean stopInstances(Properties targets);

    // hibernate or delete infrastructure
    boolean destroyInstances(Properties targets);
}
