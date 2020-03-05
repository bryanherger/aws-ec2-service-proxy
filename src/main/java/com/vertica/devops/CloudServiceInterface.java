package com.vertica.devops;

/* Manage software/service lifecycle */

import java.util.Properties;

public interface CloudServiceInterface {
    // create services, or start if they already exist
    boolean createServices(Properties targets);

    // check for service status (could also return "does not exist" before creating)
    String checkState(Properties targets);

    // alter service - update config, add or rempove nodes, etc.
    boolean alterServices(Properties targets);

    // delete service (may be done implicitly by infrastructure delete; handle e.g. cleanup or data flush here
    boolean destroyServices(Properties targets);
}
