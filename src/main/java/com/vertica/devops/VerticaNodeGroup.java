package com.vertica.devops;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

// this will represent a subcluster in Vertica vernacular.
public class VerticaNodeGroup {
    final static Logger LOG = LogManager.getLogger(VerticaNodeGroup.class);

    public String subcluster_oid,subcluster_name,parent_oid,parent_name;
    public boolean is_default,is_primary;
    public List<VerticaNode> verticaNodes = new ArrayList<>();

    public VerticaNodeGroup() { }

    public VerticaNodeGroup(String scRow) {
        this.init(scRow);
    }

    public void init(String subclustersRow) {
        LOG.info("init data: "+subclustersRow);
        String[] vals = subclustersRow.split("\\|");
        if (vals.length == 6) {
            subcluster_oid = vals[0];
            subcluster_name = vals[1];
            parent_oid = vals[2];
            parent_name = vals[3];
            is_default = "t".equalsIgnoreCase(vals[4]);
            is_primary = "t".equalsIgnoreCase(vals[5]);
            LOG.info("is_default = "+is_default+", is_primary = "+is_primary);
        } else {
            LOG.error("init data in wrong format!");
        }
    }
}
