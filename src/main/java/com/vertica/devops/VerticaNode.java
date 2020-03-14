package com.vertica.devops;

import com.vertica.example.AwsVerticaDemo;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// this will represent a "node" in Vertica vernacular.
public class VerticaNode {
    final static Logger LOG = LogManager.getLogger(VerticaNode.class);

    public String node_name,node_id,node_state,is_primary,node_address,node_address_family,export_address,export_address_family,catalog_path,node_type,is_ephemeral,standing_in_for,subcluster_name,last_msg_from_node_at,node_down_since;
    public boolean primary, ephemeral;
    public String instanceId = null, publicDns = null, privateDns = null;
    public long created = System.currentTimeMillis(), updated = System.currentTimeMillis();
    public double loadAvg = 0.0;

    public VerticaNode() { }

    public VerticaNode(String nRow) {
        init(nRow);
    }

    public void init(String nodesRow) {
        LOG.info("init data: " + nodesRow);
        String[] vals = nodesRow.split("\\|");
        if (vals.length == 15) {
            node_name = vals[0];
            node_id = vals[1];
            node_state = vals[2];
            primary = "t".equalsIgnoreCase(vals[3]);
            catalog_path = vals[8];
            node_type = vals[9];
            ephemeral = "t".equalsIgnoreCase(vals[10]);
            subcluster_name = vals[12];
            LOG.info("subcluster = "+subcluster_name+", is_primary = " + primary + ", is_ephemeral = " + ephemeral);
        } else {
            LOG.error("init data in wrong format! "+vals.length);
        }
    }

    public double getLoadAvg() {
        return loadAvg;
    }
}
