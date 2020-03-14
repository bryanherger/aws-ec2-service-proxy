package com.vertica.example;

import com.vertica.aws.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;
import java.util.Properties;

public class MonitorJob implements Job {
    final static Logger LOG = LogManager.getLogger(MonitorJob.class);
    private static boolean latch = false;
    private static int idleCount = 0;

    public void execute(JobExecutionContext context) throws JobExecutionException {
        // Say Hello to the World and display the date/time
        //Util.td();
        JobDataMap jdm = context.getMergedJobDataMap();
        Properties p = (Properties) jdm.get("params");
        int c = Util.countProxyThreads();
        LOG.info("Quartz cron job! "+c+" running proxy threads - " + new Date());
        if (c == 0 && "terminate".equalsIgnoreCase(p.getProperty("stopBehavior"))) {
            LOG.info("All proxy threads stopped! Terminating");
            idleCount = 0;
            latch = false;
            AwsVerticaService avs = new AwsVerticaService();
            avs.getClusterState(p);
            avs.destroyServices(p);
            AwsCloudProvider acp = new AwsCloudProvider();
            acp.init(p);
            acp.destroyInstances(p);
            LOG.info("Instances terminated, check AWS console for details");
            System.exit(0);
        }
        if (c > 0) { latch =  true; idleCount = 0; }
        if (c == 0 && latch) {
            LOG.info("All proxy threads stopped!");
            idleCount++;
            if (idleCount > 3) {
                // TODO: hibernate is probably better here, and make idleCount configurable
                LOG.info("All ProxyThread stopped for three checks, terminating instances");
                idleCount = 0;
                latch = false;
                AwsVerticaService avs = new AwsVerticaService();
                // get info for test
                avs.destroyServices(p);
                AwsCloudProvider acp = new AwsCloudProvider();
                acp.init(p);
                acp.destroyInstances(p);
                LOG.info("Instances terminated, check AWS console for details");
            }
        }
    }
}
