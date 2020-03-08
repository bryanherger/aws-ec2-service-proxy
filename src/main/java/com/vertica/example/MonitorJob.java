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
        int c = Util.countProxyThreads();
        LOG.info("Quartz cron job! "+c+" running proxy threads - " + new Date());
        if (c > 0) { latch =  true; idleCount = 0; }
        if (c == 0 /*&& latch*/) {
            LOG.info("All proxy threads stopped!");
            idleCount++;
            if (idleCount > 0) {
                LOG.info("All ProxyThread stopped for three checks, stopping instance");
                JobDataMap jdm = context.getMergedJobDataMap();
                /*AwsCloudProvider acp = new AwsCloudProvider();
                acp.init((Properties) jdm.get("params"));
                acp.stopInstances((Properties) jdm.get("params"));
                idleCount = 0;
                latch = false;*/
                Properties p = (Properties) jdm.get("params");
                AwsVerticaService avs = new AwsVerticaService();
                avs.destroyServices(p);
                AwsSpotInstanceManager asim = new AwsSpotInstanceManager();
                asim.terminateSpotInstances(p);
                System.exit(0);
            }
        }
    }
}
