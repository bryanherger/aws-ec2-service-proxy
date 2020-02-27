package com.vertica.aws;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;

public class MonitorJob implements Job {
    final static Logger LOG = LogManager.getLogger(LogUtil.class);
    private static boolean latch = false;
    private static int idleCount = 0;

    public void execute(JobExecutionContext context) throws JobExecutionException {
        // Say Hello to the World and display the date/time
        //Util.td();
        int c = Util.countProxyThreads();
        LOG.info("Quartz cron job! "+c+" running proxy threads - " + new Date());
        if (c > 0) { latch =  true; idleCount = 0; }
        if (c == 0 && latch) {
            LOG.info("All proxy threads stopped!");
            idleCount++;
            if (idleCount > 2) {
                LOG.info("All ProxyThread stopped for three checks, stopping instance");
                String instanceId = "i-0496825e4f6bfcfb4";
                Main.testHibernateInstance(instanceId);
            }
        }
    }
}
