package com.vertica.aws;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class ScheduleUtil {
    final static Logger LOG = LogManager.getLogger(LogUtil.class);

    public static void scheduleInit() {
        try {
            // First we must get a reference to a scheduler
            SchedulerFactory sf = new StdSchedulerFactory();
            Scheduler sched = sf.getScheduler();
            // define the job and tie it to our HelloJob class
            JobDetail job = newJob(MonitorJob.class).withIdentity("job1", "group1").build();
            // Trigger the job to run on the next round minute
            //Trigger trigger = newTrigger().withIdentity("trigger1", "group1").startAt(runTime).build();
            Trigger trigger = newTrigger()
                    .withIdentity("trigger3", "group1")
                    .withSchedule(cronSchedule("0 0/2 * * * ?"))
                    .build();
            // Tell quartz to schedule the job using our trigger
            sched.scheduleJob(job, trigger);
            // Start up the scheduler (nothing can actually run until the
            // scheduler has been started)
            sched.start();
            LOG.info("------- Started Scheduler -----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
