package com.vertica.aws;

import com.vertica.example.ThreadProxy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Set;

public class Util {
    final static Logger LOG = LogManager.getLogger(Util.class);

    public static void threadDump() {
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        int count = 0;
        for (Thread t : threadSet) {
            String name = t.getName();
            Thread.State state = t.getState();
            int priority = t.getPriority();
            String type = t.isDaemon() ? "Daemon" : "Normal";
            System.out.printf("%d : %-20s \t %s \t %d \t %s\n", count++, name, state, priority, type);
        }
    }

    public static void td() {
        ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
        int noThreads = currentGroup.activeCount();
        Thread[] lstThreads = new Thread[noThreads];
        currentGroup.enumerate(lstThreads);

        for (int i = 0; i < noThreads; i++) {
            LOG.info("Thread No:" + i + " = " + lstThreads[i].getName());
        }
    }

    public static boolean lastProxyThread() {
        return (countProxyThreads() > 1 ? false : true);
    }

    public static int countProxyThreads() {
        ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
        int noThreads = currentGroup.activeCount();
        Thread[] lstThreads = new Thread[noThreads];
        currentGroup.enumerate(lstThreads);
        int count = 0;
        for (int i = 0; i < noThreads; i++) {
            if (lstThreads[i].getName().contains("ProxyThread-")) {
                LOG.info("*** proxy thread: Thread No:" + i + " = " + lstThreads[i].getName());
                count++;
            }
        }
        return count;
    }
}
