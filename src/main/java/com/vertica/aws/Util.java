package com.vertica.aws;

import java.util.Set;

public class Util {
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
            System.out.println("Thread No:" + i + " = " + lstThreads[i].getName());
        }
    }

    public static boolean lastProxyThread() {
        ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
        int noThreads = currentGroup.activeCount();
        Thread[] lstThreads = new Thread[noThreads];
        currentGroup.enumerate(lstThreads);
        int count = 0;
        for (int i = 0; i < noThreads; i++) {
            System.out.println("Thread No:" + i + " = " + lstThreads[i].getName());
            if (lstThreads[i].getName().contains("ProxyThread-")) {
                System.out.println("*** proxy thread");
                count++;
            }
        }
        return (count > 1 ? false : true);
    }
}
