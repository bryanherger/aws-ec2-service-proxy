package com.vertica.aws;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;

/**
 * Handles a socket connection to the proxy server from the client and uses 2
 * threads to proxy between server and client
 *
 * @author jcgonzalez.com
 *
 */
public class ThreadProxy extends Thread {
    final static Logger LOG = LogManager.getLogger(ThreadProxy.class);

    private Socket sClient;
    private String SERVER_URL;
    private int SERVER_PORT;
    ThreadProxy(Socket sClient, String ServerUrl, int ServerPort) {
        LOG.info("Constructing ProxyThread");
        this.SERVER_URL = ServerUrl;
        this.SERVER_PORT = ServerPort;
        this.sClient = sClient;
        this.start();
    }
    @Override
    public void run() {
        try {
            LOG.info("Started ProxyThread");
            setName("ProxyThread-"+System.currentTimeMillis());
            final byte[] request = new byte[1024];
            byte[] reply = new byte[4096];
            final InputStream inFromClient = sClient.getInputStream();
            final OutputStream outToClient = sClient.getOutputStream();
            Socket client = null, server = null;
            String instanceId = "i-0496825e4f6bfcfb4";
            // connects a socket to the server
            try {
                System.out.println("Checking whether to wake server");
                String sts = AwsUtil.getInstanceState(instanceId);
                if (sts.contains("stop")) {
                    Main.testStartInstance(instanceId);
                }
                SERVER_URL = AwsUtil.getInstancePublicDns(instanceId);
                int count = 15;
                while (VerticaUtil.checkIfAlive(SERVER_URL) == false) {
                    count--;
                    if (count <= 0) { throw new IOException("Vertica is not available!"); }
                    try {
                        sleep(5000);
                    } catch (Exception e) {

                    }
                }
                System.out.println("Cluster DNS = "+ SERVER_URL);
                server = new Socket(SERVER_URL, SERVER_PORT);
            } catch (IOException e) {
                PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        outToClient));
                out.flush();
                throw new RuntimeException(e);
            }
            // a new thread to manage streams from server to client (DOWNLOAD)
            final InputStream inFromServer = server.getInputStream();
            final OutputStream outToServer = server.getOutputStream();
            // a new thread for uploading to the server
            new Thread() {
                public void run() {
                    int bytes_read;
                    try {
                        System.out.println("Starting >>> client-server thread");
                        while ((bytes_read = inFromClient.read(request)) != -1) {
                            outToServer.write(request, 0, bytes_read);
                            outToServer.flush();
                            //TODO CREATE YOUR LOGIC HERE
                        }
                    } catch (IOException e) {
                    }
                    try {
                        outToServer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
            // current thread manages streams from server to client (DOWNLOAD)
            int bytes_read;
            try {
                System.out.println("Starting <<< server-client thread");
                while ((bytes_read = inFromServer.read(reply)) != -1) {
                    outToClient.write(reply, 0, bytes_read);
                    outToClient.flush();
                    //TODO CREATE YOUR LOGIC HERE
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (server != null)
                        server.close();
                    if (client != null)
                        client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            outToClient.close();
            sClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Finished ProxyThread");
        Util.threadDump(); Util.td();
        // the scheduler handles this now
        /*if (Util.lastProxyThread()) {
            LOG.info("Last ProxyThread, stopping instance");
            VerticaUtil.checkIfActive(SERVER_URL);
            String instanceId = "i-0496825e4f6bfcfb4";
            Main.testHibernateInstance(instanceId);
        }*/
        LOG.info("Exiting ProxyThread");
    }
}
