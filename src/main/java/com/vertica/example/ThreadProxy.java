// Based on http://jcgonzalez.com/java-simple-proxy-socket-server-examples

package com.vertica.example;

import com.vertica.aws.AwsCloudProvider;
import com.vertica.aws.Util;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Properties;

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
    private String myThreadName;
    private String SERVER_URL;
    private int SERVER_PORT;
    // use this constructor if not sure instances are running
    ThreadProxy(Socket sClient, String ServerUrl, int ServerPort, AwsCloudProvider acp, Properties awsParams) {
        LOG.info("Verifying infrasructure");
        acp.startInstances(awsParams);
        LOG.info("Constructing ProxyThread");
        this.SERVER_URL = ServerUrl;
        this.SERVER_PORT = ServerPort;
        this.sClient = sClient;
        this.start();
    }
    // otherwise, use this constructor, which will probably start faster
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
            myThreadName = "ProxyThread-"+System.currentTimeMillis();
            setName(myThreadName);
            final byte[] request = new byte[1024];
            byte[] reply = new byte[4096];
            final InputStream inFromClient = sClient.getInputStream();
            final OutputStream outToClient = sClient.getOutputStream();
            Socket client = null, server = null;
            // connects a socket to the server
            try {
                System.out.println("Cluster DNS = "+ SERVER_URL);
                server = new Socket(SERVER_URL, SERVER_PORT);
            } catch (IOException e) {
                PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        outToClient));
                out.flush();
                throw new RuntimeException(e);
            }
            // a new thread to manage streams from client to server (UPLOAD)
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
        LOG.info("Finished "+myThreadName);
        Util.threadDump(); Util.td();
        LOG.info("Exiting "+myThreadName);
    }
}
