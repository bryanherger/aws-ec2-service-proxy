package com.vertica.devops;

import com.jcraft.jsch.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;

public class SshUtil {
    final static Logger LOG = LogManager.getLogger(SshUtil.class);

    // this is kind of generic but will put here anyways for now
    public static boolean testConnection(String host, int port) {
        boolean connectionStatus=false;

        try {
            Socket Skt = new Socket(host, port);
            connectionStatus = true;
            Skt.close();
        } catch (UnknownHostException e) {
        } catch (IOException e) {}

        return connectionStatus;
    }

    private Session getConnection(Properties params) throws Exception {
        int count = 3;
        while (count > 0) {
            try {
                JSch jsch = new JSch();
                LOG.warn("Connecting to "+params.getProperty("node")+" as "+params.getProperty("DBUSER"));
                Session jschSession = jsch.getSession(params.getProperty("DBUSER"), params.getProperty("node"));
                java.util.Properties config = new java.util.Properties();
                // ignore host key since it changes for each new cloud instance
                config.put("StrictHostKeyChecking", "no");
                jschSession.setConfig(config);
                // set key file
                String keyFile = params.getProperty("VERTICA_PEM_KEYFILE");
                jsch.addIdentity(keyFile);
                jschSession.connect();
                return jschSession;
            } catch (Exception e) {
                //e.printStackTrace();
                LOG.error(e.getMessage(), e);
                LOG.error(params.toString());
                LOG.error("Trying again in 5 seconds");
                Thread.sleep(5000L);
                count--;
            }
        }
        return null;
    }

    public void sftp(Properties params, String src, String dst) throws Exception {
        Session jschSession = getConnection(params);
        LOG.info("SFTP: "+src+" to "+dst);
        ChannelSftp csftp = (ChannelSftp) jschSession.openChannel("sftp");
        csftp.connect();
        csftp.put(src, dst);
        csftp.exit();
        jschSession.disconnect();
    }

    public int ssh(Properties params, String command) throws Exception {
        int exitStatus = -1;
        Session jschSession = getConnection(params);
        LOG.info(params.getProperty("node")+"|SSH EXEC: "+command);
        Channel channel=jschSession.openChannel("exec");
        ((ChannelExec)channel).setCommand(command);
        channel.setInputStream(null);
        ((ChannelExec)channel).setErrStream(System.err);
        InputStream in=channel.getInputStream();
        channel.connect();
        byte[] tmp=new byte[1024];
        while(true){
            while(in.available()>0){
                int i=in.read(tmp, 0, 1024);
                if(i<0)break;
                LOG.info(new String(tmp, 0, i));
            }
            if(channel.isClosed()){
                if(in.available()>0) continue;
                exitStatus = channel.getExitStatus();
                if (exitStatus > 0) {
                    LOG.error("!!!");
                    LOG.error("!!! remote command failed!");
                    LOG.error("!!! exit-status: " + exitStatus);
                    LOG.error("!!!");
                    Thread.sleep(1000L);
                } else {
                    LOG.info("exit-status: " + exitStatus);
                }
                break;
            }
            try{Thread.sleep(1000);}catch(Exception ee){}
        }
        channel.disconnect();
        jschSession.disconnect();
        return exitStatus;
    }

    public String sshWithOutput(Properties params, String command) throws Exception {
        int exitStatus = -1;
        Session jschSession = getConnection(params);
        LOG.info(params.getProperty("node")+"|SSH EXEC with output: "+command);
        Channel channel=jschSession.openChannel("exec");
        ((ChannelExec)channel).setCommand(command);
        channel.setInputStream(null);
        ((ChannelExec)channel).setErrStream(System.err);
        InputStream in=channel.getInputStream();
        channel.connect();
        String output = "";
        byte[] tmp=new byte[1024];
        while(true){
            while(in.available()>0){
                int i=in.read(tmp, 0, 1024);
                if(i<0)break;
                String thisBatch = new String(tmp, 0, i);
                LOG.info(thisBatch);
                output = output + thisBatch;
            }
            if(channel.isClosed()){
                if(in.available()>0) continue;
                exitStatus = channel.getExitStatus();
                if (exitStatus > 0) {
                    LOG.error("!!!");
                    LOG.error("!!! remote command failed!");
                    LOG.error("!!! exit-status: " + exitStatus);
                    LOG.error("!!!");
                    throw new Exception("ssh failed with exit-status: "+exitStatus);
                } else {
                    LOG.info("exit-status: " + exitStatus);
                }
                break;
            }
            try{Thread.sleep(1000);}catch(Exception ee){}
        }
        channel.disconnect();
        jschSession.disconnect();
        return output;
    }
}
