package com.vertica.devops;

import com.jcraft.jsch.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

public class SshUtil {
    final static Logger LOG = LogManager.getLogger(SshUtil.class);

    private Session getConnection(Properties params) throws Exception {
        JSch jsch = new JSch();
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
        LOG.info("SSH EXEC: "+command);
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
                LOG.info("exit-status: "+exitStatus);
                break;
            }
            try{Thread.sleep(1000);}catch(Exception ee){}
        }
        channel.disconnect();
        jschSession.disconnect();
        return exitStatus;
    }
}
