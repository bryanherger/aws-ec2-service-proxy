package com.vertica.devops;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.util.Properties;

public class SshUtil {
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
        jschSession.disconnect();
    }

    public void ssh(Properties params, String command) throws Exception {
        Session jschSession = getConnection(params);
        jschSession.disconnect();
    }
}
