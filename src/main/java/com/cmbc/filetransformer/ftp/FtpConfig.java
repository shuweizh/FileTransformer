package com.cmbc.filetransformer.ftp;

/**
 * Created by tangyuan on 16/8/17.
 */
public class FtpConfig {
    private String username;

    private String password;

    private String url;

    private int port;

    private String remoteDir;

    public FtpConfig() {

    }

    public FtpConfig(String username, String password, String url, int port,
                    String remoteDir) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.port = port;
        this.remoteDir = remoteDir;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRemoteDir() {
        return remoteDir;
    }

    public void setRemoteDir(String remoteDir) {
        this.remoteDir = remoteDir;
    }
}