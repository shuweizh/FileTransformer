package com.cmbc.filetransformer.ftp;

/**
 * Created by tangyuan on 16/8/17.
 */
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.SocketException;

public class FtpUtil {
    private static Logger logger = Logger.getLogger(FtpUtil.class);

    private FTPClient ftpClient;

    private FtpConfig ftpConfig;

    public FtpUtil(FtpConfig ftpconfig) {
        super();
        // 从配置文件中读取初始化信息  
        this.ftpClient = new FTPClient();
        this.ftpConfig = ftpconfig;
    }

    /**
     * 连接并登录FTP服务器 
     *
     */
    public boolean ftpLogin() {
        boolean isLogin = false;
        FTPClientConfig ftpClientConfig = new FTPClientConfig(
                FTPClientConfig.SYST_NT);
        this.ftpClient.setControlEncoding("GBK");
        this.ftpClient.configure(ftpClientConfig);
        try {
            if (this.ftpConfig.getPort() > 0) {
                this.ftpClient.connect(ftpConfig.getUrl(), ftpConfig.getPort());
            } else {
                this.ftpClient.connect(ftpConfig.getUrl());
            }
            // FTP服务器连接回答  
            int reply = this.ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                this.ftpClient.disconnect();
                return isLogin;
            }
            this.ftpClient.login(this.ftpConfig.getUsername(), this.ftpConfig
                    .getPassword());
            this.ftpClient.changeWorkingDirectory(this.ftpConfig.getRemoteDir());
            this.ftpClient.setFileType(FTPClient.FILE_STRUCTURE);
            logger.info("成功登陆FTP服务器：" + this.ftpConfig.getUrl() + " 端口号："
                    + this.getFtpConfig().getPort() + " 目录："
                    + this.ftpConfig.getRemoteDir());
            isLogin = true;
        } catch (SocketException e) {
            e.printStackTrace();
            logger.error("连接FTP服务失败！" + this.ftpConfig.getUrl() + " 端口号："
                    + this.getFtpConfig().getPort() + " 目录："
                    + this.ftpConfig.getRemoteDir());
            logger.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("连接FTP服务失败！" + this.ftpConfig.getUrl() + " 端口号："
                    + this.getFtpConfig().getPort() + " 目录："
                    + this.ftpConfig.getRemoteDir());
            logger.error(e.getMessage());
        }
        System.out.println(this.ftpClient.getBufferSize());
        this.ftpClient.setBufferSize(1024 * 2);
        this.ftpClient.setDataTimeout(2000);
        return isLogin;
    }

    /**
     * 退出并关闭FTP连接 
     *
     */
    public void close() {
        if (null != this.ftpClient && this.ftpClient.isConnected()) {
            try {
                boolean reuslt = this.ftpClient.logout();// 退出FTP服务器  
                if (reuslt) {
                    logger.info("退出并关闭FTP服务器的连接");
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("退出FTP服务器异常！");
                logger.error(e.getMessage());
            } finally {
                try {
                    this.ftpClient.disconnect();// 关闭FTP服务器的连接  
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error("关闭FTP服务器的连接异常！");
                    logger.error(e.getMessage());
                }
            }
        }
    }

    /**
     * 检查FTP服务器是否关闭 ，如果关闭接则连接登录FTP 
     *
     * @return
     */
    public boolean isOpenFTPConnection() {
        boolean isOpen = false;
        if (null == this.ftpClient) {
            return false;
        }
        try {
            // 没有连接  
            if (!this.ftpClient.isConnected()) {
                isOpen = this.ftpLogin();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("FTP服务器连接登录异常！");
            logger.error(e.getMessage());
            isOpen = false;
        }
        return isOpen;
    }

    /**
     * 设置传输文件的类型[文本文件或者二进制文件] 
     *
     * @param fileType--FTPClient.BINARY_FILE_TYPE,FTPClient.ASCII_FILE_TYPE 
     */
    public void setFileType(int fileType) {
        try {
            this.ftpClient.setFileType(fileType);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("设置传输文件的类型异常！");
            logger.error(e.getMessage());
        }
    }

    /**
     * 下载文件 
     *
     * @param localFilePath
     *            本地文件名及路径 
     * @param remoteFileName
     *            远程文件名称 
     * @return
     */
    public boolean downloadFile(String localFilePath, String remoteFileName) {
        BufferedOutputStream outStream = null;
        boolean success = false;
        try {
            outStream = new BufferedOutputStream(new FileOutputStream(
                    localFilePath));
            success = this.ftpClient.retrieveFile(remoteFileName, outStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outStream != null) {
                try {
                    outStream.flush();
                    outStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return success;
    }

    /**
     * 下载文件 
     *
     * @param localFile
     *            本地文件 
     * @param remoteFileName
     *            远程文件名称 
     * @return
     */
    public boolean downloadFile(File localFile, String remoteFileName) {
        BufferedOutputStream outStream = null;
        FileOutputStream outStr = null;
        boolean success = false;
        try {
            outStr = new FileOutputStream(localFile);
            outStream = new BufferedOutputStream(outStr);
            success = this.ftpClient.retrieveFile(remoteFileName, outStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != outStream) {
                    try {
                        outStream.flush();
                        outStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null != outStr) {
                    try {
                        outStr.flush();
                        outStr.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
        return success;
    }

    /**
     * 上传文件 
     *
     * @param localFilePath
     *            本地文件路径及名称 
     * @param remoteFileName
     *            FTP 服务器文件名称 
     * @return
     */
    public boolean uploadFile(String localFilePath, String remoteFileName) {
        BufferedInputStream inStream = null;
        boolean success = false;
        try {
            inStream = new BufferedInputStream(new FileInputStream(
                    localFilePath));
            success = this.ftpClient.storeFile(remoteFileName, inStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return success;
    }

    /**
     * 上传文件 
     *
     * @param localFilePath
     *            本地文件 
     * @param remoteFileName
     *            FTP 服务器文件名称 
     * @return
     */
    public boolean uploadFile(File localFile, String remoteFileName) {
        BufferedInputStream inStream = null;
        boolean success = false;
        try {
            inStream = new BufferedInputStream(new FileInputStream(localFile));
            success = this.ftpClient.storeFile(remoteFileName, inStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return success;
    }

    /**
     * 变更工作目录 
     *
     * @param remoteDir--目录路径 
     */
    public void changeDir(String remoteDir) {
        try {
            this.ftpClient.changeWorkingDirectory(remoteDir);
            logger.info("变更工作目录为:" + remoteDir);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("变更工作目录为:" + remoteDir + "时出错！");
            logger.error(e.getMessage());
        }

    }

    /**
     * 变更工作目录 
     *
     * @param remoteDir--目录路径 
     */
    public void changeDir(String[] remoteDirs) {
        String dir = "";
        try {
            for (int i = 0; i < remoteDirs.length; i++) {
                this.ftpClient.changeWorkingDirectory(remoteDirs[i]);
                dir = dir + remoteDirs[i] + "/";
            }
            logger.info("变更工作目录为:" + dir);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("变更工作目录为:" + dir + "时出错！");
            logger.error(e.getMessage());
        }

    }

    /**
     * 返回上级目录 
     *
     */
    public void toParentDir(String[] remoteDirs) {
        try {
            for (int i = 0; i < remoteDirs.length; i++) {
                this.ftpClient.changeToParentDirectory();
            }
            logger.info("返回上级目录");

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("返回上级目录时出错！");
            logger.error(e.getMessage());
        }
    }

    /**
     * 返回上级目录 
     *
     */
    public void toParentDir() {
        try {
            this.ftpClient.changeToParentDirectory();
            logger.info("返回上级目录");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("返回上级目录时出错！");
            logger.error(e.getMessage());
        }
    }

    /**
     * 获得FTP 服务器下所有的文件名列表 
     *
     * @return
     */
    public String[] getListFiels() {
        String files[] = null;
        try {
            files = this.ftpClient.listNames();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }

}  