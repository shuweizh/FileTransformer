package com.cmbc.filetransformer.filehandler;


import com.cmbc.filetransformer.ftp.FtpConfig;
import com.cmbc.filetransformer.ftp.FtpUtil;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Map;

/**
 * Created by tangyuan on 16/10/11.
 * 主类,1. 读取配置文件 2.读取缓存的文件发送记录 3.循环扫描并发送
 */
public class FileTransformer {
    private static Logger logger = Logger.getLogger(FileTransformer.class);

    private static CacheManager cm = new CacheManager("/Users/tangyuan/Projects/FileTransformer/test", "[a-z].log", false, 0, 1, 5, "cacheFile.txt");
    private static Map<Long, File> updateFiles = Maps.newHashMap();

    private static FtpUtil ftpUtil = new FtpUtil(new FtpConfig("","","",0,""));

    public static void main(String[] args){
        cm.loadCache();
        updateFiles = cm.match();

        for (Map.Entry<Long, File> entry : updateFiles.entrySet()){
            Long inode = entry.getKey();
            File file = entry.getValue();
            ftpUtil.uploadFile(file, file.getName());
            cm.updateCache(inode, file);
            cm.writeCache();
        }
    }
}
