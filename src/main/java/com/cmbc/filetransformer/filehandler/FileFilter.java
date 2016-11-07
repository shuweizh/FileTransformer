package com.cmbc.filetransformer.filehandler;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by tangyuan on 16/8/23.
 */
public class FileFilter implements FilenameFilter{
    private static Logger logger = Logger.getLogger(FileFilter.class);

    private Pattern pattern;
    private boolean isIdleMode;
    private int idleMin;

    public FileFilter(String regex, boolean idleMode, int idleTime) {
        // TODO Auto-generated constructor stub
        pattern= Pattern.compile(regex);
        isIdleMode = idleMode;
        idleMin = idleTime;
    }

    public boolean accept(File dir, String name) {
        // TODO Auto-generated method stub
        boolean result=false;
        Path tmpPath = Paths.get(dir.getAbsolutePath(), name);
        File tmpFile = tmpPath.toFile();
        boolean isFile = tmpFile.exists() && tmpFile.isFile();
        boolean isMatch = pattern.matcher(name).matches();

        result = isFile && isMatch;
        if(isIdleMode) {
            boolean isIdleEnough = isIdleEnough(tmpFile);
            result = result && isIdleEnough;
        }else {
            boolean isOpened = isOpened(tmpFile);
            result = result && isOpened;
        }
        return result;
    }

    private boolean isOpened(File file){
        boolean result=false;
        try {
            FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            // Get an exclusive lock on the whole file
            FileLock lock = channel.lock();
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                // File is open by someone else
                result = true;
            } finally {
                lock.release();
            }
        } catch(Exception e){
            logger.error("Filter Error:" + e.getMessage());
        }
        finally {
            return result;
        }
    }

    private boolean isIdleEnough(File file){
        long lastModifyTimeStamp = file.lastModified();
        Date now = new Date();
        long diff = now.getTime() - lastModifyTimeStamp;
        long diffMin = diff/(1000 * 60);
        if (diffMin >= idleMin){
            return true;
        }else{
            return false;
        }
    }
}


