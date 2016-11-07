package com.cmbc.filetransformer.filehandler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * Created by tangyuan on 16/8/23.
 */

public class FileMatcher{
    private static Logger logger = Logger.getLogger(FileMatcher.class);

    private String parentDir;
    private String filePattern;

    private boolean idleMode;
    private int idleTime;

    private String sendFilePath;
    private Map<Long, String> sendFileLog;
    private Map<Long, String> updateFileLog;



    public FileMatcher(String pDir, String fPattern, boolean isIdleMode, int idleMin, String sFilePath){
        parentDir = pDir;
        filePattern = fPattern;
        idleMode = isIdleMode;
        idleTime = idleMin;
        sendFilePath = sFilePath;
    }

    public ArrayList<File> scan(String parentDir, final String filePattern){
        ArrayList<File> resultList = Lists.newArrayList();
        try {
            File parentDirFile = new File(parentDir);
            FileFilter filter = new FileFilter(filePattern, idleMode, idleTime);
            File[] tmpList = parentDirFile.listFiles(filter);
            resultList = (ArrayList<File>) Arrays.asList(tmpList);

        }catch(Exception e){
            logger.error("Scan Error:" + e.getMessage());
        }
        finally {
            return resultList;
        }
    }
    private long getInode(File file) throws IOException {
        long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
        return inode;
    }

    private void writeSendLog() {
        File file = new File(sendFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            if (!existingInodes.isEmpty()) {
                String json = toSendLogJson();
                writer.write(json);
            }
        } catch (Throwable t) {
            logger.error("Failed writing positionFile", t);
        } finally {
            try {
                if (writer != null) writer.close();
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }

    private String toSendLogJson() {
        @SuppressWarnings("rawtypes")
        List<Map> posInfos = Lists.newArrayList();
        for (Long inode : existingInodes) {
            TailFile tf = reader.getTailFiles().get(inode);
            posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
        }
        return new Gson().toJson(posInfos);
    }

    /**
     * Load a position file which has the last read position of each file.
     * If the position file exists, update tailFiles mapping.
     */
    public void loadSendLogFile(String filePath) {
        Long inode, pos;
        String path;
        FileReader fr = null;
        JsonReader jr = null;
        try {
            fr = new FileReader(filePath);
            jr = new JsonReader(fr);
            jr.beginArray();
            while (jr.hasNext()) {
                inode = null;
                pos = null;
                path = null;
                jr.beginObject();
                while (jr.hasNext()) {
                    switch (jr.nextName()) {
                        case "inode":
                            inode = jr.nextLong();
                            break;
                        case "pos":
                            pos = jr.nextLong();
                            break;
                        case "file":
                            path = jr.nextString();
                            break;
                    }
                }
                jr.endObject();

                for (Object v : Arrays.asList(inode, pos, path)) {
                    Preconditions.checkNotNull(v, "Detected missing value in position file. "
                            + "inode: " + inode + ", pos: " + pos + ", path: " + path);
                }
                TailFile tf = tailFiles.get(inode);
                if (tf != null && tf.updatePos(path, inode, pos)) {
                    tailFiles.put(inode, tf);
                } else {
                    logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
                }
            }
            jr.endArray();
        } catch (FileNotFoundException e) {
            logger.info("File not found: " + filePath + ", not updating position");
        } catch (IOException e) {
            logger.error("Failed loading positionFile: " + filePath, e);
        } finally {
            try {
                if (fr != null) fr.close();
                if (jr != null) jr.close();
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }
}



