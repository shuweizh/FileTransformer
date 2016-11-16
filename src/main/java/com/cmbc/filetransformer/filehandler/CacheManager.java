package com.cmbc.filetransformer.filehandler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by tangyuan on 16/11/14.
 */
public class CacheManager {
    private static Logger logger = Logger.getLogger(CacheManager.class);
    private String cacheFilePath;

    private String parentDir;
    private String pattern;
    private Boolean isIdleMode;
    private int idleMinTime;
    private int minDaysBefore;
    private int maxDaysBefore;

    private Map<Long, Map> cachedFiles;
    private Map<Long, File> needUpFiles;

    public CacheManager(String pDir, String fnPattern, Boolean idleMode, int idleMin, int daysBeforeMin, int daysBeforeMax, String cfPath){
        parentDir = pDir;
        pattern = fnPattern;
        isIdleMode = idleMode;
        idleMinTime = idleMin;
        maxDaysBefore = daysBeforeMax;
        minDaysBefore = daysBeforeMin;
        cacheFilePath = cfPath;

        cachedFiles = Maps.newHashMap();
        needUpFiles = Maps.newHashMap();
    }

    public void loadCache() {
        Long inode, lastModifiedTime;
        String path;
        FileReader fr = null;
        JsonReader jr = null;
        try {
            fr = new FileReader(cacheFilePath);
            jr = new JsonReader(fr);
            jr.beginArray();
            while (jr.hasNext()) {
                inode = null;
                lastModifiedTime = null;
                path = null;
                jr.beginObject();
                while (jr.hasNext()) {
                    switch (jr.nextName()) {
                        case "inode":
                            inode = jr.nextLong();
                            break;
                        case "lastModifiedTime":
                            lastModifiedTime = jr.nextLong();
                            break;
                        case "file":
                            path = jr.nextString();
                            break;
                    }
                }
                jr.endObject();

                for (Object v : Arrays.asList(inode, lastModifiedTime, path)) {
                    Preconditions.checkNotNull(v, "Detected missing value in position file. "
                            + "inode: " + inode + ", lastModifiedTime: " + lastModifiedTime + ", path: " + path);
                }
                cachedFiles.put(inode, ImmutableMap.of("path", path, "lastmodified", lastModifiedTime));
            }
            jr.endArray();
        } catch (FileNotFoundException e) {
            logger.info("File not found: " + cachedFiles + ", not updating position");
        } catch (IOException e) {
            logger.error("Failed loading positionFile: " + cachedFiles, e);
        } finally {
            try {
                if (fr != null) fr.close();
                if (jr != null) jr.close();
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }

    public void writeCache(){
        File file = new File(cacheFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            if (!cachedFiles.isEmpty()) {
                String json = mapToJson();
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

    public void updateCache(Long inode, Map fileProp){
        cachedFiles.put(inode, fileProp);
        logger.info("Update cache: " + inode + ", path:" + fileProp.get("path"));
    }

    public void updateCache(Long inode, File f){
        long lastModified = f.lastModified();
        String path = f.getAbsolutePath();
        cachedFiles.put(inode, ImmutableMap.of("path", path, "lastmodified", lastModified));
        logger.info("Update cache: " + inode + ", path:" + path);
    }

    public void deleteCache(Long inode){
        cachedFiles.remove(inode);
        logger.info("Delete cache: " + inode);
    }

    private long getInode(File file) throws IOException{
        long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
        return inode;
    }

    private long getLastModifiedTime(File file) throws IOException {
        long lastModifiedTime = file.lastModified();
        return lastModifiedTime;
    }

    private String mapToJson(){
        @SuppressWarnings("rawtypes")
        List<Map> cacheInfos = Lists.newArrayList();
        for (Long inode : cachedFiles.keySet()) {
            Map fileProp = cachedFiles.get(inode);
            cacheInfos.add(ImmutableMap.of("inode", inode, "path", fileProp.get("path"), "lastmodified", fileProp.get("lastmodified")));
        }
        return new Gson().toJson(cacheInfos);
    }

    public Map<Long, File> match(){
        File pDir = new File(parentDir);
        File[] allFiles = pDir.listFiles(new FileFilter(pattern, isIdleMode, idleMinTime, minDaysBefore, maxDaysBefore));

        needUpFiles.clear();
        // 删掉不存在的
        // 更新已经存在的
        for (File file : allFiles){
            try {
                long inode = getInode(file);
                if (cachedFiles.keySet().contains(inode)){
                    if (file.lastModified() > (long) cachedFiles.get(inode).get("lastModified")){
                        needUpFiles.put(inode, file);
                        logger.info("File exist, update: " + file.getAbsolutePath());
                    }
                }else{
                    needUpFiles.put(inode, file);
                    logger.info("File not exist, update: " + file.getAbsolutePath());
                }
            }catch(Exception e){
                logger.error("Match file error: ");
            }
        }
        return needUpFiles;
    }
}