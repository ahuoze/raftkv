package top.chaohaorui.raftkv.store.impl;

import com.sun.source.tree.Tree;
import org.checkerframework.checker.units.qual.t;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.store.LogSegment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.TreeMap;

public class DefaultLogSegment implements LogSegment {
    TreeMap<Long,Segement> segementMap = new TreeMap<>();
    String logDirPath;
    @Override
    public void init(String logDirPath) {
        this.logDirPath = logDirPath + File.separator + "log";
        File logDirFile = new File(this.logDirPath);
        if(!logDirFile.exists()){
            logDirFile.mkdirs();
        }
        for (File file : logDirFile.listFiles()) {
            String name = file.getName();
            if(!name.endsWith(".log")){
                if(file.isFile()) file.delete();
                continue;
            }
            String[] startAndEnd = name.substring(0, name.indexOf(".")).split("_");
            Segement segement = new Segement();
            segement.open(file);
            long start = Long.parseLong(startAndEnd[0]);
            segement.setStartIndex(start);
            if(!startAndEnd[1].equals("open")){
                long end = Long.parseLong(startAndEnd[1]);
                segement.setEndIndex(end);
            }else segement.setCanWrite(true);
            try {
                segement.loadMetaData();
            } catch (IOException e) {
                e.printStackTrace();
            }
            segementMap.put(start,segement);
        }
    }

    @Override
    public void write(RaftProto.LogEntry entry) throws IOException {
        Map.Entry<Long, Segement> mapEntry = segementMap.lastEntry();
        if(mapEntry == null){
            newSegementFile(entry);
            return;
        }
        Segement segement = mapEntry.getValue();
        if(!segement.isCanWrite()){
            newSegementFile(entry);
            return;
        }
        if(segement.records.size() >= 1000){
            String newLogFilePath = this.logDirPath + File.separator +
                    segement.getStartIndex() + "_" + segement.getEndIndex() + ".log";
            segement.setLogFile(newLogFilePath);
            segement.setCanWrite(false);
            newSegementFile(entry);
            return;
        }
        try{
            segement.write(entry);
        }catch (IOException e){
            if(e.toString().contains("Stream Closed")){
                segement.open(segement.getLogFilePath());
                segement.write(entry);
            }else throw e;
        }
    }


    private void newSegementFile(RaftProto.LogEntry entry) throws IOException {
        long startIndex = entry.getIndex();
        String newLogFilePath = this.logDirPath + File.separator + startIndex + "_open";
        File newLogFile = new File(newLogFilePath);
        Segement segement = new Segement();
        try {
            newLogFile.createNewFile();
            segement.open(newLogFile);
            segement.setStartIndex(startIndex);
            segement.setEndIndex(startIndex);
            segement.setCanWrite(true);
            RandomAccessFile randomAccessFile = segement.getRandomAccessFile();
            for(int i = 0;i < 1000;i++) randomAccessFile.writeLong(0L);
            segementMap.put(startIndex,segement);
            write(entry);
            newLogFilePath = this.logDirPath + File.separator + startIndex + "_open.log";
            segement.close();
            segement.setLogFile(newLogFilePath);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("create new log file error");
        }
    }

    @Override
    public RaftProto.LogEntry get(long index) throws IOException {
        Map.Entry<Long, Segement> longSegementEntry = segementMap.floorEntry(index);
        if(longSegementEntry == null) return null;
        Segement segement = longSegementEntry.getValue();
        if(index > segement.getEndIndex()) return  null;
        try{
            return segement.get(index);
        }catch (IOException e){
            if(e.toString().contains("Stream Closed")){
                segement.open(segement.getLogFilePath());
                return segement.get(index);
            }else throw e;
        }
    }

    @Override
    public void truncateSuffix(long startIndex) throws IOException {
        TreeMap<Long, Segement> newSegementTreeMap = new TreeMap<>();
        Segement targetSegement = null;
        for(Map.Entry<Long, Segement> entry : segementMap.entrySet()){
            Segement segement = entry.getValue();
            if(segement.getStartIndex() > startIndex){
                segement.delete();
            }else {
                newSegementTreeMap.put(entry.getKey(),segement);
                if(segement.getStartIndex() <= startIndex && segement.getEndIndex() >= startIndex){
                    targetSegement = segement;
                }
            }
        }
        segementMap = newSegementTreeMap;
        if(targetSegement != null) {
            try{
                targetSegement.truncateSuffix(startIndex);
            }catch (IOException e){
                if(e.toString().contains("Stream Closed")){
                    targetSegement.open(targetSegement.getLogFilePath());
                    targetSegement.truncateSuffix(startIndex);
                }else throw e;
            }
        }
            
    }

    @Override
    public void truncatePrefix(long startIndex) throws IOException {
        TreeMap<Long, Segement> newSegementTreeMap = new TreeMap<>();
        segementMap.forEach((k,segement) -> {
            if(segement.getEndIndex() < startIndex){
                segement.delete();
            }else {
                newSegementTreeMap.put(k,segement);
            }
        });
        segementMap = newSegementTreeMap;
    }
}
