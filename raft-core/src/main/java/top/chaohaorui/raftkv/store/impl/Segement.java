package top.chaohaorui.raftkv.store.impl;

import lombok.Data;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.util.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class Segement {
    List<Record> records = new ArrayList<>();

    boolean isClosed = false;
    public String getLogFilePath() {
        return logFilePath;
    }

    String logFilePath;
    public Segement() {
        threadLocalRAF = new ThreadLocal<>();
        isClosed = false;
    }

    public void loadMetaData() throws IOException {
        RandomAccessFile randomAccessFile = getRandomAccessFile();
        long pre = (Long.SIZE / Byte.SIZE) * 1000L;
        for(int i = 0;i < 1000;i++){
            Record record = new Record();
            long endOffset = randomAccessFile.readLong();
            if(endOffset == 0) break;
            record.setStartOffset(pre);
            record.setEndOffset(endOffset);
            records.add(record);
            pre = endOffset;
        }
        endIndex = startIndex + records.size() - 1;
    }

    public void write(RaftProto.LogEntry entry) throws IOException {
        if(canWrite){
            RandomAccessFile randomAccessFile = getRandomAccessFile();
            long pos = entry.getIndex() - startIndex;
            randomAccessFile.seek(pos == 0 ? Long.SIZE / Byte.SIZE * 1000
                    : records.get((int) pos - 1).getEndOffset());
            Record record = new Record();

            record.setStartOffset(randomAccessFile.getFilePointer());
            FileUtils.writeProtoToFile(randomAccessFile,entry);

            record.setEndOffset(randomAccessFile.getFilePointer());
            records.add(record);
            randomAccessFile.seek(Long.SIZE / Byte.SIZE * pos);
            randomAccessFile.writeLong(record.getEndOffset());
            endIndex = entry.getIndex();
        }else throw new IOException("can not write");
    }

    public RaftProto.LogEntry get(long index) throws IOException {
        RandomAccessFile randomAccessFile = getRandomAccessFile();
        if(index > endIndex || index < startIndex) throw new RuntimeException("get index out of bound");
        Record record = records.get((int) (index - startIndex));
        long startOffset = record.getStartOffset();
        randomAccessFile.seek(startOffset);
        RaftProto.LogEntry logEntry = FileUtils.readProtoFromFile(randomAccessFile, RaftProto.LogEntry.class);
        return logEntry;
    }

    public RandomAccessFile getRandomAccessFile() {
        RandomAccessFile randomAccessFile = threadLocalRAF.get();
        if(randomAccessFile == null){
            try {
                randomAccessFile = new RandomAccessFile(logFilePath,"rws");
                threadLocalRAF.set(randomAccessFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return randomAccessFile;
    }

    public void delete(){
        File logFile = new File(logFilePath);
        isClosed = true;
        logFile.delete();
    }

    public void truncateSuffix(long startIndex) throws IOException {
        RandomAccessFile randomAccessFile = getRandomAccessFile();
        int pos = (int)(startIndex - this.startIndex) + 1;
        randomAccessFile.seek(Long.SIZE / Byte.SIZE * pos);
        for(int i = pos;i < 1000;i++){
            randomAccessFile.writeLong(0);
        }
    }

    public void close() {
        try {
            RandomAccessFile randomAccessFile = getRandomAccessFile();
            isClosed = true;
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void open(String logFilePath) {
        this.logFilePath= logFilePath;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(logFilePath,"rws");
            threadLocalRAF.remove();
            threadLocalRAF.set(randomAccessFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void open(File file) {
        logFilePath = file.getAbsolutePath();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(logFilePath,"rws");
            threadLocalRAF.remove();
            threadLocalRAF.set(randomAccessFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void setLogFile(String newLogFilePath) {
        File oldLogFile = new File(logFilePath);
        File newLogFile = new File(newLogFilePath);
        logFilePath = newLogFilePath;
        oldLogFile.renameTo(newLogFile);
    }

    @Data
    public static class Record{
        public long startOffset;
        public long endOffset;
        public RaftProto.LogEntry entry;
    }
    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    
    //每个线程都有一个RandomAccessFile，避免多线程读取时出现问题
    private ThreadLocal<RandomAccessFile> threadLocalRAF;
//    private RandomAccessFile randomAccessFile;

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }





}
