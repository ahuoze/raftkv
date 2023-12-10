package top.chaohaorui.raftkv.store.impl;

import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.store.LogModule;
import top.chaohaorui.raftkv.store.Snapshot;
import top.chaohaorui.raftkv.util.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DefaultLogModule extends LogModule {
    public DefaultLogModule(Snapshot snapshot,String logDir) {
        super(snapshot,logDir);
    }

    @Override
    public void init() throws RuntimeException {

        String metaFilePath = logDir + File.separator + "metaData";
        if(!new File(metaFilePath).exists()){
            try {
                new File(metaFilePath).getParentFile().mkdirs();
                new File(metaFilePath).createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("create meta file error");
            }
        }
        RandomAccessFile metaFile = null;
        try {
            metaFile = new RandomAccessFile(metaFilePath, "rw");
            metaData = FileUtils.readProtoFromFile(metaFile,RaftProto.MetaData.class);
            if(metaData == null){
                RaftProto.MetaData.Builder metaDataBuilder = RaftProto.MetaData.newBuilder();
                metaDataBuilder.setCurrentTerm(0);
                metaDataBuilder.setVotedFor("nil");
                metaDataBuilder.setCommitIndex(0);
                metaDataBuilder.setFirstLogIndex(0);
                metaDataBuilder.setLastLogIndex(0);
                metaDataBuilder.setLastLogTerm(0);
                metaDataBuilder.setLastIncludedIndex(0);
                metaDataBuilder.setLastIncludedTerm(0);
                metaDataBuilder.setSnapIndex(0);
                metaData = metaDataBuilder.build();
            }
            currTerm = metaData.getCurrentTerm();
            voteFor = metaData.getVotedFor();
            commitIndex = metaData.getCommitIndex();
            firstLogIndex = metaData.getFirstLogIndex();
            lastLogIndex = metaData.getLastLogIndex();
            lastLogTerm = metaData.getLastLogTerm();
            lastIncludedIndex = metaData.getLastIncludedIndex();
            lastIncludedTerm = metaData.getLastIncludedTerm();
            snap_index = metaData.getSnapIndex();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("meta initial error");
        }

        logSegment.init(logDir);
    }

    @Override
    public RaftProto.LogEntry write(RaftProto.LogEntry entry) throws IOException {
        logSegmentLock.writeLock().lock();
        RaftProto.LogEntry logEntry = RaftProto.LogEntry.newBuilder(entry)
                .setIndex(lastLogIndex + 1)
                .build();
        try {
            logSegment.write(logEntry);
            lastLogIndex++;
            updateMetaData(null,null,null,lastLogIndex,logEntry.getTerm(),
                    null,null,null,null);
        }catch (IOException e){
            logSegmentLock.writeLock().unlock();
            e.printStackTrace();
            throw new IOException("write error");
        }finally {
            logSegmentLock.writeLock().unlock();
        }
        return logEntry;
    }

    @Override
    public RaftProto.LogEntry getEntry(long index) throws IOException {
        logSegmentLock.readLock().lock();
        RaftProto.LogEntry ret = null;
        try {
            ret = logSegment.get(index);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            logSegmentLock.readLock().unlock();
        }
        return ret;
    }

    @Override
    public long getTerm(long index) {
        logSegmentLock.readLock().lock();
        long term = -1l;
        try {
            RaftProto.LogEntry logEntry = logSegment.get(index);
            if (logEntry != null) {
                term = logEntry.getTerm();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        logSegmentLock.readLock().unlock();
        return term;
    }

    @Override
    public void truncateSuffix(long startIndex) {
        logSegmentLock.writeLock().lock();
        try {
            if(startIndex < firstLogIndex - 1 || startIndex > lastLogIndex){
                return;
            }
            updateMetaData(null,null,null,
                    startIndex,null,null,
                    null,null,null);
            logSegment.truncateSuffix(startIndex);
        } catch (IOException e) {
            logSegmentLock.writeLock().unlock();
            e.printStackTrace();
            throw new RuntimeException("truncateSuffix error");
        }finally {
            logSegmentLock.writeLock().unlock();
        }
    }

    @Override
    public void truncatePrefix(long startIndex) {
        logSegmentLock.writeLock().lock();
        try {
            if(startIndex < firstLogIndex || startIndex > lastLogIndex + 1){
                return;
            }
            updateMetaData(null,null,startIndex,
                    null,null,null,
                    null,null,null);
            logSegment.truncatePrefix(startIndex);
        } catch (IOException e) {
            logSegmentLock.writeLock().unlock();
            e.printStackTrace();
            throw new RuntimeException("truncateSuffix error");
        }finally {
            logSegmentLock.writeLock().unlock();
        }

    }

    @Override
    public void close() {

    }

    @Override
    public RaftProto.LogEntry getLast() {
        logSegmentLock.readLock().lock();
        RaftProto.LogEntry ret = null;
        try{
            ret = logSegment.get(lastLogIndex);
        } catch (IOException e) {
            logSegmentLock.readLock().unlock();
            e.printStackTrace();
            throw new RuntimeException("get last entry error");
        }finally {
            logSegmentLock.readLock().unlock();
        }
        return ret;
    }

    @Override
    public void updateMetaData(Long currentTerm,
                               String votedFor,
                               Long firstLogIndex,
                               Long lastLogIndex,
                               Long lastLogTerm,
                               Long commitIndex,
                               Long lastIncludedIndex,
                               Long lastIncludedTerm,
                               Integer snap_index) {
        metaDataLock.writeLock().lock();
        try {
            RaftProto.MetaData.Builder builder = RaftProto.MetaData.newBuilder(metaData);
            if (currentTerm != null) {
                this.currTerm = currentTerm;
                builder.setCurrentTerm(currentTerm);
            }
            if (votedFor != null) {
                this.voteFor = votedFor;
                builder.setVotedFor(votedFor);
            }
            if (firstLogIndex != null) {
                this.firstLogIndex = firstLogIndex;
                builder.setFirstLogIndex(firstLogIndex);
            }
            if (lastLogIndex != null) {
                this.lastLogIndex = lastLogIndex;
                builder.setLastLogIndex(lastLogIndex);
            }
            if (lastLogTerm != null) {
                this.lastLogTerm = lastLogTerm;
                builder.setLastLogTerm(lastLogTerm);
            }
            if (commitIndex != null) {
                this.commitIndex = commitIndex;
                builder.setCommitIndex(commitIndex);
            }
            if (lastIncludedIndex != null) {
                this.lastIncludedIndex = lastIncludedIndex;
                builder.setLastIncludedIndex(lastIncludedIndex);
            }
            if (lastIncludedTerm != null) {
                this.lastIncludedTerm = lastIncludedTerm;
                builder.setLastIncludedTerm(lastIncludedTerm);
            }
            if (snap_index != null) {
                this.snap_index = snap_index;
                builder.setSnapIndex(snap_index);
            }
            metaData = builder.build();
            String metaFilePath = logDir + File.separator + "metaData";
            RandomAccessFile metaFile = new RandomAccessFile(metaFilePath, "rws");
            FileUtils.writeProtoToFile(metaFile, metaData);
        } catch (FileNotFoundException e) {
            metaDataLock.writeLock().unlock();
            e.printStackTrace();
            throw new RuntimeException("update meta data error");
        } catch (IOException e) {
            metaDataLock.writeLock().unlock();
            e.printStackTrace();
            throw new RuntimeException("write meta data error");
        } finally {
            metaDataLock.writeLock().unlock();
        }
    }
}
