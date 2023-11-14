package top.chaohaorui.raftkv.store;

import top.chaohaorui.raftkv.proto.RaftProto;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public abstract class LogModule {
    private LogSegment logSegment;
    private Snapshot snapshot;
    private long firstLogIndex;
    private long lastLogIndex;
    private long lastLogTerm;
    private long lastIncludedIndex;
    private long lastIncludedTerm;

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public void setLastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(long lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
    }

    public LogSegment getLogSegment() {
        return logSegment;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    private ReentrantLock snapshotLock;
    private ReentrantLock logSegmentLock;
    private ReentrantLock metaDataLock;
    public abstract void init();
    public abstract void write(RaftProto.LogEntry entry) throws IOException;
    public abstract RaftProto.LogEntry getEntry(long index) ;
    public abstract long getTerm(long index);
    public abstract void truncateSuffix(long startIndex) ;

    public long getFirstLogIndex() {
        return firstLogIndex;
    }

    public void setFirstLogIndex(long firstLogIndex) {
        this.firstLogIndex = firstLogIndex;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public abstract void truncatePrefix(long startIndex) ;
    public abstract void close() ;
    public abstract RaftProto.LogEntry getLast() ;
    public abstract void updateMetaData(Long currentTerm, String votedFor, Long firstLogIndex, Long commitIndex);

    public ReentrantLock getSnapshotLock() {
        return snapshotLock;
    }

    public ReentrantLock getLogSegmentLock() {
        return logSegmentLock;
    }

    public ReentrantLock getMetaDataLock() {
        return metaDataLock;
    }
}
