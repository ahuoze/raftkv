package top.chaohaorui.raftkv.store;

import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.store.impl.DefaultLogSegment;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class LogModule {
    protected LogSegment logSegment;
    protected Snapshot snapshot;
    protected volatile long commitIndex;

    protected volatile long currTerm;
    protected volatile String voteFor;
    protected volatile long firstLogIndex;
    protected volatile long lastLogIndex;
    protected volatile long lastLogTerm;
    protected volatile long lastIncludedIndex;
    protected volatile long lastIncludedTerm;
    protected ReadWriteLock snapshotLock;
    protected ReadWriteLock logSegmentLock;
    protected ReadWriteLock metaDataLock;
    public volatile int snap_index;
    protected RaftProto.MetaData metaData;

    protected String logDir;
    protected String snapshotDir;

    public LogModule(LogSegment logSegment, Snapshot snapshot,String logDir) {
        this.logSegment = logSegment;
        this.snapshot = snapshot;
        this.snapshotLock = new ReentrantReadWriteLock();
        this.logSegmentLock = new ReentrantReadWriteLock ();
        this.metaDataLock = new ReentrantReadWriteLock ();
        this.logDir = logDir;
    }


    public LogModule(Snapshot snapshot,String logDir) {
        this.logSegment = new DefaultLogSegment();
        this.snapshot = snapshot;
        this.snapshotLock = new ReentrantReadWriteLock();
        this.logSegmentLock = new ReentrantReadWriteLock();
        this.metaDataLock = new ReentrantReadWriteLock();
        this.logDir = logDir;
    }

    public LogSegment getLogSegment() {
        return logSegment;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public abstract void init() throws RuntimeException;
    public abstract RaftProto.LogEntry write(RaftProto.LogEntry entry) throws IOException;
    public abstract RaftProto.LogEntry getEntry(long index) throws IOException;
    public abstract long getTerm(long index);
    public abstract void truncateSuffix(long startIndex) ;
    public abstract void truncatePrefix(long startIndex) ;
    public abstract void close() ;
    public abstract RaftProto.LogEntry getLast() ;
    public abstract void updateMetaData(Long currentTerm,
                                        String votedFor,
                                        Long firstLogIndex,
                                        Long lastLogIndex,
                                        Long lastLogTerm,
                                        Long commitIndex,
                                        Long lastIncludedIndex,
                                        Long lastIncludedTerm,
                                        Integer snap_index
                                        ) ;

    public int getSnap_index() {
        return snap_index;
    }

    public void setSnap_index(int snap_index) {
        this.snap_index = snap_index;
    }

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

    public ReadWriteLock getSnapshotLock() {
        return snapshotLock;
    }

    public ReadWriteLock getLogSegmentLock() {
        return logSegmentLock;
    }

    public ReadWriteLock getMetaDataLock() {
        return metaDataLock;
    }


    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

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



    public long getCurrTerm() {
        return currTerm;
    }

    public void setCurrTerm(long currTerm) {
        this.currTerm = currTerm;
    }

    public String getVoteFor() {
        return voteFor;
    }

    public void setVoteFor(String voteFor) {
        this.voteFor = voteFor;
    }

    public String getVotedFor() {
        return voteFor;
    }



    public String getLogDir() {
        return logDir;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public void setSnapshotDir(String snapshotDir) {
        this.snapshotDir = snapshotDir;
    }

}
