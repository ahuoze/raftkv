package top.chaohaorui.raftkv.store;

import top.chaohaorui.raftkv.proto.RaftProto;

public abstract class LogModule {
    private LogSegment logSegment;
    private Snapshot snapshot;
    private long lastLogIndex;
    private long lastLogTerm;
    public abstract void init();
    public abstract void write(RaftProto.LogEntry entry) ;
    public abstract RaftProto.LogEntry read(long index) ;
    public abstract void removeOnStartIndex(long startIndex) ;
    public abstract void close() ;
    public abstract void getLast() ;
    public abstract void getlastLogIndex();
}
