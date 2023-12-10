package top.chaohaorui.raftkv.store;

import top.chaohaorui.raftkv.proto.RaftProto;

import java.io.IOException;

public interface LogSegment {
    void write(RaftProto.LogEntry entry) throws IOException;

    RaftProto.LogEntry get(long index) throws IOException;

    void truncateSuffix(long startIndex) throws IOException;

    void truncatePrefix(long startIndex) throws IOException;

    void init(String logDir);
}
