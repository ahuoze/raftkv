package top.chaohaorui.raftkv;

import top.chaohaorui.raftkv.proto.RaftProto;

public interface StateMachine {
    public byte[] apply(byte[] commandBytes);
    public void readSnapshot(String snapshotPath);
    public void writeSnapshot(String snapshotPath);
    void apply(RaftProto.LogEntry entry);
}
