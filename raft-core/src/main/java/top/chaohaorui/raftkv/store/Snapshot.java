package top.chaohaorui.raftkv.store;

public interface Snapshot {
    void writeSnapshot(String snapshotDir);
    void readSnapshot(String snapshotDir);

    byte[] getSnapshotBytes(long offset, long intervalSize);

    long getSnapshotSize();
}
