package top.chaohaorui.raftkv.store;

public interface Snapshot {
    void writeSnapshot(String snapshotDir);
    void readSnapshot(String snapshotDir);

    byte[] getSnapshotBytes(long offset, long intervalSize);


    void writeSnapshotBytes(long offset,byte[] data,boolean isLast);

    long getSnapshotSize();
}
