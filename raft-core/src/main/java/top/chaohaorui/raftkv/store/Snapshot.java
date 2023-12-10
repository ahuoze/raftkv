package top.chaohaorui.raftkv.store;

public abstract class Snapshot {

    protected String path;

    public Snapshot() {
    }
    public Snapshot(String path) {
        this.path = path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public abstract byte[] getSnapshotBytes(long offset, long intervalSize,String path);

    public abstract void open(String path);

    public abstract void close();

    public abstract void writeSnapshotBytes(long offset,byte[] data,String path);

    public abstract long getSnapshotSize();
}
