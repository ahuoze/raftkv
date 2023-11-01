package top.chaohaorui.raftkv;

public interface StateMachine {
    public void apply(String key,String value);
    public void get(String key);
    public void readSnapshot(String snapshotPath);
    public void writeSnapshot(String snapshotPath);
}
