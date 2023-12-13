package top.chaohaorui.raftkv;

import top.chaohaorui.raftkv.store.Snapshot;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ExampleSnapshot extends Snapshot {
    @Override
    public byte[] getSnapshotBytes(long offset, long intervalSize, String path) {
        try {
            path = path + File.separator + "example.data";
            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
            randomAccessFile.seek(offset);
            int size = Math.min((int) intervalSize, (int) (randomAccessFile.length() - offset));
            byte[] bytes = new byte[size];
            randomAccessFile.read(bytes);
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("get snapshot bytes failed");
    }

    @Override
    public void writeSnapshotBytes(long offset, byte[] data, String path) {
        try {
            path = path + File.separator + "example.data";
            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
            randomAccessFile.seek(offset);
            randomAccessFile.write(data);
            return;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("write snapshot bytes failed");
    }

    @Override
    public long getSnapshotSize() {
        try {
            String datapath = path + File.separator + "example.data";
            RandomAccessFile randomAccessFile = new RandomAccessFile(datapath, "rw");
            return randomAccessFile.length();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("write snapshot bytes failed");
    }
}
