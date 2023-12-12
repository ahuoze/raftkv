package top.chaohaorui.raftkv;

import top.chaohaorui.raftkv.StateMachine;
import top.chaohaorui.raftkv.proto.RaftProto;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ExampleStateMachine implements StateMachine {
    Map<String,String> state = new HashMap<>();
    @Override
    public byte[] apply(byte[] commandBytes) {
        if(commandBytes == null) return null;
        String command = new String(commandBytes);
        if(command.length() == 0) return null;
        System.out.println("command: " + command);
        String[] s = command.split("\\s+");
        if (s[0].equals("put")) {
            state.put(s[1],s[2]);
            System.out.println("put " + s[1] + " " + s[2]);
        } else if (s[0].equals("get")) {
            String key = s[1];
            if(!state.containsKey(key)) return "Not such key".getBytes();
            return state.get(s[1]).toString().getBytes();
        } else{
            System.out.println("command error");
        }
        return null;
    }

    @Override
    public void readSnapshot(String snapshotPath) {
        String dataFile = snapshotPath + File.separator + "example.data";
        if(!new File(dataFile).exists()){
            try {
                new File(dataFile).getParentFile().mkdirs();
                new File(dataFile).createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
            String line;
            while ((line = randomAccessFile.readLine()) != null) {
                String[] s = line.split("\\s+");
                state.put(s[0],s[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeSnapshot(String snapshotPath) {
        String dataFile = snapshotPath + File.separator + "example.data";
        if(new File(dataFile).exists()){
            new File(dataFile).delete();
        }
        try {
            new File(dataFile).getParentFile().mkdirs();
            new File(dataFile).createNewFile();
            RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "rw");
            randomAccessFile.setLength(0);
            state.forEach((k,v) -> {
                try {
                    randomAccessFile.writeChars(k + " " + v + "\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void apply(RaftProto.LogEntry entry) {
        throw new RuntimeException("not implemented");
    }
}
