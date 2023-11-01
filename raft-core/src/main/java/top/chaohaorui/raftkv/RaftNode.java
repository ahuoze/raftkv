package top.chaohaorui.raftkv;

import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.store.LogModule;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {
    private Peer peer;
    private long currentTerm;
    // 引入zab的思想，不需要数据同步时旧事务和空op的原子性操作
    private long accpectTerm;
    private String votedFor;
    private RaftProto.RaftState status;
    private LogModule logModule;
    private Map<String,Peer> peerMap;
    private ReentrantLock voteLock;
    private ReentrantLock appendLock;
    private ReentrantLock commitLock;

    private StateMachine stateMachine;

    private long[] nextIndex;
    private long[] matchIndex;
    private long commitIndex;

    public RaftNode(String multiAddress,String localid,StateMachine stateMachine){
        String[] address = multiAddress.split(",");
        for (String s : address) {
            String[] split = s.split(":");
            String id = split[0];
            String ip = split[1];
            String port = split[2];
            Peer peer = new Peer(id,ip,Integer.parseInt(port),id.equals(localid));
            peerMap.put(id,peer);
        }
    }

    public void init(){
        peer.init();
        logModule.init();
        peerMap.forEach((k,v)->{
            if (!v.isLocal()){
                v.init();
            }
        });

    }

    public boolean replicate(Byte[] command,RaftProto.EntryType entryType){
        return false;
    }

    public boolean replicate(RaftProto.Configuration configuration, RaftProto.EntryType entryType){
        return false;
    }

    public byte[] read(Byte[] command){
        return null;
    }

    public void appendEntry(RaftProto.LogEntry entry){

    }

    public void applyConfigration(RaftProto.Configuration configuration){

    }

    public void requestVote(Peer peer){

    }


}
