package top.chaohaorui.raftkv;

import lombok.Data;
import top.chaohaorui.raftkv.service.ConsensusService;

import java.util.concurrent.atomic.AtomicBoolean;

@Data
public class Peer {
    private String id;
    private String ip;
    private int port;
    private ConsensusService consensusService;
    private boolean isLeader;
    private boolean isLocal;
    private boolean isVoteGranted;
    public AtomicBoolean isInstallingSnapshot = new AtomicBoolean(false);

    public Peer(String id, String ip, int port,boolean isLocal) {
    }

    public void init() {
    }
}
