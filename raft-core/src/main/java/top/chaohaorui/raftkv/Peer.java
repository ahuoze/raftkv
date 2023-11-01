package top.chaohaorui.raftkv;

import lombok.Data;
import top.chaohaorui.raftkv.service.ConsensusService;

@Data
public class Peer {
    private String id;
    private String ip;
    private int port;
    private ConsensusService consensusService;
    private boolean isLeader;
    private boolean isLocal;

    public Peer(String id, String ip, int port,boolean isLocal) {
    }

    public void init() {
    }
}
