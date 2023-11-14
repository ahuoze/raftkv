package top.chaohaorui.raftkv;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
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
    private RpcClient rpcClient;

    public Peer(String id, String ip, int port,boolean isLocal) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.isLocal = isLocal;
        rpcClient = new RpcClient(new Endpoint(ip, port));
        this.consensusService = BrpcProxy.getProxy(rpcClient, ConsensusService.class);
    }

    public void init() {
    }
}
