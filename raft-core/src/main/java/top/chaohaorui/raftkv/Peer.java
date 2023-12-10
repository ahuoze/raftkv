package top.chaohaorui.raftkv;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import lombok.Data;
import org.apache.log4j.Logger;
import top.chaohaorui.raftkv.service.ConsensusService;

import java.util.concurrent.atomic.AtomicBoolean;

public class Peer {
    private String id;
    private String ip;
    private int port;

    public String getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public ConsensusService getConsensusService() {
        if(consensusService == null && !tryConnect())
            throw new RuntimeException("connect to " + id + " failed" + ",so can't get consensusService");
        return consensusService;
    }

    public boolean isVoteGranted() {
        return isVoteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        isVoteGranted = voteGranted;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public boolean isLocal() {
        return isLocal;
    }



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
    }

    public void init() {
       tryConnect();
    }

    public boolean tryConnect(){
        try {
            System.out.println("try connect to " + id);
            rpcClient = new RpcClient(new Endpoint(ip, port));
            this.consensusService = BrpcProxy.getProxy(rpcClient, ConsensusService.class);
        } catch (Error e) {
            return false;
        }  catch (Exception e) {
            return false;
        }
        return true;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }
}
