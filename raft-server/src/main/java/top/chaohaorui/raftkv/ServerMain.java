package top.chaohaorui.raftkv;

import com.baidu.brpc.server.RpcServer;
import top.chaohaorui.raftkv.conf.RaftOptions;
import top.chaohaorui.raftkv.service.impl.ServerServiceImpl;

import java.io.File;
import java.util.Properties;

public class ServerMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java -jar raft-server.jar <No>");
            return;
        }
        int No = Integer.parseInt(args[0]);
        Properties properties = new Properties();
        String propertiesPath = No + File.separator + "server.properties";
        properties.load(ServerMain.class.getClassLoader().getResourceAsStream(propertiesPath));
        String port = properties.getProperty("server.port");
        RpcServer rpcServer = new RpcServer(Integer.parseInt(port));
        RaftOptions.RaftOptionsBuilder builder = RaftOptions.builder();
        builder.multiAddress(properties.getProperty("server.multiAddress"));
        builder.localid(properties.getProperty("server.localid"));
        builder.RaftLogDir(properties.getProperty("server.RaftLogDir"));
        builder.SnapshotDir(properties.getProperty("server.SnapshotDir"));
        RaftOptions raftOptions = builder.build();
        ExampleStateMachine exampleStateMachine = new ExampleStateMachine();
        ExampleSnapshot exampleSnapshot = new ExampleSnapshot();
        RaftNode raftNode = new RaftNode(raftOptions, exampleStateMachine, exampleSnapshot);
        raftNode.init();
        ServerService serverService = new ServerServiceImpl(raftNode);
        rpcServer.registerService(serverService);
        rpcServer.start();
    }
}
