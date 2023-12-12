package top.chaohaorui.raftkv;

import com.baidu.brpc.client.RpcClient;
import top.chaohaorui.raftkv.proto.SimpleProto;

import java.util.Scanner;

public class ClientMain {
    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Usage: java -jar raft-client.jar <MultiAddress>");
            return;
        }
        String multiAddress = args[0];
        RpcClient rpcClient = new RpcClient("list://"+multiAddress);
        ServerService serverService = (ServerService) rpcClient.getProxy(ServerService.class);
        Scanner scanner = new Scanner(System.in);
        String line = null;
        while (!(line = scanner.nextLine()).equals("quit")){
            String[] split = line.split(" ");
            if(split[0].equals("put")){
                SimpleProto.MsgRequest request = SimpleProto.MsgRequest.newBuilder().setKey(split[1]).setValue(split[2])
                        .setType(SimpleProto.MsgType.Write).build();
                SimpleProto.MsgResponse response = serverService.put(request);
                System.out.println(response.getSuccess());
            }else if(split[0].equals("get")){
                SimpleProto.MsgRequest request = SimpleProto.MsgRequest.newBuilder().setKey(split[1])
                        .setType(SimpleProto.MsgType.Read).build();
                SimpleProto.MsgResponse response = serverService.get(request);
                System.out.println(response.getValue());
            }else {
                System.out.println("Usage: put <key> <value> or get <key>");
            }
        }
        System.exit(0);
    }
}
