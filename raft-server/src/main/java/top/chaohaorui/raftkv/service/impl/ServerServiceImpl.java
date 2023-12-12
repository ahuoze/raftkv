package top.chaohaorui.raftkv.service.impl;

import org.apache.log4j.Logger;
import top.chaohaorui.raftkv.RaftNode;
import top.chaohaorui.raftkv.ServerService;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.proto.SimpleProto;

public class ServerServiceImpl implements ServerService {
    private static final Logger logger = Logger.getLogger(ServerServiceImpl.class);

    private RaftNode raftNode;

    public ServerServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public SimpleProto.MsgResponse put(SimpleProto.MsgRequest request) {
        SimpleProto.MsgResponse.Builder builder = SimpleProto.MsgResponse.newBuilder();
        builder.setSuccess(false);
        if(request.getType() != SimpleProto.MsgType.Write) {
            logger.error("put request type error");
            return builder.build();
        }
        String key = request.getKey();
        String value = request.getValue();
        String command = "put " + key + " " + value;
        boolean success = raftNode.replicate(command.getBytes(), RaftProto.EntryType.DADA);
        return success ? builder.setSuccess(true).build() : builder.build();
    }

    @Override
    public SimpleProto.MsgResponse get(SimpleProto.MsgRequest request) {
        SimpleProto.MsgResponse.Builder builder = SimpleProto.MsgResponse.newBuilder();
        builder.setSuccess(false);
        if(request.getType() != SimpleProto.MsgType.Read) {
            logger.error("get request type error");
            return builder.build();
        }
        String key = request.getKey();
        String command = "get " + key;
        byte[] read = raftNode.read(command.getBytes());
        String value = new String(read);
        return builder.setSuccess(true).setValue(value).build();
    }
}
