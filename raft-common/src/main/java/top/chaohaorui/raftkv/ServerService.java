package top.chaohaorui.raftkv;

import top.chaohaorui.raftkv.proto.SimpleProto;

public interface ServerService {
    public SimpleProto.MsgResponse put(SimpleProto.MsgRequest request);
    public SimpleProto.MsgResponse get(SimpleProto.MsgRequest request);
}
