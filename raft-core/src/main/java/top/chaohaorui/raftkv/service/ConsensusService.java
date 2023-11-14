package top.chaohaorui.raftkv.service;

import top.chaohaorui.raftkv.proto.RaftProto;

public interface ConsensusService {
    public RaftProto.VoteResponse vote(RaftProto.VoteRequest request);
    public RaftProto.AppendEntryResponse appendEntries(RaftProto.AppendEntryRequest request);
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
    public RaftProto.FowardResponse forward(RaftProto.ForwardRequest request);
}
