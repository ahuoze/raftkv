package top.chaohaorui.raftkv.service.impl;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import top.chaohaorui.raftkv.RaftNode;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.service.ConsensusService;
import top.chaohaorui.raftkv.store.LogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.chaohaorui.raftkv.store.Snapshot;

import java.io.IOException;


public class DefaultConSensusService implements ConsensusService {
    public RaftNode raftNode;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultConSensusService.class);

    public DefaultConSensusService(RaftNode raftNode){
        this.raftNode = raftNode;
    }

    @Override
    public RaftProto.VoteResponse vote(RaftProto.VoteRequest request) {
        RaftProto.VoteResponse.Builder builder = RaftProto.VoteResponse.newBuilder();
        builder.setTerm(raftNode.getCurrTerm());
        builder.setVoteGranted(false);
        if(request.getTerm() < raftNode.getCurrTerm()) return builder.build();
        if(request.getTerm() > raftNode.getCurrTerm()) raftNode.stepdown(request.getTerm());
        if(raftNode.getVotedFor() == null || raftNode.getVotedFor() == "nil" || raftNode.getVotedFor().equals(request.getCandidateId())){
            raftNode.setVoteFor(request.getCandidateId());
            builder.setVoteGranted(true);
        }
        return builder.build();
    }

    @Override
    public RaftProto.AppendEntryResponse appendEntries(RaftProto.AppendEntryRequest request) {
        RaftProto.AppendEntryResponse.Builder builder = RaftProto.AppendEntryResponse.newBuilder();
        builder.setTerm(raftNode.getCurrTerm());
        builder.setSuccess(false);
        builder.setLastLogIndex(raftNode.getLogModule().getLastLogIndex());
        if(request.getTerm() < raftNode.getCurrTerm()) return builder.build();
        if(request.getTerm() > raftNode.getCurrTerm()) raftNode.stepdown(request.getTerm());
        if(raftNode.getLeaderId() == null){
            raftNode.setLeaderId(request.getLeaderId());
        }
        LogModule logModule = raftNode.getLogModule();
        if(request.getPrevLogIndex() > logModule.getLastLogIndex()) return builder.build();
        if(request.getPrevLogIndex() >= logModule.getFirstLogIndex() && logModule.getEntry(request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm()){
            logModule.truncateSuffix(request.getPrevLogIndex() - 1);
            builder.setTerm(raftNode.getCurrTerm());
            builder.setLastLogIndex(raftNode.getLogModule().getLastLogIndex());
            LOG.info("log unmatch, truncate log from index {}", request.getPrevLogIndex());
            return builder.build();
        }
        if(request.getEntriesCount() == 0){
            builder.setSuccess(true);
            builder.setTerm(raftNode.getCurrTerm());
            builder.setLastLogIndex(logModule.getLastLogIndex());
            applyLogToMachine(request);
            return builder.build();
        }

        for (RaftProto.LogEntry logEntry : request.getEntriesList()) {
            if(logEntry.getIndex() < logModule.getFirstLogIndex()) continue;
            if(logEntry.getIndex() <= logModule.getLastLogIndex() && logModule.getEntry(logEntry.getIndex()).getTerm() != logEntry.getTerm()){
                logModule.truncateSuffix(logEntry.getIndex() - 1);
            }
            try {
                raftNode.getLogModule().write(logEntry);
            } catch (IOException e) {
                LOG.error("appendEntries error: {}", e.getMessage());
                e.printStackTrace();
            }
        }
        applyLogToMachine(request);
        builder.setSuccess(true);
        builder.setTerm(raftNode.getCurrTerm());
        builder.setLastLogIndex(logModule.getLastLogIndex());
        return builder.build();
    }

    private void applyLogToMachine(RaftProto.AppendEntryRequest request) {
        long newCommitIndex = request.getCommitIndex();
        long oldCommitIndex = raftNode.getCommitIndex();
        for(long i = oldCommitIndex + 1;i <= newCommitIndex;i++){
            RaftProto.LogEntry logEntry = raftNode.getLogModule().getEntry(i);
            if(logEntry == null) {
                LOG.warn("unexpected null entry, index {}", i);
                continue;
            }
            raftNode.getStateMachine().apply(logEntry.getData().toByteArray());
        }
        raftNode.setCommitIndex(newCommitIndex);
        raftNode.getLogModule().updateMetaData(null,null,null,newCommitIndex);
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder builder = RaftProto.InstallSnapshotResponse.newBuilder();
        builder.setTerm(raftNode.getCurrTerm());
        builder.setLastIncludedIndex(raftNode.getLogModule().getLastLogIndex());
        builder.setLastIncludedTerm(raftNode.getLogModule().getLastIncludedTerm());
        if(request.getTerm() < raftNode.getCurrTerm()) return builder.build();
        if(request.getTerm() > raftNode.getCurrTerm()) raftNode.stepdown(request.getTerm());
        if(raftNode.getLeaderId() == null) raftNode.setLeaderId(request.getLeaderId());
        if(request.getLastIncludedIndex() == raftNode.getLogModule().getLastIncludedIndex() &&
            request.getLastIncludedTerm() == raftNode.getLogModule().getLastLogTerm()){
            builder.setTerm(raftNode.getCurrTerm());
            builder.setLastIncludedIndex(raftNode.getLogModule().getLastLogIndex());
            builder.setLastIncludedTerm(raftNode.getLogModule().getLastIncludedTerm());
            return builder.build();
        }
        LogModule logModule = raftNode.getLogModule();
        Snapshot snapshot = logModule.getSnapshot();
        snapshot.writeSnapshotBytes(request.getOffset(),request.getData().toByteArray(),request.getDone());
        builder.setTerm(raftNode.getCurrTerm());
        builder.setLastIncludedIndex(raftNode.getLogModule().getLastLogIndex());
        builder.setLastIncludedTerm(raftNode.getLogModule().getLastIncludedTerm());
        return builder.build();
    }

    @Override
    public RaftProto.FowardResponse forward(RaftProto.ForwardRequest request) {
        RaftProto.FowardResponse.Builder builder = RaftProto.FowardResponse.newBuilder();
        if(raftNode.getStatus() != RaftNode.NodeState.LEADER){
            builder.setSuccess(false);
            LOG.error("forward peer is not leader");
        }
        switch (request.getType()){
            case WRITE -> {
                byte[] command = request.getData().toByteArray();
                boolean success = raftNode.replicate(command, RaftProto.EntryType.DADA);
                builder.setSuccess(success);
                return builder.build();
            }
            case READ -> {
                byte[] command = request.getData().toByteArray();
                byte[] result = raftNode.read(command);
                if(result != null) {
                    builder.setSuccess(true);
                    builder.setData(ByteString.copyFrom(result));
                }
                return builder.build();
            }
        }
        return builder.build();
    }
}

