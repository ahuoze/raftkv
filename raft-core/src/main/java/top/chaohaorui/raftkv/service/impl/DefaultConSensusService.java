package top.chaohaorui.raftkv.service.impl;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import top.chaohaorui.raftkv.RaftNode;
import top.chaohaorui.raftkv.conf.RaftOptions;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.service.ConsensusService;
import top.chaohaorui.raftkv.store.LogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.chaohaorui.raftkv.store.Snapshot;
import top.chaohaorui.raftkv.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;


public class DefaultConSensusService implements ConsensusService {
    public RaftNode raftNode;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultConSensusService.class);
    private ThreadPoolExecutor threadPoolExecutor;
    public DefaultConSensusService(RaftNode raftNode){
        this.raftNode = raftNode;
        this.threadPoolExecutor = raftNode.getThreadPoolExecutor();
    }

    @Override
    public RaftProto.VoteResponse vote(RaftProto.VoteRequest request) {
        RaftProto.VoteResponse.Builder builder = RaftProto.VoteResponse.newBuilder();
        LOG.info("now peer {} recieve {} vote",raftNode.getLocalPeer().getId(),request.getCandidateId());
        if(!raftNode.getElectionLock().tryLock()){
            LOG.info("now peer {} is electing host , reject {} vote",raftNode.getLocalPeer().getId(),request.getCandidateId());
            builder.setTerm(raftNode.getCurrTerm());
            builder.setVoteGranted(false);
            return builder.build();
        }

        builder.setTerm(raftNode.getCurrTerm());
        builder.setVoteGranted(false);
       try{
           if(request.getTerm() < raftNode.getCurrTerm()){
               LOG.info("reject vote request {} from {} for term {} < {}",raftNode.getLocalPeer().getId(),request.getCandidateId(),request.getTerm(),raftNode.getCurrTerm());
           }else{
               if(request.getTerm() > raftNode.getCurrTerm()) raftNode.stepdown(request.getTerm());
               LogModule logModule = raftNode.getLogModule();
               long lastLogIndex = logModule.getLastLogIndex();
               long lastLogTerm = logModule.getLastLogTerm();
               if(request.getLastLogTerm() < lastLogTerm ||
                       (request.getLastLogTerm() == lastLogTerm
                               && request.getLastLogIndex() < lastLogIndex)) {
                   LOG.info("reject vote request {} from {} for {} {} but {} {}",
                           raftNode.getLocalPeer().getId(),request.getCandidateId(),
                           request.getLastLogTerm(),request.getLastLogIndex(),
                           lastLogTerm,lastLogIndex);
               }else if(raftNode.getVotedFor() == null || raftNode.getVotedFor() == "nil" || raftNode.getVotedFor().equals(request.getCandidateId())){
                   logModule.updateMetaData(null,request.getCandidateId(),null,
                           null,null,null,
                           null,null,null);
                   builder.setVoteGranted(true);
                   raftNode.resetElectionTimer();
                   LOG.info("now peer {} vote {} for leader",raftNode.getLocalPeer().getId(),request.getCandidateId());
               }
           }
       }catch (Exception e){
           LOG.error("vote error: {}", e.getMessage());
           e.printStackTrace();
       }finally {
              raftNode.getElectionLock().unlock();
       }
        return builder.build();
    }

    @Override
    public RaftProto.AppendEntryResponse appendEntries(RaftProto.AppendEntryRequest request){
        RaftProto.AppendEntryResponse.Builder builder = RaftProto.AppendEntryResponse.newBuilder();
//        LOG.info("{} recieve appendEntries request from {}",raftNode.getLocalPeer().getId(),request.getLeaderId());
        builder.setTerm(raftNode.getCurrTerm());
        builder.setSuccess(false);
        builder.setLastLogIndex(raftNode.getLogModule().getLastLogIndex());
        if(request.getTerm() < raftNode.getCurrTerm()) {
            LOG.info("{} reject appendEntries request from {}, request term is {}, current term is {} , request leaderId is {} , current leaderId is {}",
                    raftNode.getLocalPeer().getId(), request.getLeaderId(), request.getTerm(), raftNode.getCurrTerm(), request.getLeaderId(), raftNode.getLeaderId());
            return builder.build();
        }
        if(request.getTerm() > raftNode.getCurrTerm()) raftNode.stepdown(request.getTerm());
        if(raftNode.getLeaderId() == null){
            raftNode.setLeaderId(request.getLeaderId());
        }
        LogModule logModule = raftNode.getLogModule();

        if(request.getPrevLogIndex() > logModule.getLastLogIndex()) return builder.build();
        try {
            if(request.getPrevLogIndex() >= logModule.getFirstLogIndex()
                    && request.getPrevLogIndex() != 0
                    && logModule.getEntry(request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm()){
                logModule.truncateSuffix(request.getPrevLogIndex() - 1);
                builder.setTerm(raftNode.getCurrTerm());
                builder.setLastLogIndex(raftNode.getLogModule().getLastLogIndex());
                LOG.info("log unmatch, truncate log from index {}", request.getPrevLogIndex());
                return builder.build();
            }
        } catch (IOException e) {
            LOG.error("appendEntries error: {}", e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("appendEntries error: " + e.getMessage());
        }
        if(request.getEntriesCount() == 0){
            builder.setSuccess(true);
            raftNode.resetElectionTimer();
            builder.setTerm(raftNode.getCurrTerm());
            builder.setLastLogIndex(logModule.getLastLogIndex());
            applyLogToMachine(request);
            return builder.build();
        }
        LOG.info("{} recieve appendEntries request from {}",raftNode.getLocalPeer().getId(),request.getLeaderId());
        for (RaftProto.LogEntry logEntry : request.getEntriesList()) {
            if(logEntry.getIndex() < logModule.getFirstLogIndex()) continue;
            try {
                if(logEntry.getIndex() <= logModule.getLastLogIndex() && logModule.getEntry(logEntry.getIndex()).getTerm() != logEntry.getTerm()){
                    logModule.truncateSuffix(logEntry.getIndex() - 1);
                }
                if(logEntry.getIndex() > logModule.getLastLogIndex()){
                    raftNode.getLogModule().write(logEntry);
                }
            } catch (IOException e) {
                LOG.error("appendEntries error: {}", e.getMessage());
                e.printStackTrace();
                return builder.build();
            }
        }
        raftNode.setCommitIndex(request.getCommitIndex());
        // 解决日志加锁问题实现日志写入定序
        applyLogToMachine(request);
        builder.setSuccess(true);
        raftNode.resetElectionTimer();
        builder.setTerm(raftNode.getCurrTerm());
        builder.setLastLogIndex(logModule.getLastLogIndex());
        return builder.build();
    }

    private void applyLogToMachine(RaftProto.AppendEntryRequest request) {
        threadPoolExecutor.submit(() ->{
            raftNode.getStateMachineLock().lock();
            try{
                long newCommitIndex = request.getCommitIndex();
                long oldCommitIndex = raftNode.getAppliedIndex();
                for(long i = oldCommitIndex + 1;i <= newCommitIndex;i++){
                    RaftProto.LogEntry logEntry = raftNode.getLogModule().getEntry(i);
                    if(logEntry == null) {
                        LOG.warn("unexpected null entry, index {}", i);
                        continue;
                    }
                    raftNode.getStateMachine().apply(logEntry.getData().toByteArray());
                }
                raftNode.setAppliedIndex(newCommitIndex);
            }catch (Exception e){
                LOG.error("apply log to machine error: {}", e.getMessage());
                e.printStackTrace();
            }finally {
                raftNode.getStateMachineLock().unlock();
            }
        });
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
        raftNode.resetElectionTimer();
        if(request.getLastIncludedIndex() == raftNode.getLogModule().getLastIncludedIndex() &&
            request.getLastIncludedTerm() == raftNode.getLogModule().getLastLogTerm()){
            builder.setTerm(raftNode.getCurrTerm());
            builder.setLastIncludedIndex(raftNode.getLogModule().getLastLogIndex());
            builder.setLastIncludedTerm(raftNode.getLogModule().getLastIncludedTerm());
            return builder.build();
        }
        LogModule logModule = raftNode.getLogModule();
        Snapshot snapshot = logModule.getSnapshot();
        ReadWriteLock snapshotLock = logModule.getSnapshotLock();
        String path = logModule.getSnapshotDir();
        try{
            snapshotLock.writeLock().lock();
            raftNode.isTakeSnapshot = true;
            int nextIndex = logModule.snap_index ^ 1;
            String newPath = path + File.separator + nextIndex;
            File newDir = new File(newPath);
            if(request.getOffset() == 0) FileUtils.deleteDirectory(newDir);
            if(!newDir.exists()) newDir.mkdirs();
            snapshot.writeSnapshotBytes(request.getOffset(),request.getData().toByteArray(),newPath);
            if(request.getDone()){
                logModule.updateMetaData(null,null,null,
                        null,null,null,
                        request.getLastIncludedIndex(),request.getLastIncludedTerm(),nextIndex);
                raftNode.getStateMachine().readSnapshot(newPath);
                raftNode.isTakeSnapshot = false;
            }
        }catch (Exception e){
            LOG.error("installSnapshot error: {}", e.getMessage());
            e.printStackTrace();
        }finally {
            snapshotLock.writeLock().unlock();
        }
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
        LOG.info("receive forward request");
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

