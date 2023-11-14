package top.chaohaorui.raftkv;

import com.google.protobuf.ByteString;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.store.LogModule;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {
    private Peer peer;
    private long currTerm;
    private String votedFor;
    private NodeState status;
    private LogModule logModule;
    private Map<String,Peer> peerMap;
    private Properties RaftProperties;
    private StateMachine stateMachine;
    private ScheduledExecutorService scheduledExecutorService;
    private CompletionService<Boolean> completionService;
    private ThreadPoolExecutor threadPoolExecutor;
    private ScheduledFuture<Boolean> electionFuture;
    private ScheduledFuture<Boolean> heartbeatFuture;
    private ReentrantLock stateMachineLock;
    private ReentrantLock parkLock;
    private Queue<Pair<Long,Condition>> parkQueue;

    private Map<String,Long> nextIndex;
    private Map<String,Long> matchIndex;
    private long commitIndex;
    private static Logger logger = Logger.getLogger(RaftNode.class);
    private String leaderId;

    public enum NodeState{
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private class Pair<K,V>{
        private K key;
        private V value;

        public Pair(K key,V value){
            this.key = key;
            this.value = value;
        }

        public K getKey(){
            return key;
        }

        public V getValue(){
            return value;
        }
    }
    public RaftNode(String multiAddress,String localid,StateMachine stateMachine){
        String[] address = multiAddress.split(",");
        for (String s : address) {
            String[] split = s.split(":");
            String id = split[0];
            String ip = split[1];
            String port = split[2];
            Peer peer = new Peer(id,ip,Integer.parseInt(port),id.equals(localid));
            peerMap.put(id,peer);
        }
        RaftProperties = new Properties();
        nextIndex = new ConcurrentHashMap<>();
        matchIndex = new ConcurrentHashMap<>();
        peerMap = new ConcurrentHashMap<>();
        parkQueue = new ConcurrentLinkedDeque<>();
        stateMachineLock = new ReentrantLock();
        parkLock = new ReentrantLock();
        try {
            RaftProperties.load(RaftNode.class.getClassLoader().getResourceAsStream("raft.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void init(){
        peer.init();
        logModule.init();
        peerMap.forEach((k,v)->{
            if (!v.isLocal()){
                v.init();
            }
        });
        status = NodeState.CANDIDATE;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
        resetElectionTimer();
        threadPoolExecutor = new ThreadPoolExecutor(
                Integer.parseInt(RaftProperties.getProperty("threadpool.corepoolsize")),
                Integer.parseInt(RaftProperties.getProperty("threadpool.maxpoolsize")),
                Long.parseLong(RaftProperties.getProperty("threadpool.keepalivetime")),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(Integer.parseInt(RaftProperties.getProperty("threadpool.queuecapacity"))),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        completionService = new ExecutorCompletionService<>(threadPoolExecutor);
    }

    private void resetElectionTimer() {
        if(electionFuture != null && !electionFuture.isDone()){
            electionFuture.cancel(true);
        }
        electionFuture = scheduledExecutorService.schedule(new Callable<Boolean>() {
                                                       @Override
                                                       public Boolean call() throws Exception {
                                                           return election();
                                                       }
                                                   }, (long) (Long.parseLong(RaftProperties.getProperty("election.timeout.basetime"), 10) *
                Math.random() *
                Math.min(Integer.parseInt(RaftProperties.getProperty("election.timeout.ratio"),10),3))
        , TimeUnit.MILLISECONDS);
    }

    private Boolean election() {
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        requestBuilder.setTerm(++currTerm);
        requestBuilder.setCandidateId(peer.getId());
        requestBuilder.setLastLogIndex(logModule.getLastLogIndex());
        requestBuilder.setLastLogTerm(logModule.getLastLogTerm());
        RaftProto.VoteRequest request = requestBuilder.build();
        List<Future> futureList = new ArrayList<>();
        for (Peer peer : peerMap.values()) {
            futureList.add(
                    completionService.submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            RaftProto.VoteResponse voteResponse = peer.getConsensusService().vote(request);
                            if (voteResponse == null) {
                                logger.warn("vote response is null");
                                return false;
                            }
                            if (voteResponse.getTerm() > currTerm) {
                                stepdown(voteResponse.getTerm());
                                return false;
                            }
                            if (voteResponse.getVoteGranted()) {
                                return true;
                            }
                            return false;
                        }
                    })
            );
        }
        int count = (peerMap.size() + 1) / 2;
        while (count > 0){
            try {
                Future<Boolean> future = completionService.take();
                if(future.get()){
                    count--;
                }
                if(status != NodeState.CANDIDATE){
                    for (Future future1 : futureList) {
                        if(future1 != null && !future1.isDone()){
                            future1.cancel(true);
                        }
                    }
                    return false;
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        status = NodeState.LEADER;
        logger.info("node " + peer.getId() + "become leader");
        replicate((byte[]) null,RaftProto.EntryType.DADA);
        resetHeartbeat();
        return true;
    }

    private void resetHeartbeat() {
        if(heartbeatFuture != null){
            heartbeatFuture.cancel(true);
        }
        heartbeatFuture = scheduledExecutorService.schedule(
                new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        resetHeartbeat();
                        threadPoolExecutor.submit(new Runnable() {
                            @Override
                            public void run() {
                                logCopyHandler(peer);
                            }
                        });
                        return true;
                    }
                }
                , Long.parseLong(RaftProperties.getProperty("heartbeat.interval"))
                , TimeUnit.MILLISECONDS
        );
    }

    private void stepdown(long term) {
        if(currTerm > term){
            logger.warn("can not stepdown because term is smaller than current term");
            return;
        }
        logModule.getMetaDataLock().lock();
        try {
            if(currTerm < term){
                currTerm = term;
            }
            status = NodeState.FOLLOWER;
            votedFor = "nil";
            leaderId = null;
            logModule.updateMetaData(term, votedFor,null,null);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            logModule.getMetaDataLock().unlock();
        }
    }

    public boolean replicate(byte[] command,RaftProto.EntryType entryType){
        if(status != NodeState.LEADER){
            logger.info("node " + peer.getId() + " is not leader");
            if (leaderId != null){
                RaftProto.ForwardRequest.Builder builder = RaftProto.ForwardRequest.newBuilder();
                builder.setType(RaftProto.FowardType.WRITE);
                builder.setData(ByteString.copyFrom(command));
                return peerMap.get(leaderId).getConsensusService().forward(builder.build()).getSuccess();
            }
            return false;
        }
        RaftProto.LogEntry.Builder entryBuilder = RaftProto.LogEntry.newBuilder();
        RaftProto.LogEntry logEntry = entryBuilder.setTerm(currTerm)
                .setType(entryType)
                .setData(ByteString.copyFrom(command)).build();
        Condition currThreadCondition = parkLock.newCondition();
        try {
            parkLock.lock();
            logModule.write(logEntry);
            parkQueue.add(new Pair<>(logEntry.getIndex(),currThreadCondition));
            for (Peer peer : peerMap.values()) {
                threadPoolExecutor.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return logCopyHandler(peer);
                    }
                });
            }
            long start = System.currentTimeMillis();
            long now = start;
            while (commitIndex < logEntry.getIndex()){
                if(now - start > Long.parseLong(RaftProperties.getProperty("raft.replicate.maxwaitTime"))){
                    break;
                }
                now = System.currentTimeMillis();
                try {
                    currThreadCondition.await(Long.parseLong(RaftProperties.getProperty("raft.replicate.maxwaitTime")),TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            parkLock.unlock();
        }
        return commitIndex >= logEntry.getIndex();
    }

    private boolean logCopyHandler(Peer peer) {
        Long nextLogIndex = nextIndex.getOrDefault(peer.getId(), logModule.getLastLogIndex());
        do {long prevLogIndex = nextLogIndex - 1;
            long prevLogTerm = logModule.getTerm(prevLogIndex);
            RaftProto.AppendEntryRequest.Builder requestBuilder = RaftProto.AppendEntryRequest.newBuilder();
            requestBuilder.setTerm(currTerm);
            requestBuilder.setLeaderId(peer.getId());
            requestBuilder.setPrevLogIndex(prevLogIndex);
            requestBuilder.setPrevLogTerm(prevLogTerm);
            requestBuilder.setCommitIndex(commitIndex);
            for(long i = nextLogIndex; i <= logModule.getLastLogIndex(); i++){
                RaftProto.LogEntry logEntry = logModule.getEntry(i);
                requestBuilder.addEntries(logEntry);
            }
            RaftProto.AppendEntryResponse appendEntryResponse = peer.getConsensusService().appendEntries(requestBuilder.build());
            if(appendEntryResponse == null){
                logger.warn("append entry response is null");
                return false;
            }
            if(appendEntryResponse.getTerm() > currTerm){
                stepdown(appendEntryResponse.getTerm());
                return false;
            }
            if(appendEntryResponse.getSuccess()){
                nextIndex.put(peer.getId(),logModule.getLastLogIndex() + 1);
                matchIndex.put(peer.getId(),logModule.getLastLogIndex());
                advanceCommitIndex();
                return true;
            }
            nextLogIndex = appendEntryResponse.getLastLogIndex() + 1;
        }while (nextLogIndex >= logModule.getFirstLogIndex());
        return tryInstallSnapshot(peer);
    }

    private void advanceCommitIndex() {
        try {
            List<Long> macths = new ArrayList<>(matchIndex.values());
            Collections.sort(macths);
            long newCommitIndex = macths.get(macths.size() / 2);
            long oldCommitIndex = commitIndex;
            stateMachineLock.lock();
            for (long i = oldCommitIndex + 1;i <= newCommitIndex;i++){
                RaftProto.LogEntry entry = logModule.getEntry(i);
                stateMachine.apply(entry);
            }
            commitIndex = newCommitIndex;
            while (parkQueue.peek().getKey() <= commitIndex){
                Pair<Long,Condition> pair = parkQueue.poll();
                pair.getValue().signal();
            }
        }catch (Exception e){
            logger.error("advance commit index error",e);
            e.printStackTrace();
        }finally {
            stateMachineLock.unlock();
        }
    }

    private boolean tryInstallSnapshot(Peer peer) {
        if(peer.isInstallingSnapshot.compareAndSet(false,true)){
            long offset = 0;
            long intervalSize = Long.parseLong((String) RaftProperties.get("snapshot.transmit.size"));
            long totalSize = logModule.getSnapshot().getSnapshotSize();
            logModule.getSnapshotLock().lock();
            try {
                while (offset < totalSize) {
                    boolean isDone = offset + intervalSize >= totalSize;
                    RaftProto.InstallSnapshotRequest installSnapshotRequest = RaftProto.InstallSnapshotRequest.newBuilder()
                            .setTerm(currTerm)
                            .setLeaderId(peer.getId())
                            .setLastIncludedIndex(logModule.getLastIncludedIndex())
                            .setLastIncludedTerm(logModule.getLastIncludedTerm())
                            .setOffset(offset)
                            .setDone(isDone)
                            .setData(ByteString.copyFrom(logModule.getSnapshot().getSnapshotBytes(offset,intervalSize))).build();
                    trySendSnapShotBytes(installSnapshotRequest,peer);
                    offset += intervalSize;
                }
            }catch (Exception e){
                logModule.getSnapshotLock().unlock();
                logger.error("install snapshot failed",e);
                e.printStackTrace();
                return false;
            }
            nextIndex.put(peer.getId(),logModule.getLastIncludedIndex() + 1);
            logModule.getSnapshotLock().unlock();
            peer.isInstallingSnapshot.set(false);
            return logCopyHandler(peer);
        }else return false;
    }


    private void trySendSnapShotBytes(RaftProto.InstallSnapshotRequest installSnapshotRequest, Peer peer) throws RuntimeException{
        int times = RaftProperties.getProperty("install.snapshot.max.times") == null ? 3 : Integer.parseInt(RaftProperties.getProperty("install.snapshot.max.times"));
        for(int i = 0; i < times; i++){
            RaftProto.InstallSnapshotResponse installSnapshotResponse = peer.getConsensusService().installSnapshot(installSnapshotRequest);
            if(installSnapshotResponse == null){
                logger.error("install snapshot response is null,at times {}".formatted(i));
                continue;
            }
            if(installSnapshotResponse.getTerm() > currTerm){
                stepdown(installSnapshotResponse.getTerm());
                throw new RuntimeException("install snapshot failed because of other term is larger than current");
            }
            return;
        }
        throw new RuntimeException("install snapshot failed");
    }

    public boolean replicate(RaftProto.Configuration configuration, RaftProto.EntryType entryType){
        return false;
    }

    public byte[] read(byte[] command){
        if(status != NodeState.LEADER){
            logger.info("node " + peer.getId() + " is not leader");
            if (leaderId != null){
                RaftProto.ForwardRequest.Builder builder = RaftProto.ForwardRequest.newBuilder();
                builder.setType(RaftProto.FowardType.READ);
                builder.setData(ByteString.copyFrom(command));
                return peerMap.get(leaderId).getConsensusService().forward(builder.build()).getData().toByteArray();
            }
            return null;
        }
        byte[] result = stateMachine.apply(command);
        return result;
    }

    public boolean appendEntry(RaftProto.LogEntry entry){
        try {
            logModule.write(entry);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void applyConfigration(RaftProto.Configuration configuration){

    }

}
