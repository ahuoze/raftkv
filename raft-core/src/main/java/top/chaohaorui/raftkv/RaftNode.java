package top.chaohaorui.raftkv;

import com.baidu.brpc.server.RpcServer;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.chaohaorui.raftkv.conf.RaftOptions;
import top.chaohaorui.raftkv.proto.RaftProto;
import top.chaohaorui.raftkv.service.ConsensusService;
import top.chaohaorui.raftkv.service.impl.DefaultConSensusService;
import top.chaohaorui.raftkv.store.LogModule;
import top.chaohaorui.raftkv.store.Snapshot;
import top.chaohaorui.raftkv.store.impl.DefaultLogModule;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {

    private Peer localPeer;
    private volatile NodeState status;
    private LogModule logModule;
    private Map<String,Peer> peerMap;
    private RaftOptions raftOptions;
    private StateMachine stateMachine;
    private ScheduledExecutorService scheduledExecutorService;
    private CompletionService<Boolean> completionService;


    private ThreadPoolExecutor threadPoolExecutor;
    private ScheduledFuture<Boolean> electionFuture;

    private ReentrantLock stateMachineLock;
    private ReentrantLock parkLock;
    private ReentrantLock electionLock;

    private Queue<Pair<Long,Condition>> parkQueue;

    private Map<String,Long> nextIndex;
    private Map<String,Long> matchIndex;
    private Map<String,FollowerHandler> handlerMap;
    private volatile long commitIndex;
    private volatile long appliedIndex;
    private static Logger logger = LoggerFactory.getLogger(RaftNode.class);
    private String leaderId;

    private AtomicBoolean isSkipElection;

    private RpcServer rpcServer;
    public ConsensusService consensusService;

    public volatile boolean isTakeSnapshot;



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

    public class FollowerHandler extends Thread{
        private Peer peer;
        private volatile boolean isRunning;
        private BlockingDeque<RaftProto.LogEntry> msgQueue;
        private long waitOutTime;
        private long limitSize;
        private long heartbeatInterval;
        private long heartbeatRecord;
        public FollowerHandler(Peer peer){
            super("FollowerHandler");
            isRunning = true;
            this.peer = peer;
            msgQueue = new LinkedBlockingDeque<>();
            waitOutTime = Math.min(
                    raftOptions.RaftFollowerHandlerWaitOutTime == null?
                    Long.parseLong(raftOptions.RaftReplicateMaxWaitTime) / 5 :
                    Long.parseLong(raftOptions.RaftFollowerHandlerWaitOutTime),
                    Long.parseLong(raftOptions.RaftReplicateMaxWaitTime) / 5
            );
            limitSize = Math.max(
                    raftOptions.RaftFollowerHandlerLimitSize == null ?
                    1 : Long.parseLong(raftOptions.RaftFollowerHandlerLimitSize)
                    ,1);
            heartbeatInterval = Long.parseLong(raftOptions.RaftHeartbeatInterval);
        }
        @Override
        public void run() {
            super.run();
            boolean synSuccess = logSynHandler(peer);
            if(!synSuccess){
                logger.error("FollowerHandler : " + peer.getId() + " syn failed");
            }else{
                logger.info("FollowerHandler : " + peer.getId() + " start working");
            }
            isRunning = synSuccess;
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long pre = System.currentTimeMillis();
            long now = pre;
            run : while (isRunning){
                try {
                    if(peer.getRpcClient().isShutdown()) break run;
                    RaftProto.LogEntry entry = msgQueue.poll();
                    if (entry != null){
                        if(entry.getIndex() < nextIndex.get(peer.getId())){
                            continue run;
                        }
                        entries.add(entry);
                    }
                    if(entries.size() >= limitSize ||
                    (System.currentTimeMillis() - pre >= waitOutTime && entries.size() > 0) ||
                        (now = System.currentTimeMillis()) / heartbeatInterval != heartbeatRecord
                    ){
                        heartbeatRecord = now / heartbeatInterval;
                        RaftProto.AppendEntryRequest.Builder builder = RaftProto.AppendEntryRequest.newBuilder();
                        builder.setTerm(logModule.getCurrTerm());
                        builder.setLeaderId(localPeer.getId());
                        RaftProto.LogEntry lastMatchEntry = logModule.getEntry(matchIndex.get(peer.getId()));
                        if(lastMatchEntry == null && matchIndex.get(peer.getId()) != 0){
                            logger.error("FollowerHandler Error : lastMatchEntry is null when matchIndex is {}",matchIndex.get(peer.getId()));
                            throw new IOException("lastMatchEntry is null");
                        }
                        if(matchIndex.get(peer.getId()) == 0){
                            builder.setPrevLogIndex(0);
                            builder.setPrevLogTerm(0);
                        }else {
                            builder.setPrevLogIndex(lastMatchEntry.getIndex());
                            builder.setPrevLogTerm(lastMatchEntry.getTerm());
                        }
                        long lastEntryIndex = (lastMatchEntry == null ? 0 : lastMatchEntry.getIndex()) +
                                entries.size();
                        builder.setCommitIndex(commitIndex > lastEntryIndex ?
                                lastEntryIndex : 
                                commitIndex);
                        for (RaftProto.LogEntry logEntry : entries) {
                            builder.addEntries(logEntry);
                        }
                        ConsensusService consensusService = peer.getConsensusService();
                        RaftProto.AppendEntryResponse response = consensusService.appendEntries(builder.build());
                        if(response == null){
                            logger.error("response is null");
                            throw new IOException("response is null");
                        }
                        if(response.getTerm() > logModule.getCurrTerm()){
                            stepdown(response.getTerm());
                            break;
                        }
                        if(response.getSuccess()){
                            nextIndex.put(peer.getId(),lastEntryIndex + 1);
                            matchIndex.put(peer.getId(),lastEntryIndex);
                            advanceCommitIndex();
                        }
                        entries.clear();
                        pre = System.currentTimeMillis();
                    }

                } catch (Exception e) {
                    logger.error("FollowerHandler is exception");
                    e.printStackTrace();
                    break;
                }
            }
            shutdown();
        }
        
        public void shutdown(){
            isRunning = false;
            synchronized (peer){
                handlerMap.remove(peer.getId());
                logger.info("FollowerHandler : " + peer.getId() + " stop working");
                checkHalfMember();
            }
        }
        
        public void addMsg(RaftProto.LogEntry entry){
            msgQueue.add(entry);
        }
    }

    private synchronized void checkHalfMember() {
        if(handlerMap.size() < peerMap.size() / 2 && status == NodeState.LEADER){
            logger.info(localPeer.getId() + ": half member crash down");
            stepdown(logModule.getCurrTerm());
        }
    }

    private void end() {
        handlerMap.forEach((k,v)->{
            v.shutdown();
        });
        handlerMap.clear();
    }

    public RaftNode(RaftOptions raftOptions, StateMachine stateMachine, Snapshot snapshot) throws IOException {
        this.raftOptions = raftOptions;
        String multiAddress = raftOptions.getMultiAddress();
        String localid = raftOptions.getLocalid();
        String[] address = multiAddress.split(",");
        snapshot.setPath(raftOptions.SnapshotDir);
        handlerMap = new ConcurrentHashMap<>();
        peerMap = new HashMap<>();
        for (String s : address) {
            String[] split = s.split(":");
            String id = split[0];
            String ip = split[1];
            String port = split[2];
            if(id.equals(localid)){
                localPeer = new Peer(id,ip,Integer.parseInt(port),true);
                continue;
            }
            Peer peer = new Peer(id,ip,Integer.parseInt(port),id.equals(localid));
            peerMap.put(id,peer);
            handlerMap.put(id,new FollowerHandler(peer));
        }
        threadPoolExecutor = new ThreadPoolExecutor(
                Integer.parseInt(raftOptions.ThreadPoolCorePoolSize),
                Integer.parseInt(raftOptions.ThreadPoolMaxPoolSize),
                Long.parseLong(raftOptions.ThreadPoolKeepAliveTime),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(Integer.parseInt(raftOptions.ThreadPoolQueueCapacity)),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        isSkipElection = new AtomicBoolean(false);
        rpcServer = new RpcServer(localPeer.getPort());
        consensusService = new DefaultConSensusService(this);
        rpcServer.registerService(consensusService);
        this.logModule = new DefaultLogModule(snapshot,raftOptions.RaftLogDir);
        nextIndex = new ConcurrentHashMap<>();
        matchIndex = new ConcurrentHashMap<>();
        parkQueue = new ConcurrentLinkedDeque<>();
        stateMachineLock = new ReentrantLock();
        parkLock = new ReentrantLock();
        electionLock = new ReentrantLock();
        this.stateMachine = stateMachine;
        completionService = new ExecutorCompletionService<>(threadPoolExecutor);
        scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
    }

    public void init(){
        rpcServer.start();
        logModule.init();
        commitIndex = logModule.getCommitIndex();
        peerMap.forEach((k,v)->{
            if (!v.isLocal()){
                v.init();
            }
        });
        status = NodeState.CANDIDATE;
        try {
            loadData();
            appliedIndex = logModule.getCommitIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
        beginElectionTimer();
        beginSnapshot();
    }

    private synchronized void beginSnapshot() {
        long snapshotTime = (long)(Long.parseLong(raftOptions.SnapshotInterval) * (1 + Math.random()));
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                takeSnapshot();
            }
        },snapshotTime,snapshotTime,TimeUnit.MILLISECONDS);
    }

    private void loadData() throws IOException {
        String path = this.logModule.getSnapshotDir();
        int snap_index = logModule.getSnap_index();
        String snap_path = path + File.separator + snap_index;
        stateMachine.readSnapshot(snap_path);
        long commitIndex = logModule.getCommitIndex();
        long lastIncludedIndex = logModule.getLastIncludedIndex();
        logModule.truncatePrefix(lastIncludedIndex + 1);
        logModule.updateMetaData(null,null,lastIncludedIndex + 1,
                null,null,null
                ,null,null,null);
        long firstLogIndex = logModule.getFirstLogIndex();
        for (long i = firstLogIndex; i <= commitIndex; i++) {
            RaftProto.LogEntry entry = logModule.getEntry(i);
            if(entry == null) {
                logger.error("load Data : could not find committed entry in log module");
                throw new RuntimeException("entry is null");
            }
            stateMachine.apply(entry.getData().toByteArray());
        }
    }

    public void resetElectionTimer() {
//        logger.info(localPeer.getId() + ": reset election timer");
        isSkipElection.compareAndSet(false,true);
    }

    public synchronized void beginElectionTimer(){
        electionFuture = scheduledExecutorService.schedule(new Callable<Boolean>() {
                                                               @Override
                                                               public Boolean call() {
                                                                   Boolean result = false;
                                                                   try {
                                                                       result = election();
                                                                       if(!result) {
                                                                           logger.info(localPeer.getId() + ": election failed");
                                                                       }
                                                                   }catch (Exception e){
                                                                       logger.info(localPeer.getId() + ": election failed");
                                                                       e.printStackTrace();
                                                                   }catch (Error e){
                                                                       logger.info(localPeer.getId() + ": election failed because of error");
                                                                       e.printStackTrace();
                                                                   }
                                                                   beginElectionTimer();
                                                                   return result;
                                                               }
                                                           },
                (long) (Long.parseLong(raftOptions.ElectionTimeoutBaseTime, 10) *
                                (
                                Math.random() *
                                        Math.min(Integer.parseInt(raftOptions.ElectionTimeoutRatio,10),10)
                                 + 1
                                ))
                , TimeUnit.MILLISECONDS);
    }

    private Boolean election() {
        if(isSkipElection.compareAndSet(true,false) || status == NodeState.LEADER){
            logger.info(localPeer.getId() + ": skip election");
            return true;
        }
        electionLock.lock();
        logger.info(localPeer.getId() + ": begin election");
        status = NodeState.CANDIDATE;
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        long electionTerm = logModule.getCurrTerm() + 1;
        logModule.updateMetaData(electionTerm, localPeer.getId(), null,
                null,null,null
                ,null,null,null);
        requestBuilder.setTerm(electionTerm);
        requestBuilder.setCandidateId(localPeer.getId());
        requestBuilder.setLastLogIndex(logModule.getLastLogIndex());
        requestBuilder.setLastLogTerm(logModule.getLastLogTerm());
        RaftProto.VoteRequest request = requestBuilder.build();
        List<Future> futureList = new ArrayList<>();

        int count = (peerMap.size() + 1) / 2;
        CountDownLatch latch = new CountDownLatch(count);
        for (Peer peer : peerMap.values()) {
            futureList.add(
                    completionService.submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() {
                            try {
                                logger.info("{} send vote request to {}",localPeer.getId(),peer.getId());
                                logger.info("vote to {} in {}:{}",peer.getId(),peer.getIp(),peer.getPort());
                                RaftProto.VoteResponse voteResponse = peer.getConsensusService().vote(request);
                                if (voteResponse == null) {
                                    logger.warn("vote response is null");
                                    return false;
                                }
                                if (voteResponse.getTerm() > logModule.getCurrTerm()) {
                                    stepdown(voteResponse.getTerm());
                                    return false;
                                }
                                if (voteResponse.getVoteGranted()) {
                                    latch.countDown();
                                    logger.info(peer.getId() + " agree the host " + localPeer.getId() + " to be leader in  term :  " + logModule.getCurrTerm());
                                    return true;
                                }
                                return false;
                            }catch (Exception e){
                                logger.error("vote to {} failed",peer.getId());
                                return false;
                            }
                        }
                    })
            );
        }
        try {
            latch.await(Long.parseLong(raftOptions.ElectionElectWaitTime),TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("选举等待过程被打断");
            e.printStackTrace();
        }
        if(latch.getCount() > 0){
            logger.info(localPeer.getId() + " : 选举失败");
            electionLock.unlock();
            return false;
        }
        status = NodeState.LEADER;
        setLeaderId(localPeer.getId());
        logger.info("node " + localPeer.getId() + " become leader");
        electionLock.unlock();
        replicate((byte[]) null,RaftProto.EntryType.DADA);
        handlerMap.forEach((k,v)->{
            threadPoolExecutor.submit(v);
        });
        return true;
    }

    public synchronized void stepdown(long term) {
        if(logModule.getCurrTerm() > term){
            logger.warn("can not stepdown because term is smaller than current term");
            return;
        }
        try {
            logger.info("node " + localPeer.getId() + " stepdown");
            if(status == NodeState.LEADER){
                end();
            }
            resetElectionTimer();
            status = NodeState.FOLLOWER;
            setLeaderId(null);
            if(logModule.getCurrTerm() < term){
                logger.info("node " + localPeer.getId() + " turn term to : " + term);
                logModule.updateMetaData(term, "nil",null,
                        null,null,null,
                        null,null,null);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public boolean replicate(byte[] command,RaftProto.EntryType entryType){
        if(status != NodeState.LEADER){
            logger.info("node " + localPeer.getId() + " is not leader");
            if (leaderId != null){
                RaftProto.ForwardRequest.Builder builder = RaftProto.ForwardRequest.newBuilder();
                builder.setType(RaftProto.FowardType.WRITE);
                builder.setData(ByteString.copyFrom(command));
                return peerMap.get(leaderId).getConsensusService().forward(builder.build()).getSuccess();
            }
            return false;
        }
        RaftProto.LogEntry.Builder entryBuilder = RaftProto.LogEntry.newBuilder();
        if(command != null){
            entryBuilder.setData(ByteString.copyFrom(command));
        }
        RaftProto.LogEntry logEntry = entryBuilder.setTerm(logModule.getCurrTerm())
                .setType(entryType)
                .build();
        Condition currThreadCondition = parkLock.newCondition();
        try {
            parkLock.lock();
            logEntry = logModule.write(logEntry);
//            System.out.println("写入成功并测试");
//            System.out.println(logModule.getEntry(logEntry.getIndex()));
            parkQueue.add(new Pair<>(logEntry.getIndex(),currThreadCondition));
            for (Peer peer : peerMap.values()) {
                checkAndHandle(peer,logEntry);
            }
            long start = System.currentTimeMillis();
            long now = start;
            while (appliedIndex < logEntry.getIndex()){
                if(now - start > Long.parseLong(raftOptions.RaftReplicateMaxWaitTime) &&
                    !isTakeSnapshot){
                    break;
                }
                now = System.currentTimeMillis();
                try {
                    currThreadCondition.await(Long.parseLong(raftOptions.RaftReplicateMaxWaitTime),TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            parkLock.unlock();
        }
        if(appliedIndex >= logEntry.getIndex())
            logger.info("node " + localPeer.getId() + " replicate at index " + logEntry.getIndex() + " success");
        return appliedIndex >= logEntry.getIndex();
    }

//    public void recieveMsgAndHandle(String peerId){
//        if(status != NodeState.LEADER){
//            logger.warn("node " + localPeer.getId() + " is not leader so can not recieve msg and handle");
//            return;
//        }
//        if(!peerMap.containsKey(peerId)){
//            logger.error("node " + localPeer.getId() + " has not peer " + peerId);
//            return;
//        }
//        checkAndHandle(peerMap.get(peerId),null);
//    }

    private void checkAndHandle(Peer peer, RaftProto.LogEntry logEntry) {
        synchronized (peer){
            if(status != NodeState.LEADER){
                logger.warn("node " + localPeer.getId() + " is not leader so can not create handle");
                return;
            }
            if(!handlerMap.containsKey(peer.getId())){
                FollowerHandler followerHandler = new FollowerHandler(peer);
                handlerMap.put(peer.getId(),followerHandler);
                threadPoolExecutor.submit(followerHandler);
            }
            if(logEntry != null)
                handlerMap.get(peer.getId()).addMsg(logEntry);
        }
    }

    private boolean logSynHandler(Peer peer) {
        Long nextLogIndex = nextIndex.getOrDefault(peer.getId(), logModule.getLastLogIndex());
        try {
            if(nextLogIndex == 0) {
                nextIndex.put(peer.getId(), 1L);
                matchIndex.put(peer.getId(), 0L);
                return true;
            }
            do {long prevLogIndex = nextLogIndex - 1;
                long prevLogTerm = prevLogIndex <= 0 ? 0 : logModule.getTerm(prevLogIndex);
                if(prevLogTerm == -1){
                    logger.error("Syn Data : prevLogTerm is -1 when prevLogIndex is " + prevLogIndex);
                    throw new RuntimeException("prevLogTerm is -1");
                }
                RaftProto.AppendEntryRequest.Builder requestBuilder = RaftProto.AppendEntryRequest.newBuilder();
                requestBuilder.setTerm(logModule.getCurrTerm());
                requestBuilder.setLeaderId(localPeer.getId());
                requestBuilder.setPrevLogIndex(prevLogIndex);
                requestBuilder.setPrevLogTerm(prevLogTerm);
                requestBuilder.setCommitIndex(commitIndex);
                long lastLogIndex = logModule.getLastLogIndex();
                for(long i = nextLogIndex; i <= lastLogIndex; i++){
                    RaftProto.LogEntry logEntry = logModule.getEntry(i);
                    if(logEntry == null) {
                        logger.error("Syn Data : before reaching the last log index, the log entry is null, and the index is " + i + ", the last log index is " + lastLogIndex);
                        return false;
                    }
                    requestBuilder.addEntries(logEntry);
                }
                RaftProto.AppendEntryResponse appendEntryResponse = peer.getConsensusService().appendEntries(requestBuilder.build());
                if(appendEntryResponse == null){
                    logger.warn("append entry response is null");
                    return false;
                }
                if(appendEntryResponse.getTerm() > logModule.getCurrTerm()){
                    stepdown(appendEntryResponse.getTerm());
                    return false;
                }
                if(appendEntryResponse.getSuccess()){
                    nextIndex.put(peer.getId(),lastLogIndex + 1);
                    matchIndex.put(peer.getId(),lastLogIndex);
                    advanceCommitIndex();
                    return true;
                }
                nextLogIndex = appendEntryResponse.getLastLogIndex() + 1;
            }while (nextLogIndex >= logModule.getFirstLogIndex());
            return tryInstallSnapshot(peer);
        }catch (Exception e){
            logger.error("Syn Data : exception happened when syn data to peer " + peer.getId());
            e.printStackTrace();
            return false;
        }
    }


    private void advanceCommitIndex() {
        try {
            List<Long> macths = new ArrayList<>(matchIndex.values());
            Collections.sort(macths);
            long newCommitIndex = macths.get(macths.size() / 2);
            setCommitIndex(newCommitIndex);
            stateMachineLock.lock();
            for (long i = appliedIndex + 1;i <= newCommitIndex;i++){
                RaftProto.LogEntry entry = logModule.getEntry(i);
                if(entry == null) {
                    logger.error("Adavance Commit : there is some entries which is committed but not exist in leader log module");
                    throw new RuntimeException("entry is null");
                }
                byte[] bytes = entry.getData().toByteArray();
                stateMachine.apply(bytes);
            }
            setAppliedIndex(newCommitIndex);
            parkLock.lock();
            while (!parkQueue.isEmpty() && parkQueue.peek().getKey() <= appliedIndex){
                Pair<Long,Condition> pair = parkQueue.poll();
                pair.getValue().signal();
            }
        }catch (Exception e){
            logger.error("advance commit index error",e);
            e.printStackTrace();
        }finally {
            stateMachineLock.unlock();
            parkLock.unlock();
        }
    }


    /** 在合适的时机对当前已经提交的数据做快照 ，也就是需要做判断**/
    public void takeSnapshot() {
        if(isTakeSnapshot){
            return;
        }
        ReadWriteLock snapshotLock = logModule.getSnapshotLock();
        snapshotLock.writeLock().lock();
        stateMachineLock.lock();
        isTakeSnapshot = true;
        try {
            String path = logModule.getSnapshotDir();
            int nextIndex = logModule.getSnap_index() ^ 1;
            String newPath = path + File.separator + nextIndex;
            stateMachine.writeSnapshot(newPath);
            logModule.updateMetaData(null,null,null,
                    null,null,null,
                    appliedIndex, logModule.getTerm(appliedIndex),nextIndex);

        }catch (Exception e){
            logger.error("take snapshot error",e);
            e.printStackTrace();
        }finally {
            snapshotLock.writeLock().unlock();
            stateMachineLock.unlock();
            isTakeSnapshot = false;
        }
    }

    private boolean tryInstallSnapshot(Peer peer) throws IOException {
        if(peer.isInstallingSnapshot.compareAndSet(false,true)){
            long offset = 0;
            long intervalSize = Long.parseLong(raftOptions.SnapshotTransmitSize);
            long totalSize = logModule.getSnapshot().getSnapshotSize();
            logModule.getSnapshotLock().writeLock().lock();
            try {
                while (offset < totalSize) {
                    boolean isDone = offset + intervalSize >= totalSize;
                    RaftProto.InstallSnapshotRequest installSnapshotRequest = RaftProto.InstallSnapshotRequest.newBuilder()
                            .setTerm(logModule.getCurrTerm())
                            .setLeaderId(peer.getId())
                            .setLastIncludedIndex(logModule.getLastIncludedIndex())
                            .setLastIncludedTerm(logModule.getLastIncludedTerm())
                            .setOffset(offset)
                            .setDone(isDone)
                            .setData(ByteString.copyFrom(logModule.getSnapshot().getSnapshotBytes(offset,
                                    intervalSize,logModule.getSnapshotDir()))).build();
                    trySendSnapShotBytes(installSnapshotRequest,peer);
                    offset += intervalSize;
                }
            }catch (Exception e){
                logModule.getSnapshotLock().writeLock().unlock();
                logger.error("install snapshot failed",e);
                e.printStackTrace();
                return false;
            }
            nextIndex.put(peer.getId(),logModule.getLastIncludedIndex() + 1);
            logModule.getSnapshotLock().writeLock().unlock();
            peer.isInstallingSnapshot.set(false);
            return logSynHandler(peer);
        }else return false;
    }


    private void trySendSnapShotBytes(RaftProto.InstallSnapshotRequest installSnapshotRequest, Peer peer) throws RuntimeException,IOException{
        int times = raftOptions.InstallSnapshotMaxTime == null ? 3 : Integer.parseInt(raftOptions.InstallSnapshotMaxTime);
        for(int i = 0; i < times; i++){
            RaftProto.InstallSnapshotResponse installSnapshotResponse = peer.getConsensusService().installSnapshot(installSnapshotRequest);
            if(installSnapshotResponse == null){
                logger.error("install snapshot response is null,at times {}".formatted(i));
                continue;
            }
            if(installSnapshotResponse.getTerm() > logModule.getCurrTerm()){
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
            logger.info("node " + localPeer.getId() + " is not leader");
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



    public long getCurrTerm() {
        return logModule.getCurrTerm();
    }

    public String getVotedFor() {
        return logModule.getVotedFor();
    }



    public NodeState getStatus() {
        return status;
    }

    public LogModule getLogModule() {
        return logModule;
    }

    public RaftOptions getRaftOptions() {
        return raftOptions;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }


    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }


    public ReentrantLock getStateMachineLock() {
        return stateMachineLock;
    }


    public synchronized void setCommitIndex(long commitIndex) {
        if(commitIndex <= this.commitIndex) return;
        this.commitIndex = commitIndex;
        logModule.updateMetaData(null,null,null,
                null,null,commitIndex,
                null,null,null);
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setAppliedIndex(long appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    public long getAppliedIndex() {
        return appliedIndex;
    }



    public ReentrantLock getElectionLock() {
        return electionLock;
    }


    public Peer getLocalPeer() {
        return localPeer;
    }


}
