#RaftKV
### 1. 项目简介
RaftKV是一个基于Raft协议实现的分布式KV存储系统，能够做到数据的高可用性和一致性。RaftKV的实现参考了MIT 6.824课程的Raft实验，但是在实现过程中，我对Raft协议的一些细节进行了修改，使得RaftKV能够更好的适应分布式KV存储系统的实现。RaftKV的实现语言为Java，使用了百度的brpc作为RPC框架，使用了Google的Protobuf作为序列化框架。  
需要强调的是系统的键值对存储只是一个简单的例子，RaftKV的实现可以很容易的扩展到其他的分布式存储系统中。项目的主要目的是为了学习Raft协议的实现，以及分布式系统的实现，所以重点实现是项目的中raft-core模块。这个可以作为一个第三方库供其他项目使用，只需要实现其中的几个接口，就可以实现分布式系统的快速开发。

### 2. raft核心功能
##### 2.1 Leader选举
Raft协议是一个中心化的协议，所有的操作都需要通过Leader节点进行处理，所以Leader选举的过程是Raft协议的核心。所以每个节点都需要具备选举功能，当节点启动时，首先会进行选举，选举出Leader后，Leader会周期性的向其他节点发送心跳包，如果其他节点在一定时间内没有收到Leader的心跳包，就会发起新一轮的选举，选举出新的Leader。
##### 2.2 日志复制
日志复制是Raft协议的核心功能，raft强调强一致性，所以日志复制需要做到过半数节点复制成功才能认为是成功。日志复制的过程中，Leader会将操作日志复制给Follower，Follower接受到日志后，会将日志写入本地日志文件，然后返回给Leader，Leader接受到过半数节点的返回后，会将日志应用到状态机，然后返回给客户端。需要强调的是，raft协议保证的是强一致性，具体说是线性一致性，所以不论是读还是写请求，都需要通过Leader节点进行处理，所以Follower接受这些请求后都会转发给Leader。Follower节点的状态往往会慢一步，需要Leader节点周期性的向Follower发送心跳包，来保证Follower节点的状态与Leader节点的状态一致。
##### 2.3 快照复制
随着日志增长，日志文件会越来越大，所以需要定期的对日志进行压缩，这个过程就是快照，节省空间的同时也可以让节点重启进行数据的一个快速恢复。论文提到，每个节点进行快照的时机是由各个节点自己决定，所以会出现有些落后节点的日志在Leader这边看来已经被压缩进快照了，这时候为了数据同步，必须进行快照复制，即Leader将快照发送给落后节点，落后节点接受到快照后，会将快照写入本地，然后读取状态到状态机，然后再继续后续的日志复制。需要强调我的实现里，进行快照不会影响到日志复制的进行，只不过不允许在快照完成前对状态机进行修改，所以会出现偶尔请求耗时会很长的情况，但会保证操作一定成功除非是集群崩溃。
##### 2.4 集群成员变更
TODO 目前还未实现  
按照论文的说法，集群成员变更是一个比较复杂的过程，需要依赖特殊的日志实现，要求增加节点时需要通过Leader发起这种配置更改的特殊日志，同步集群，所有收到该类日志的节点需要按照新旧配置进行处理，包括新增节点，新增节点哪怕想发起选主，不仅需要满足新配置的过半同意，还需要在旧配置的过半同意，这样才能保证新旧配置的过半节点都同意，才能保证选主成功。删除节点也是类似的，需要通过特殊日志进行删除，然后同步集群，所有收到该类日志的节点需要按照新旧配置进行处理，包括删除节点，删除节点哪怕想发起选主，不仅需要满足新配置的过半同意，还需要在旧配置的过半同意，这样才能保证新旧配置的过半节点都同意，才能有资格当主。直到新旧配置集群的节点过半同意这个配置更新后，当前leader就可以提交这个配置，之后都按照新配置来进行操作。
### 3. 系统运行结构
RaftKV的系统运行结构如下图所示：
![RaftKV系统运行结构](raftkv.png)
RaftKV的系统结构分为三层，分别是客户端层、Raft层和服务端层。客户端层用于发起操作请求，服务端用于接受请求通过调用Raft层实现读写，Raft层根据raft协议构建中心化集群，接受服务端的调用，将请求转发给集群中的其他节点，实现请求日志复制和操作同步，然后返回操作结果。需要强调的是，因为raft协议保证的是强一致性，具体说是线性一致性，所以不论是读还是写请求，都需要通过Leader节点进行处理，所以Follower接受这些请求后都会转发给Leader。
### 4. 项目结构
##### 4.1 raft-core
该模块是Raft协议的实现，主要包括Leader选举、日志复制、快照、集群成员变更等功能。该模块可以作为一个第三方库供其他项目使用，只需要实现StateMachine和继承SnapShot，根据自身需要定制实现服务即可使得该服务做到高可用和一致性。
```java
public interface StateMachine {
    public byte[] apply(byte[] commandBytes);
    public void readSnapshot(String snapshotPath);
    public void writeSnapshot(String snapshotPath);
    void apply(RaftProto.LogEntry entry);
}
```
```java
public abstract class Snapshot {

    protected String path;

    public Snapshot() {
    }
    public Snapshot(String path) {
        this.path = path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public abstract byte[] getSnapshotBytes(long offset, long intervalSize,String path);

    public abstract void writeSnapshotBytes(long offset,byte[] data,String path);

    public abstract long getSnapshotSize();
}
```
示例如下：
```java
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
```
通过调用raftNode的replicate方法实现日志复制以及状态机应用更改，read方法实现读取状态机的状态。
##### 4.2 raft-server
该模块是服务端的实现，主要包括服务端的启动、RPC服务的注册、RaftNode的初始化等功能。通过对SnapShot和StateMachine的示例实现，实现了一个简易的键值对存储服务器。
##### 4.3 raft-client
该模块是客户端的实现，主要包括客户端的启动、调用服务端的服务，测试分布式服务端的读写功能。
##### 4.4 raft-common
该模块是公共模块，内容只有客户端与服务端公用的序列化protobuf规则类和服务端服务接口。
