package top.chaohaorui.raftkv.conf;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class RaftOptions {
    @NonNull
    public String multiAddress;
    @NonNull
    public String localid;

    @NonNull
    public String RaftLogDir;

    @NonNull
    public String SnapshotDir;

    @Builder.Default
    public String RaftFollowerHandlerWaitOutTime = "200";
    @Builder.Default
    public String RaftFollowerHandlerLimitSize = "5";
    @Builder.Default
    public String RaftHeartbeatInterval = "300";
    @Builder.Default
    public String ThreadPoolCorePoolSize = "6";
    @Builder.Default
    public String ThreadPoolMaxPoolSize = "12";
    @Builder.Default
    public String ThreadPoolKeepAliveTime = "0";
    @Builder.Default
    public String ThreadPoolQueueCapacity = "2";
    @Builder.Default
    public String SnapshotInterval  = "600000";
    @Builder.Default
    public String ElectionTimeoutBaseTime = "3000";
    @Builder.Default
    public String ElectionTimeoutRatio = "6";
    @Builder.Default
    public String ElectionElectWaitTime = "900";
    @Builder.Default
    public String RaftReplicateMaxWaitTime = "300";
    @Builder.Default
    public String InstallSnapshotMaxTime = "1";
    @Builder.Default
    public String SnapshotTransmitSize = "1048576";
}
