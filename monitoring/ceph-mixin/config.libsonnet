{
  _config+:: {
    dashboardTags: ['ceph-mixin'],

    clusterLabel: 'cluster',
    showMultiCluster: true,

    CephNodeNetworkPacketDropsThreshold: 0.005,
    CephNodeNetworkPacketDropsPerSec: 10,
    CephRBDMirrorImageTransferBandwidthThreshold: 0.8,
    CephRBDMirrorImagesPerDaemonThreshold: 100,
    NVMeoFMaxGatewaysPerGroup: 8,
    NVMeoFMaxGatewayGroups: 4,
    NVMeoFMaxGatewaysPerCluster: 32,
    NVMeoFHighGatewayCPU: 80,
    NVMeoFMaxSubsystemsPerGateway: 128,
    NVMeoFMaxNamespaces: 2048,
    NVMeoFHighClientCount: 128,
    NVMeoFHostKeepAliveTimeoutTrackDurationHours: 24,
    NVMeoFHighHostCPU: 80,
    //
    // Read/Write latency is defined in ms
    NVMeoFHighClientReadLatency: 10,
    NVMeoFHighClientWriteLatency: 20,
  },
}
