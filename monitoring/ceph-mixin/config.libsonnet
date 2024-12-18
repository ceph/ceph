{
  _config+:: {
    dashboardTags: ['ceph-mixin'],

    clusterLabel: 'cluster',
    showMultiCluster: true,

    CephNodeNetworkPacketDropsThreshold: 0.005,
    CephNodeNetworkPacketDropsPerSec: 10,
    CephRBDMirrorImageTransferBandwidthThreshold: 0.8,
    CephRBDMirrorImagesPerDaemonThreshold: 100,
    NVMeoFMaxGatewaysPerGroup: 4,
    NVMeoFMaxGatewaysPerCluster: 4,
    NVMeoFHighGatewayCPU: 80,
    NVMeoFMaxSubsystemsPerGateway: 128,
    NVMeoFMaxNamespaces: 1024,
    NVMeoFHighClientCount: 32,
    NVMeoFHighHostCPU: 80,
    //
    // Read/Write latency is defined in ms
    NVMeoFHighClientReadLatency: 10,
    NVMeoFHighClientWriteLatency: 20,
  },
}
