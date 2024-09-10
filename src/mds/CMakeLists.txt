if(WITH_RADOSGW_KAFKA_ENDPOINT)
  find_package(RDKafka 0.9.2 REQUIRED)
endif()

set(mds_srcs
  MDSNotificationInterface.cc
  BatchOp.cc
  Capability.cc
  MDSDaemon.cc
  MDSRank.cc
  Beacon.cc
  flock.cc
  locks.c
  journal.cc
  Server.cc
  Mutation.cc
  MDCache.cc
  RecoveryQueue.cc
  StrayManager.cc
  PurgeQueue.cc
  Locker.cc
  Migrator.cc
  MDBalancer.cc
  CDentry.cc
  CDir.cc
  CInode.cc
  LogEvent.cc
  MDSTable.cc
  InoTable.cc
  JournalPointer.cc
  MDSTableClient.cc
  MDSTableServer.cc
  ScrubStack.cc
  DamageTable.cc
  SimpleLock.cc
  SnapRealm.cc
  SnapServer.cc
  SnapClient.cc
  snap.cc
  SessionMap.cc
  MDSContext.cc
  MDLog.cc
  MDSCacheObject.cc
  Mantle.cc
  Anchor.cc
  OpenFileTable.cc
  MDSPinger.cc
  MetricAggregator.cc
  MetricsHandler.cc
  QuiesceDbManager.cc
  QuiesceAgent.cc
  MDSRankQuiesce.cc
  ${CMAKE_SOURCE_DIR}/src/common/TrackedOp.cc
  ${CMAKE_SOURCE_DIR}/src/common/MemoryModel.cc
  ${CMAKE_SOURCE_DIR}/src/osdc/Journaler.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MDSPerfMetricTypes.cc)

if(WITH_RADOSGW_KAFKA_ENDPOINT)
  list(APPEND mds_srcs MDSKafka.cc)
endif()

add_library(mds STATIC ${mds_srcs})
target_link_libraries(mds PRIVATE
  legacy-option-headers Boost::url
  heap_profiler cpu_profiler osdc ${LUA_LIBRARIES})

if(WITH_RADOSGW_KAFKA_ENDPOINT)
  target_link_libraries(mds PRIVATE RDKafka::RDKafka)
endif()

target_include_directories(mds PRIVATE "${LUA_INCLUDE_DIR}")
