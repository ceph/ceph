include(GetGitRevisionDescription)

set(bindir ${CMAKE_INSTALL_PREFIX}/bin)
set(sbindir ${CMAKE_INSTALL_PREFIX}/sbin)
set(libdir ${CMAKE_INSTALL_PREFIX}/lib)
set(sysconfdir ${CMAKE_INSTALL_PREFIX}/etc)
set(pkgdatadir ${CMAKE_INSTALL_PREFIX}/share)
set(prefix ${CMAKE_INSTALL_PREFIX})

add_definitions("-DCEPH_LIBDIR=\"${libdir}\"")
add_definitions("-DCEPH_PKGLIBDIR=\"${libdir}\"")
add_definitions("-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -ftemplate-depth-1024 -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel -Woverloaded-virtual")

set(EXTRALIBS uuid rt dl ${Boost_LIBS})

if(${WITH_PROFILER})
  list(APPEND EXTRALIBS profiler)
endif(${WITH_PROFILER})

if(WITH_CDS)
  list(APPEND EXTRALIBS ${CDS_LIBS})
endif(WITH_CDS)

set(CRYPTO_LIBS cryptopp)

if(${HAVE_ATOMIC_OPS})
  set(EXTRALIBS
	atomic_ops
	${EXTRALIBS})
endif(${HAVE_ATOMIC_OPS})

set(GCOV_PREFIX_STRIP 4)

get_git_head_revision(GIT_REFSPEC CEPH_GIT_VER)
git_describe(CEPH_GIT_NICE_VER --always)

# Python stuff
find_package(PythonInterp 2 QUIET)
if(NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Python 2 interpreter not found.")
endif(NOT PYTHONINTERP_FOUND)

# if CMAKE_INSTALL_PREFIX is an empty string, must replace
# it with "/" to make PYTHON_INSTALL_TEMPLATE an absolute path to be
# consistent with all other installation paths.
if(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "${CMAKE_INSTALL_PREFIX}")
else(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "/")
endif(CMAKE_INSTALL_PREFIX)

execute_process(
  COMMAND
  ${PYTHON_EXECUTABLE} -c "from distutils import sysconfig; print sysconfig.get_python_lib(1,0,prefix='${PYTHON_INSTALL_TEMPLATE}')"
  OUTPUT_VARIABLE PYTHON_INSTDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(HAVE_XIO)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${Xio_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${Xio_LIBRARY} ibverbs rdmacm pthread rt)
endif(HAVE_XIO)

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

# tcmalloc heap profiler
set(heap_profiler_files ${TCMALLOC_srcs})
add_library(heap_profiler_objs OBJECT ${heap_profiler_files})

set(LIBEDIT_LIBS edit)

# Common infrastructure
configure_file(
  ${CMAKE_SOURCE_DIR}/src/ceph_ver.h.in.cmake
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  @ONLY)

set(arch_files
  arch/arm.c
  arch/intel.c
  arch/probe.cc)

set(auth_files
  auth/AuthAuthorizeHandler.cc
  auth/AuthClientHandler.cc
  auth/AuthSessionHandler.cc
  auth/AuthMethodList.cc
  auth/cephx/CephxAuthorizeHandler.cc
  auth/cephx/CephxClientHandler.cc
  auth/cephx/CephxProtocol.cc
  auth/cephx/CephxSessionHandler.cc
  auth/none/AuthNoneAuthorizeHandler.cc
  auth/unknown/AuthUnknownAuthorizeHandler.cc
  auth/Crypto.cc
  auth/KeyRing.cc
  auth/RotatingKeyRing.cc)

set(mds_files)
list(APPEND mds_files
  mds/MDSMap.cc
  mds/inode_backtrace.cc
  mds/mdstypes.cc)

set(crush_srcs
  crush/builder.c
  crush/mapper.c
  crush/crush.c
  crush/hash.c
  crush/CrushWrapper.cc
  crush/CrushCompiler.cc
  crush/CrushTester.cc)

add_subdirectory(json_spirit)

set(xio_common_srcs)
if(HAVE_XIO)
  list(APPEND xio_common_srcs
    msg/xio/XioConnection.cc
    msg/xio/XioMsg.cc
    msg/xio/XioPool.cc
    msg/xio/XioMessenger.cc
    msg/xio/XioPortal.cc
    msg/xio/QueueStrategy.cc)
endif(HAVE_XIO)

set(libcommon_files
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  ceph_ver.c
  common/DecayCounter.cc
  common/LogClient.cc
  common/LogEntry.cc
  common/PrebufferedStreambuf.cc
  common/BackTrace.cc
  common/perf_counters.cc
  common/Mutex.cc
  common/OutputDataSocket.cc
  common/admin_socket.cc
  common/admin_socket_client.cc
  common/bloom_filter.cc
  common/Readahead.cc
  ${crush_srcs}
  common/cmdparse.cc
  common/escape.c
  common/io_priority.cc
  common/Clock.cc
  common/Throttle.cc
  common/Timer.cc
  common/Finisher.cc
  common/environment.cc
  common/sctp_crc32.c
  common/crc32c.cc
  common/crc32c_intel_baseline.c
  common/crc32c_intel_fast.c
  common/assert.cc
  common/run_cmd.cc
  common/WorkQueue.cc
  common/ConfUtils.cc
  common/MemoryModel.cc
  common/fd.cc
  common/xattr.c
  common/str_list.cc
  common/str_map.cc
  common/snap_types.cc
  common/errno.cc
  common/TrackedOp.cc
  common/SloppyCRCMap.cc
  common/types.cc
  common/TextTable.cc
  log/Log.cc
  log/SubsystemMap.cc
  mon/MonCap.cc
  mon/MonClient.cc
  mon/MonMap.cc
  msg/simple/Accepter.cc
  msg/simple/DispatchQueue.cc
  msg/Message.cc
  osd/ECMsgTypes.cc
  osd/HitSet.cc
  common/RefCountedObj.cc
  msg/Messenger.cc
  msg/simple/Pipe.cc
  msg/simple/PipeConnection.cc
  msg/simple/SimpleMessenger.cc
  msg/async/AsyncConnection.cc
  msg/async/AsyncMessenger.cc
  msg/async/Event.cc
  msg/async/EventEpoll.cc
  msg/async/EventSelect.cc
  msg/async/net_handler.cc
  ${xio_common_srcs}
  msg/msg_types.cc
  common/hobject.cc
  osd/OSDMap.cc
  common/histogram.cc
  osd/osd_types.cc
  common/blkdev.cc
  common/common_init.cc
  common/pipe.c
  common/ceph_argparse.cc
  common/ceph_context.cc
  common/buffer.cc
  common/code_environment.cc
  common/dout.cc
  common/signal.cc
  common/simple_spin.cc
  common/Thread.cc
  common/Formatter.cc
  common/HeartbeatMap.cc
  common/ceph_fs.cc
  common/ceph_hash.cc
  common/ceph_strings.cc
  common/ceph_frag.cc
  common/config.cc
  common/utf8.c
  common/mime.c
  common/strtol.cc
  common/page.cc
  common/lockdep.cc
  common/version.cc
  common/hex.cc
  common/entity_name.cc
  common/ceph_crypto.cc
  common/ceph_crypto_cms.cc
  common/ceph_json.cc
  common/ipaddr.cc
  common/pick_address.cc
  common/address_helper.cc
  common/linux_version.c
  osdc/Striper.cc
  osdc/Objecter.cc
  ${arch_files}
  ${auth_files}
  ${mds_files})
set(mon_common_files
  auth/AuthSessionHandler.cc
  auth/cephx/CephxSessionHandler.cc
  erasure-code/ErasureCodePlugin.cc)
add_library(mon_common_objs OBJECT ${mon_common_files})
set(common_mountcephfs_files
  common/armor.c
  common/safe_io.c
  common/module.c
  common/addr_parsing.c)
add_library(common_mountcephfs_objs OBJECT
  ${common_mountcephfs_files})

if(${WITH_PROFILER})
  list(APPEND libcommon_files
    perfglue/cpu_profiler.cc)
else()
  list(APPEND libcommon_files
    perfglue/disabled_stubs.cc)
endif(${WITH_PROFILER})

if(${ENABLE_SHARED})
  list(APPEND libcommon_files
    $<TARGET_OBJECTS:global_common_objs>)
endif(${ENABLE_SHARED})

add_library(common STATIC ${libcommon_files}
  $<TARGET_OBJECTS:mon_common_objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>)

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c
  ${CMAKE_SOURCE_DIR}/src/common/version.cc
  ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if(${WITH_PROFILER})
  target_link_libraries(common profiler)
endif(${WITH_PROFILER})

target_link_libraries(common json_spirit erasure_code rt uuid ${CRYPTO_LIBS} ${Boost_LIBRARIES})

set(libglobal_srcs
  global/global_init.cc
  global/pidfile.cc 
  global/signal_handler.cc)
set(global_common_files
  global/global_context.cc)
add_library(global_common_objs OBJECT ${global_common_files})
add_library(global STATIC ${libglobal_srcs}
  $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS}
  ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(global PROPERTIES
    OUTPUT_NAME ceph-global VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

# rados object classes
add_subdirectory(cls)

# RADOS client/library
set(osdc_files
  osdc/Objecter.cc)
set(osdc_rbd_files
  osdc/ObjectCacher.cc
  osdc/Striper.cc)
if(${WITH_CLIENT})
  list(APPEND osdc_files
    osdc/Filer.cc)
endif(${WITH_CLIENT})
add_library(osdc_rbd_objs OBJECT ${osdc_rbd_files})
add_library(osdc STATIC ${osdc_files} $<TARGET_OBJECTS:osdc_rbd_objs>)

set(librados_srcs
  librados/librados.cc
  librados/RadosClient.cc
  librados/IoCtxImpl.cc
  librados/snap_set_diff.cc
  librados/RadosXattrIter.cc
  )
add_library(librados ${CEPH_SHARED} ${librados_srcs}
  $<TARGET_OBJECTS:cls_references_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>)
add_dependencies(librados osdc)
target_link_libraries(librados PRIVATE osdc osd os global common cls_lock_client
  ${CRYPTO_LIBS} ${EXTRALIBS} ${TCMALLOC_LIBS})
if(${ENABLE_SHARED})
  set_target_properties(librados PROPERTIES OUTPUT_NAME rados VERSION 2.0.0
    SOVERSION 2)
endif(${ENABLE_SHARED})
install(FILES include/rados/librados.h
  include/rados/rados_types.h
  include/rados/rados_types.hpp
  include/rados/librados.hpp
  include/buffer.h
  include/page.h
  include/crc32c.h
  DESTINATION include/rados)
install(TARGETS librados DESTINATION lib)

set(rados_srcs
  tools/rados/rados.cc
  tools/rados/rados_sync.cc
  tools/rados/rados_import.cc
  tools/rados/rados_export.cc
  common/obj_bencher.cc)
add_executable(rados ${rados_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(rados librados global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

set(librados_config_srcs
  librados-config.cc)
add_executable(librados-config ${librados_config_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(librados-config librados global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS})

install(TARGETS rados librados-config DESTINATION bin)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/
  DESTINATION ${PYTHON_INSTDIR})

# Monitor
set(lib_mon_srcs
  auth/cephx/CephxKeyServer.cc
  auth/cephx/CephxServiceHandler.cc
  auth/AuthServiceHandler.cc
  ${osd_mon_files} mon/Paxos.cc 
  mon/PaxosService.cc
  mon/OSDMonitor.cc
  mon/MDSMonitor.cc
  mon/MonmapMonitor.cc 
  mon/LogMonitor.cc
  mon/AuthMonitor.cc
  mon/Elector.cc
  mon/MonitorStore.cc  
  mon/HealthMonitor.cc
  ${os_mon_files}
  mon/DataHealthService.cc
  mon/PGMonitor.cc
  mon/PGMap.cc
  mon/ConfigKeyService.cc)

set(common_util_src
  common/util.cc)
set(monitorstore_src
  mon/MonitorStore.cc)
add_library(common_util_obj OBJECT ${common_util_src})
add_library(monitorstore_obj OBJECT ${monitorstore_src})
add_library(mon STATIC ${lib_mon_srcs} $<TARGET_OBJECTS:mon_common_objs>
  $<TARGET_OBJECTS:os_mon_objs> $<TARGET_OBJECTS:osd_mon_objs>
  $<TARGET_OBJECTS:common_util_obj> $<TARGET_OBJECTS:monitorstore_obj>)

set(ceph_mon_srcs
  ceph_mon.cc
  common/TextTable.cc)
add_executable(ceph-mon ${ceph_mon_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(ceph-mon mon boost_thread common os global ${EXTRALIBS}
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})
install(TARGETS ceph-mon DESTINATION bin)

# OSD
set(libos_srcs
  os/FileJournal.cc
  os/FileStore.cc
  os/chain_xattr.cc
  os/ObjectStore.cc
  os/JournalingObjectStore.cc
  os/LFNIndex.cc
  os/IndexManager.cc
  os/FlatIndex.cc
  os/LevelDBStore.cc
  os/DBObjectMap.cc 
  os/Transaction.cc
  os/WBThrottle.cc
  os/GenericFileStoreBackend.cc
  os/BtrfsFileStoreBackend.cc
  os/ZFSFileStoreBackend.cc
  os/XfsFileStoreBackend.cc
  os/KeyValueStore.cc
  os/KeyValueDB.cc
  os/MemStore.cc
  os/GenericObjectMap.cc
  os/HashIndex.cc)
set(os_mon_files
  os/LevelDBStore.cc)
add_library(os_mon_objs OBJECT ${os_mon_files})
add_library(os STATIC ${libos_srcs} $<TARGET_OBJECTS:os_mon_objs>)
if(${HAVE_LIBAIO})
  target_link_libraries(os aio)
endif(${HAVE_LIBAIO})
target_link_libraries(os leveldb snappy)

set(cls_references_files objclass/class_api.cc)
add_library(cls_references_objs OBJECT ${cls_references_files})

set(osdc_osd_srcs
  osdc/Objecter.cc
  osdc/Striper.cc)

set(osd_srcs
  osd/OSD.cc
  osd/Watch.cc
  osd/ClassHandler.cc
  osd/OpRequest.cc
  osd/PG.cc
  osd/PGLog.cc
  osd/ReplicatedPG.cc
  osd/ReplicatedBackend.cc
  osd/ECBackend.cc
  osd/ECTransaction.cc
  osd/PGBackend.cc
  osd/Ager.cc
  osd/OSD.cc
  osd/OSDCap.cc
  osd/Watch.cc
  osd/ClassHandler.cc
  osd/OpRequest.cc
  common/TrackedOp.cc
  osd/SnapMapper.cc
  osd/osd_types.cc
  osd/ECUtil.cc
  objclass/class_api.cc
  ${osdc_osd_srcs})
set(osd_mon_files
  mon/Monitor.cc)
add_library(osd_mon_objs OBJECT ${osd_mon_files})
add_library(osd STATIC ${osd_srcs} $<TARGET_OBJECTS:osd_mon_objs>
  $<TARGET_OBJECTS:cls_references_objs>)
target_link_libraries(osd dl leveldb)

set(ceph_osd_srcs
  ceph_osd.cc
  objclass/class_api.cc)
add_executable(ceph-osd ${ceph_osd_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(ceph-osd osd os global ${TCMALLOC_LIBS})
install(TARGETS ceph-osd DESTINATION bin)

# MDS
if(${WITH_MDS})
  set(mds_srcs 
    mds/Capability.cc
    mds/MDS.cc
    mds/Beacon.cc
    mds/flock.cc
    mds/locks.c
    mds/journal.cc
    mds/Server.cc
    mds/Mutation.cc
    mds/MDCache.cc
    mds/RecoveryQueue.cc
    mds/Locker.cc
    mds/Migrator.cc
    mds/MDBalancer.cc
    mds/CDentry.cc
    mds/CDir.cc
    mds/CInode.cc
    mds/LogEvent.cc
    mds/MDSTable.cc
    mds/InoTable.cc
    mds/MDSTableClient.cc
    mds/MDSTableServer.cc
    mds/SnapRealm.cc
    mds/SnapServer.cc
    mds/snap.cc
    mds/SessionMap.cc
    mds/MDSContext.cc
    mds/MDSAuthCaps.cc
    mds/MDLog.cc
    mds/JournalPointer.cc
    ${CMAKE_SOURCE_DIR}/src/osdc/Journaler.cc)
  add_library(mds ${mds_srcs})
  set(ceph_mds_srcs
    ceph_mds.cc)
  add_executable(ceph-mds ${ceph_mds_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
  target_link_libraries(ceph-mds mds osdc ${CMAKE_DL_LIBS} global
    ${TCMALLOC_LIBS} boost_thread)
  install(TARGETS ceph-mds DESTINATION bin)
endif(${WITH_MDS})

add_subdirectory(erasure-code)

# Support/Tools
add_subdirectory(gmock)
add_subdirectory(test)
set(cephfs_srcs cephfs.cc)
add_executable(cephfstool ${cephfs_srcs})
target_link_libraries(cephfstool common ${EXTRALIBS})
set_target_properties(cephfstool PROPERTIES OUTPUT_NAME cephfs)
install(TARGETS cephfstool DESTINATION bin)

#set(ceph_srcs tools/ceph.cc tools/common.cc)
#add_executable(ceph ${ceph_srcs})
#target_link_libraries(ceph global ${LIBEDIT_LIBS})

set(ceph_conf_srcs
  tools/ceph_conf.cc)
add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)
install(TARGETS ceph-conf DESTINATION bin)

set(monmaptool_srcs
  tools/monmaptool.cc)
add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)
install(TARGETS monmaptool DESTINATION bin)

set(osdomaptool_srcs
  tools/osdmaptool.cc)
add_executable(osdmaptool ${osdomaptool_srcs})
target_link_libraries(osdmaptool global ${CMAKE_DL_LIBS})
install(TARGETS osdmaptool DESTINATION bin)

set(ceph_authtool_srcs
  tools/ceph_authtool.cc)
add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global ${EXTRALIBS})
install(TARGETS ceph-authtool DESTINATION bin)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
  ${CMAKE_BINARY_DIR}/ceph-coverage @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
  ${CMAKE_BINARY_DIR}/ceph-debugpack @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph.in.cmake
  ${CMAKE_BINARY_DIR}/ceph @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-crush-location.in
  ${CMAKE_BINARY_DIR}/ceph-crush-location @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/init-ceph.in
  ${CMAKE_BINARY_DIR}/init-ceph @ONLY)

install(PROGRAMS
  ${CMAKE_BINARY_DIR}/ceph
  ${CMAKE_BINARY_DIR}/ceph-debugpack
  ${CMAKE_BINARY_DIR}/ceph-coverage
  ${CMAKE_BINARY_DIR}/init-ceph
  ${CMAKE_SOURCE_DIR}/src/ceph-run
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph-clsinfo
  DESTINATION bin)

install(FILES
  ${CMAKE_SOURCE_DIR}/doc/start/ceph.conf
  DESTINATION ${sysconfdir}/ceph/ RENAME ceph.conf.example)

install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/src/ceph_common.sh
  DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/ceph)

install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/src/ceph-create-keys
  ${CMAKE_SOURCE_DIR}/src/ceph-disk
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-activate
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-prepare
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-udev
  DESTINATION sbin)

if(WITH_LIBCEPHFS)
  set(libclient_srcs
    client/Client.cc
    client/Dentry.cc
    client/Inode.cc
    client/MetaRequest.cc
    client/ClientSnapRealm.cc
    client/MetaSession.cc
    client/Trace.cc)
  add_library(client ${libclient_srcs})
  target_link_libraries(client osdc mds ${LIBEDIT_LIBS})
  set(libcephfs_srcs libcephfs.cc)
  add_library(cephfs SHARED ${libcephfs_srcs})
  target_link_libraries(cephfs client global)
  install(TARGETS cephfs DESTINATION lib)
  install(DIRECTORY
    "${CMAKE_SOURCE_DIR}/src/include/cephfs"
    DESTINATION include)
  set(ceph_syn_srcs
    ceph_syn.cc
    client/SyntheticClient.cc)
  add_executable(ceph-syn ${ceph_syn_srcs})
  target_link_libraries(ceph-syn client global)

  set(mount_ceph_srcs
    mount/mount.ceph.c)
  add_executable(mount.ceph ${mount_ceph_srcs}
    $<TARGET_OBJECTS:common_mountcephfs_objs>
    $<TARGET_OBJECTS:rbd_mountcephfs_objs>)
  target_link_libraries(mount.ceph keyutils)

  install(TARGETS ceph-syn DESTINATION bin)
  install(TARGETS mount.ceph DESTINATION sbin)

  if(WITH_FUSE)
    set(ceph_fuse_srcs
      ceph_fuse.cc
      client/fuse_ll.cc)
    add_executable(ceph-fuse ${ceph_fuse_srcs})
    target_link_libraries(ceph-fuse fuse client global)
    install(TARGETS ceph-fuse DESTINATION bin)
  endif(WITH_FUSE)
endif(WITH_LIBCEPHFS)

if(${WITH_RBD})
  set(librbd_srcs
    krbd.cc
    common/ContextCompletion.cc
    librbd/AioCompletion.cc
    librbd/AioRequest.cc
    librbd/AsyncFlattenRequest.cc
    librbd/AsyncObjectThrottle.cc
    librbd/AsyncOperation.cc
    librbd/AsyncRequest.cc
    librbd/AsyncResizeRequest.cc
    librbd/AsyncTrimRequest.cc
    librbd/CopyupRequest.cc
    librbd/ImageCtx.cc
    librbd/ImageWatcher.cc
    librbd/WatchNotifyTypes.cc
    librbd/internal.cc
    librbd/librbd.cc
    librbd/LibrbdWriteback.cc
    librbd/ObjectMap.cc)
  add_library(librbd ${CEPH_SHARED} ${librbd_srcs}
    $<TARGET_OBJECTS:osdc_rbd_objs>
    $<TARGET_OBJECTS:common_util_obj>)
  target_link_libraries(librbd PRIVATE librados common cls_lock_client cls_rbd_client
    ${CMAKE_DL_LIBS})
  if(${ENABLE_SHARED})
    set_target_properties(librbd PROPERTIES VERSION "1.0.0" SOVERSION "1"
      OUTPUT_NAME rbd)
  endif(${ENABLE_SHARED})
  install(TARGETS librados librbd DESTINATION lib)
  set(rbd_srcs
    rbd.cc common/TextTable.cc)
  set(rbd_mountcephfs_files
    common/secret.c)
  add_library(rbd_mountcephfs_objs OBJECT ${rbd_mountcephfs_files})
  add_executable(rbd ${rbd_srcs} $<TARGET_OBJECTS:common_util_obj>
    $<TARGET_OBJECTS:rbd_mountcephfs_objs>
    $<TARGET_OBJECTS:heap_profiler_objs>)
  set_target_properties(rbd PROPERTIES OUTPUT_NAME rbd)
  target_link_libraries(rbd global librbd librados common keyutils udev
    ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})
  install(TARGETS rbd DESTINATION bin)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/src/ceph-rbdnamer DESTINATION bin)
endif(${WITH_RBD})

# RadosGW
if(${WITH_KVS})
  set(kvs_srcs
    key_value_store/cls_kvs.cc)
  add_library(cls_kvs SHARED ${kvs_srcs})
  set_target_properties(cls_kvs PROPERTIES VERSION "1.0.0" SOVERSION "1")
  install(TARGETS cls_kvs DESTINATION lib/rados-classes)
endif(${WITH_KVS})

if(${WITH_RADOSGW})
  set(rgw_a_srcs
    rgw/librgw.cc
    rgw/rgw_acl.cc
    rgw/rgw_acl_s3.cc
    rgw/rgw_acl_swift.cc
    rgw/rgw_client_io.cc
    rgw/rgw_fcgi.cc
    rgw/rgw_xml.cc
    rgw/rgw_usage.cc
    rgw/rgw_json_enc.cc
    rgw/rgw_user.cc
    rgw/rgw_bucket.cc
    rgw/rgw_tools.cc
    rgw/rgw_rados.cc
    rgw/rgw_http_client.cc
    rgw/rgw_rest_client.cc
    rgw/rgw_rest_conn.cc
    rgw/rgw_op.cc
    rgw/rgw_common.cc
    rgw/rgw_cache.cc
    rgw/rgw_formats.cc
    rgw/rgw_log.cc
    rgw/rgw_multi.cc
    rgw/rgw_policy_s3.cc
    rgw/rgw_gc.cc
    rgw/rgw_multi_del.cc
    rgw/rgw_env.cc
    rgw/rgw_cors.cc
    rgw/rgw_cors_s3.cc
    rgw/rgw_auth_s3.cc
    rgw/rgw_metadata.cc
    rgw/rgw_replica_log.cc
    rgw/rgw_keystone.cc
    rgw/rgw_quota.cc
    rgw/rgw_dencoder.cc)

  add_library(rgw_a STATIC ${rgw_a_srcs})

  include_directories("${CMAKE_SOURCE_DIR}/src/civetweb/include")

  set(radosgw_srcs
    rgw/rgw_resolve.cc
    rgw/rgw_rest.cc
    rgw/rgw_rest_swift.cc
    rgw/rgw_rest_s3.cc
    rgw/rgw_rest_usage.cc
    rgw/rgw_rest_user.cc
    rgw/rgw_rest_bucket.cc
    rgw/rgw_rest_metadata.cc
    rgw/rgw_replica_log.cc
    rgw/rgw_rest_log.cc
    rgw/rgw_rest_opstate.cc
    rgw/rgw_rest_replica_log.cc
    rgw/rgw_rest_config.cc
    rgw/rgw_http_client.cc
    rgw/rgw_swift.cc
    rgw/rgw_swift_auth.cc
    rgw/rgw_loadgen.cc
    rgw/rgw_civetweb.cc
    rgw/rgw_civetweb_log.cc
    civetweb/src/civetweb.c
    rgw/rgw_main.cc)

  set(radosgw_admin_srcs
    rgw/rgw_admin.cc
    rgw/rgw_orphan.cc)

  add_executable(radosgw ${radosgw_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
  target_link_libraries(radosgw rgw_a librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_version_client
    cls_replica_log_client cls_user_client
    curl expat global fcgi resolv ${TCMALLOC_LIBS})
  install(TARGETS radosgw DESTINATION bin)

  add_executable(radosgw-admin ${radosgw_admin_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
  target_link_libraries(radosgw-admin rgw_a librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_version_client
    cls_replica_log_client cls_user_client
    curl expat global fcgi resolv ${TCMALLOC_LIBS})
  install(TARGETS radosgw-admin DESTINATION bin)
endif(${WITH_RADOSGW})
