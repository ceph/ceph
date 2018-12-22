set(libos_srcs
  ObjectStore.cc
  Transaction.cc
  filestore/chain_xattr.cc
  filestore/BtrfsFileStoreBackend.cc
  filestore/DBObjectMap.cc
  filestore/FileJournal.cc
  filestore/FileStore.cc
  filestore/JournalThrottle.cc
  filestore/GenericFileStoreBackend.cc
  filestore/JournalingObjectStore.cc
  filestore/HashIndex.cc
  filestore/IndexManager.cc
  filestore/LFNIndex.cc
  filestore/WBThrottle.cc
  memstore/MemStore.cc
  kstore/KStore.cc
  kstore/kstore_types.cc
  fs/FS.cc)

if(WITH_BLUESTORE)
  list(APPEND libos_srcs
    bluestore/Allocator.cc
    bluestore/BitmapFreelistManager.cc
    bluestore/BlockDevice.cc
    bluestore/BlueFS.cc
    bluestore/bluefs_types.cc
    bluestore/BlueRocksEnv.cc
    bluestore/BlueStore.cc
    bluestore/bluestore_types.cc
    bluestore/fastbmap_allocator_impl.cc
    bluestore/FreelistManager.cc
    bluestore/StupidAllocator.cc
    bluestore/BitmapAllocator.cc
  )
endif(WITH_BLUESTORE)

if(HAVE_LIBAIO OR HAVE_POSIXAIO)
  list(APPEND libos_srcs
    bluestore/KernelDevice.cc
    bluestore/aio.cc)
endif()

if(WITH_FUSE)
  list(APPEND libos_srcs
    FuseStore.cc)
endif(WITH_FUSE)

if(WITH_PMEM)
  list(APPEND libos_srcs
    bluestore/PMEMDevice.cc)
endif(WITH_PMEM)

if(HAVE_LIBXFS)
  list(APPEND libos_srcs
    filestore/XfsFileStoreBackend.cc
    fs/XFS.cc)
endif()

if(HAVE_LIBZFS)
  add_library(os_zfs_objs OBJECT
    filestore/ZFSFileStoreBackend.cc
    fs/ZFS.cc)
  target_include_directories(os_zfs_objs SYSTEM PRIVATE
    ${ZFS_INCLUDE_DIRS})
  list(APPEND libos_srcs $<TARGET_OBJECTS:os_zfs_objs>)
endif()

if(WITH_SPDK)
  list(APPEND libos_srcs
    bluestore/NVMEDevice.cc)
endif()

add_library(os STATIC ${libos_srcs}
  $<TARGET_OBJECTS:kv_objs>)

target_link_libraries(os heap_profiler)

if(WITH_BLUEFS)
  add_library(bluefs SHARED 
    bluestore/BlueRocksEnv.cc)
  target_include_directories(bluefs SYSTEM PUBLIC
    $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(bluefs global)
  install(TARGETS bluefs DESTINATION lib)
endif(WITH_BLUEFS)

if(HAVE_LIBAIO)
  target_link_libraries(os ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)

if(WITH_FUSE)
  target_include_directories(os SYSTEM PRIVATE ${FUSE_INCLUDE_DIRS})
  target_link_libraries(os ${FUSE_LIBRARIES})
endif()

if(HAVE_LIBZFS)
  target_link_libraries(os ${ZFS_LIBRARIES})
endif()

if(WITH_SPDK)
  target_link_libraries(os
    ${SPDK_LIBRARIES})
endif()

if(WITH_LTTNG)
  add_dependencies(os objectstore-tp)
endif()

target_link_libraries(os kv)

add_dependencies(os compressor_plugins)
add_dependencies(os crypto_plugins)


if(WITH_BLUESTORE)
  add_executable(ceph-bluestore-tool
    bluestore/bluestore_tool.cc)
  target_link_libraries(ceph-bluestore-tool
    os global)
  install(TARGETS ceph-bluestore-tool
    DESTINATION bin)
endif()

if(WITH_PMEM)
  include(ExternalProject)
  ExternalProject_Add(nvml_ext
    DOWNLOAD_DIR ${CMAKE_BINARY_DIR}/src/
    GIT_REPOSITORY "https://github.com/ceph/nvml.git"
    GIT_TAG "dd622819dd4ee97d3920f913c70be"
    SOURCE_DIR ${CMAKE_BINARY_DIR}/src/nvml
    CONFIGURE_COMMAND ""
    BUILD_COMMAND $(MAKE)
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND "true")

  ExternalProject_Add_Step(nvml_ext forcebuild
    DEPENDEES configure
    DEPENDERS build
    COMMAND "true"
    ALWAYS 1)
  add_library(pmem STATIC IMPORTED GLOBAL)
  add_dependencies(pmem nvml_ext)
  set_target_properties(pmem PROPERTIES
    IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/src/nvml/src/nondebug/libpmem.a"
    INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  target_link_libraries(os pmem)
  target_include_directories(os SYSTEM PRIVATE "${CMAKE_BINARY_DIR}/src/nvml/src/include")
endif(WITH_PMEM)

if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_dependencies(os eventtrace_tp)
endif()
