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
  fs/FS.cc
  fs/aio.cc)

if(HAVE_LIBAIO)
  list(APPEND libos_srcs
    bluestore/Allocator.cc
    bluestore/BitmapFreelistManager.cc
    bluestore/BlockDevice.cc
    bluestore/BlueFS.cc
    bluestore/bluefs_types.cc
    bluestore/BlueRocksEnv.cc
    bluestore/BlueStore.cc
    bluestore/bluestore_types.cc
    bluestore/FreelistManager.cc
    bluestore/KernelDevice.cc
    bluestore/StupidAllocator.cc
    bluestore/BitMapAllocator.cc
    bluestore/BitAllocator.cc
  )
endif(HAVE_LIBAIO)

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
  target_include_directories(os_zfs_objs PRIVATE
    ${ZFS_INCLUDE_DIRS})
  list(APPEND libos_srcs $<TARGET_OBJECTS:os_zfs_objs>)
endif()

if(WITH_SPDK)
  list(APPEND libos_srcs
    bluestore/NVMEDevice.cc)
  ADD_DEFINITIONS(-D__SSE4_2__ -D__SSE4_1__ -D__SSSE3__ -D__SSE3__)
  add_compile_options(-mcrc32 -msse3 -mssse3 -msse4.1 -msse4.2)
endif()

add_library(os STATIC ${libos_srcs} $<TARGET_OBJECTS:kv_objs>)

if(HAVE_LIBAIO)
  target_link_libraries(os ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)

if(WITH_FUSE)
  target_link_libraries(os ${FUSE_LIBRARIES})
endif()

if(WITH_PMEM)
  target_link_libraries(os ${PMEM_LIBRARY})
endif()

if(HAVE_LIBZFS)
  target_link_libraries(os ${ZFS_LIBRARIES})
endif()

if(WITH_SPDK)
  target_link_libraries(os
    ${SPDK_LIBRARIES}
    ${DPDK_LIBRARIES})
  target_include_directories(os
    PRIVATE
    ${DPDK_INCLUDE_DIR})
endif(WITH_SPDK)

if(WITH_LTTNG)
  add_dependencies(os objectstore-tp)
endif()

target_link_libraries(os kv)

add_dependencies(os compressor_plugins)
add_dependencies(os crypto_plugins)


if(HAVE_LIBAIO)
  add_executable(ceph-bluestore-tool
    bluestore/bluestore_tool.cc)
  target_link_libraries(ceph-bluestore-tool
    os global)
  install(TARGETS ceph-bluestore-tool
    DESTINATION bin)
endif()

if(WITH_SPDK)
  include_directories("${CMAKE_SOURCE_DIR}/src/spdk/include")
  add_custom_target(build_spdk
    COMMAND
    $(MAKE) DPDK_INC_DIR=${DPDK_INCLUDE_DIR}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/spdk/lib
    COMMENT "spdk building")
  # TODO: should use add_library(spdk INTERFACE IMPORTED) instead in new cmake,
  # if INTERFACE is supported.
  foreach(lib nvme log env_dpdk util)
    add_library(spdk_${lib} STATIC IMPORTED)
    add_dependencies(spdk_${lib} build_spdk)
    target_link_libraries(os LINK_PRIVATE spdk_${lib})
    set_target_properties(spdk_${lib} PROPERTIES
      IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/src/spdk/build/lib/libspdk_${lib}.a"
      INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src/spdk/include")
    list(APPEND SPDK_LIBRARIES spdk_${lib})
  endforeach()
endif(WITH_SPDK)

if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_dependencies(os eventtrace_tp)
endif()
