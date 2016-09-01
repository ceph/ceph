if(HAVE_LIBXFS)
  set(libos_xfs_srcs
    filestore/XfsFileStoreBackend.cc
    fs/XFS.cc)
endif(HAVE_LIBXFS)

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
  filestore/ZFSFileStoreBackend.cc
  memstore/MemStore.cc
  kstore/kv.cc
  kstore/KStore.cc
  kstore/kstore_types.cc
  fs/FS.cc
  ${libos_xfs_srcs})

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
    bluestore/ExtentFreelistManager.cc
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

if(WITH_SPDK)
  list(APPEND libos_srcs
    bluestore/NVMEDevice.cc)
endif()

add_library(os STATIC ${libos_srcs} $<TARGET_OBJECTS:kv_objs>)

if(HAVE_LIBAIO)
  target_link_libraries(os ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)

if(WITH_FUSE)
  target_link_libraries(os ${FUSE_LIBRARIES})
endif()

if(WITH_SPDK)
  target_link_libraries(os
    ${SPDK_LIBRARIES}
    ${DPDK_LIBRARIES}
    ${PCIACCESS_LIBRARIES})
  target_include_directories(os
    PRIVATE
    ${DPDK_INCLUDE_DIR}
    ${PCIACCESS_INCLUDE_DIR})
endif(WITH_SPDK)

if(WITH_LTTNG)
  add_dependencies(os objectstore-tp)
endif()

target_link_libraries(os kv)

add_dependencies(os compressor_plugins)

if(HAVE_LIBAIO)
  add_executable(ceph-bluefs-tool
    bluestore/bluefs_tool.cc)
  target_link_libraries(ceph-bluefs-tool
    os global)
  install(TARGETS ceph-bluefs-tool
    DESTINATION bin)
endif()

if(WITH_SPDK)
  add_custom_target(build_spdk
    COMMAND
    $(MAKE) DPDK_INC_DIR=${DPDK_INCLUDE_DIR}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/spdk/lib
    COMMENT "spdk building")
  # TODO: should use add_library(spdk INTERFACE IMPORTED) instead in new cmake,
  # if INTERFACE is supported.
  foreach(lib nvme memory util)
    add_library(spdk_${lib} STATIC IMPORTED)
    add_dependencies(spdk_${lib} build_spdk)
    set_target_properties(spdk_${lib} PROPERTIES
      IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/src/spdk/lib/${lib}/libspdk_${lib}.a"
      INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src/spdk/include")
    list(APPEND SPDK_LIBRARIES spdk_${lib})
  endforeach()
endif(WITH_SPDK)
