set(libcrimsonos_srcs
  memstore/MemStore.cc
  Finisher.cc
  ${PROJECT_SOURCE_DIR}/src/os/ObjectStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/chain_xattr.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/BtrfsFileStoreBackend.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/DBObjectMap.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/FileJournal.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/FileStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/JournalThrottle.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/GenericFileStoreBackend.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/JournalingObjectStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/HashIndex.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/IndexManager.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/LFNIndex.cc
  ${PROJECT_SOURCE_DIR}/src/os/filestore/WBThrottle.cc
  ${PROJECT_SOURCE_DIR}/src/os/kstore/KStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/kstore/kstore_types.cc
  ${PROJECT_SOURCE_DIR}/src/os/fs/FS.cc)

if(WITH_BLUESTORE)
  list(APPEND libcrimsonos_srcs
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/Allocator.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlockDevice.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueFS.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluefs_types.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueRocksEnv.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueStore.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluestore_types.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/fastbmap_allocator_impl.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/FreelistManager.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/StupidAllocator.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc
  )
endif(WITH_BLUESTORE)

if(HAVE_LIBAIO)
  list(APPEND libcrimsonos_srcs
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/KernelDevice.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/aio.cc)
endif()

if(WITH_SPDK)
  list(APPEND libcrimsonos_srcs
     ${PROJECT_SOURCE_DIR}/src/os/bluestore/NVMEDevice.cc)
endif()


if(HAVE_LIBXFS)
  list(APPEND libcrimsonos_srcs
    ${PROJECT_SOURCE_DIR}/src/os/filestore/XfsFileStoreBackend.cc
    ${PROJECT_SOURCE_DIR}/src/os/fs/XFS.cc)
endif()

add_library(crimsonos STATIC ${libcrimsonos_srcs}
  $<TARGET_OBJECTS:kv_objs>)
target_link_libraries(crimsonos heap_profiler seastar)

if(HAVE_LIBAIO)
  target_link_libraries(crimsonos ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)
if(WITH_SPDK)
  target_link_libraries(crimsonos
    ${SPDK_LIBRARIES})
endif()

target_link_libraries(crimsonos kv)




