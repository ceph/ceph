if(WITH_BLUESTORE OR WITH_RBD_RWL)
list(APPEND libblk_srcs
  BlockDevice.cc)
endif()

if(HAVE_LIBAIO OR HAVE_POSIXAIO)
  list(APPEND libblk_srcs
    kernel/KernelDevice.cc
    kernel/io_uring.cc
    aio/aio.cc)
endif()

if(WITH_BLUESTORE_PMEM)
  list(APPEND libblk_srcs
    pmem/PMEMDevice.cc)
endif()

if(WITH_SPDK)
  list(APPEND libblk_srcs
    spdk/NVMEDevice.cc)
endif()

if(HAVE_LIBZBC)
  list(APPEND libblk_srcs
    zns/HMSMRDevice.cc)
endif()

add_library(blk ${libblk_srcs})
target_include_directories(blk PRIVATE "./")

if(HAVE_LIBAIO)
  target_link_libraries(blk PUBLIC ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)

if(WITH_SPDK)
  target_link_libraries(blk PRIVATE ${SPDK_LIBRARIES})
endif()

if(HAVE_LIBZBC)
  target_link_libraries(blk PRIVATE ${ZBC_LIBRARIES})
endif()

if(WITH_BLUESTORE_PMEM OR WITH_RBD_RWL)
  target_link_libraries(blk
    PUBLIC pmem::pmemobj
    PRIVATE pmem::pmem)
endif()

if(WITH_EVENTTRACE)
  add_dependencies(blk eventtrace_tp)
endif()

if(WITH_LIBURING)
  if(WITH_SYSTEM_LIBURING)
    find_package(uring REQUIRED)
  else()
    include(Builduring)
    build_uring()
  endif()
  target_link_libraries(blk PRIVATE uring::uring)
endif()
