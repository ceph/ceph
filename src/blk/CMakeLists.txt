if(WITH_BLUESTORE OR  WITH_RBD_SSD_CACHE)
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

if(WITH_ZBD)
  list(APPEND libblk_srcs
    zoned/HMSMRDevice.cc)
endif()

if(libblk_srcs)
  add_library(blk STATIC ${libblk_srcs})
  target_include_directories(blk PRIVATE "./")
endif()

if(HAVE_LIBAIO)
  target_link_libraries(blk PUBLIC ${AIO_LIBRARIES} extblkdev)
endif(HAVE_LIBAIO)

if(WITH_SPDK)
  target_link_libraries(blk
    PRIVATE spdk::spdk)
endif()

if(WITH_ZBD)
  target_link_libraries(blk PRIVATE ${ZBD_LIBRARIES})
endif()

if(WITH_BLUESTORE_PMEM)
  if(HAVE_LIBDML)
    target_link_libraries(blk PRIVATE dml::dml dml::dmlhl)
  endif()

  target_link_libraries(blk
    PRIVATE pmdk::pmem)
endif()

if(WITH_EVENTTRACE)
  add_dependencies(blk eventtrace_tp)
endif()

if(WITH_LIBURING)
  target_link_libraries(blk PRIVATE uring::uring)
endif()
