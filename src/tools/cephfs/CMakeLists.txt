set(cephfs_journal_tool_srcs
  cephfs-journal-tool.cc
  JournalTool.cc
  ProgressTracker.cc
  JournalFilter.cc
  JournalScanner.cc
  EventOutput.cc
  Dumper.cc
  Resetter.cc
  RoleSelector.cc
  MDSUtility.cc)
add_executable(cephfs-journal-tool ${cephfs_journal_tool_srcs})
target_link_libraries(cephfs-journal-tool
  legacy-option-headers
  ceph-common librados mds osdc global uring::uring
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)

set(cephfs-meta-injection_srcs
  cephfs-meta-injection.cc
  MetaTool.cc
  RoleSelector.cc
  MDSUtility.cc)
add_executable(cephfs-meta-injection ${cephfs-meta-injection_srcs})
target_link_libraries(cephfs-meta-injection
  legacy-option-headers
        ceph-common librados mds osdc global uring::uring
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

set(cephfs_table_tool_srcs
  cephfs-table-tool.cc
  TableTool.cc
  ProgressTracker.cc
  RoleSelector.cc
  MDSUtility.cc)
add_executable(cephfs-table-tool ${cephfs_table_tool_srcs})
target_link_libraries(cephfs-table-tool ceph-common librados mds osdc global uring::uring
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)

set(cephfs_data_scan_srcs
  cephfs-data-scan.cc
  DataScan.cc
  ProgressTracker.cc
  RoleSelector.cc
  PgFiles.cc
  MDSUtility.cc)
add_executable(cephfs-data-scan ${cephfs_data_scan_srcs})
target_link_libraries(cephfs-data-scan ceph-common librados cephfs mds osdc global uring::uring
  cls_cephfs_client
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)

install(TARGETS
  cephfs-journal-tool
  cephfs-table-tool
  cephfs-data-scan
  DESTINATION bin)

option(WITH_CEPHFS_SHELL "install cephfs-shell" OFF)
if(WITH_CEPHFS_SHELL)
  add_subdirectory(shell)
endif()

option(WITH_CEPHFS_TOP "install cephfs-top utility" ON)
if(WITH_CEPHFS_TOP)
  add_subdirectory(top)
endif()
