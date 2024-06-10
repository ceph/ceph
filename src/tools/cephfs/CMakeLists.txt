set(cephfs_journal_tool_srcs
  cephfs-journal-tool.cc
  JournalTool.cc
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
  librados mds osdc global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

set(cephfs-meta-injection_srcs
  cephfs-meta-injection.cc
  MetaTool.cc
  RoleSelector.cc
  MDSUtility.cc)
add_executable(cephfs-meta-injection ${cephfs-meta-injection_srcs})
target_link_libraries(cephfs-meta-injection
  legacy-option-headers
  librados mds osdc global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

set(cephfs_table_tool_srcs
  cephfs-table-tool.cc
  TableTool.cc
  RoleSelector.cc
  MDSUtility.cc)
add_executable(cephfs-table-tool ${cephfs_table_tool_srcs})
target_link_libraries(cephfs-table-tool librados mds osdc global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

set(cephfs_data_scan_srcs
  cephfs-data-scan.cc
  DataScan.cc
  RoleSelector.cc
  PgFiles.cc
  MDSUtility.cc)
add_executable(cephfs-data-scan ${cephfs_data_scan_srcs})
target_link_libraries(cephfs-data-scan librados cephfs mds osdc global
  cls_cephfs_client
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

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
