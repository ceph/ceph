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
target_link_libraries(cephfs-journal-tool librados mds osdc global
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
  if(NOT WITH_PYTHON3)
    message(SEND_ERROR "Please enable WITH_PYTHON3 for cephfs-shell")
  endif()
  set(PYTHON_VERSION 3)
  include(Distutils)
  distutils_install_module(cephfs-shell)
endif()
