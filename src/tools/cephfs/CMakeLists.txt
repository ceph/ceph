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
if(WITH_AUDITLOGGING)
  list(APPEND cephfs_journal_tool_srcs ToolsAuditLogger.cc)
endif()
add_executable(cephfs-journal-tool ${cephfs_journal_tool_srcs})
target_link_libraries(cephfs-journal-tool
  legacy-option-headers
  ceph-common librados mds osdc global Boost::boost
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)
if(WITH_AUDITLOGGING)
  target_link_libraries(cephfs-journal-tool audit_db cephsqlite SQLite3::SQLite3)
endif()

set(cephfs-meta-injection_srcs
  cephfs-meta-injection.cc
  MetaTool.cc
  RoleSelector.cc
  MDSUtility.cc)
if(WITH_AUDITLOGGING)
  list(APPEND cephfs-meta-injection_srcs ToolsAuditLogger.cc)
endif()
add_executable(cephfs-meta-injection ${cephfs-meta-injection_srcs})
target_link_libraries(cephfs-meta-injection
  legacy-option-headers
  ceph-common librados mds osdc global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
if(WITH_AUDITLOGGING)
  target_link_libraries(cephfs-meta-injection audit_db cephsqlite SQLite3::SQLite3)
endif()

set(cephfs_table_tool_srcs
  cephfs-table-tool.cc
  TableTool.cc
  ProgressTracker.cc
  RoleSelector.cc
  MDSUtility.cc)
if(WITH_AUDITLOGGING)
  list(APPEND cephfs_table_tool_srcs ToolsAuditLogger.cc)
endif()
add_executable(cephfs-table-tool ${cephfs_table_tool_srcs})
target_link_libraries(cephfs-table-tool ceph-common librados mds osdc global Boost::boost
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)
if(WITH_AUDITLOGGING)
  target_link_libraries(cephfs-table-tool audit_db cephsqlite SQLite3::SQLite3)
endif()

set(cephfs_data_scan_srcs
  cephfs-data-scan.cc
  DataScan.cc
  ProgressTracker.cc
  RoleSelector.cc
  PgFiles.cc
  MDSUtility.cc)
if(WITH_AUDITLOGGING)
  list(APPEND cephfs_data_scan_srcs ToolsAuditLogger.cc)
endif()
add_executable(cephfs-data-scan ${cephfs_data_scan_srcs})
target_link_libraries(cephfs-data-scan ceph-common librados cephfs mds osdc global Boost::boost
  cls_cephfs_client
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)
if(WITH_AUDITLOGGING)
  target_link_libraries(cephfs-data-scan audit_db cephsqlite SQLite3::SQLite3)
endif()

set(cephfs_tool_srcs
  cephfs-tool.cc)
add_executable(cephfs-tool ${cephfs_tool_srcs})
target_include_directories(cephfs-tool PRIVATE ${PROJECT_SOURCE_DIR}/src/include)
target_link_libraries(cephfs-tool ceph-common librados cephfs osdc global uring::uring
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} Boost::filesystem)

set(auditman_srcs
  auditman.cc)
if(WITH_AUDITLOGGING)
  add_executable(auditman ${auditman_srcs})
  target_link_libraries(auditman
    ceph-common
    librados
    global
    audit_db
    cephsqlite
    SQLite3::SQLite3
    Boost::program_options
    fmt::fmt)
endif()

install(TARGETS
  cephfs-journal-tool
  cephfs-table-tool
  cephfs-data-scan
  cephfs-tool
  DESTINATION bin)

if(WITH_AUDITLOGGING)
  install(TARGETS auditman DESTINATION bin)
endif()

option(WITH_CEPHFS_SHELL "install cephfs-shell" OFF)
if(WITH_CEPHFS_SHELL)
  add_subdirectory(shell)
endif()

option(WITH_CEPHFS_TOP "install cephfs-top utility" ON)
if(WITH_CEPHFS_TOP)
  add_subdirectory(top)
endif()
