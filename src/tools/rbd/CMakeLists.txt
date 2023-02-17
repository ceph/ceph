set(CURSES_NEED_NCURSES TRUE)
# libcurses may not be available on some platforms (e.g. Windows).
find_package(Curses)

set(rbd_srcs
  rbd.cc
  ArgumentTypes.cc
  IndentStream.cc
  MirrorDaemonServiceInfo.cc
  OptionPrinter.cc
  Schedule.cc
  Shell.cc
  Utils.cc
  action/Bench.cc
  action/Children.cc
  action/Clone.cc
  action/Config.cc
  action/Copy.cc
  action/Create.cc
  action/Device.cc
  action/Diff.cc
  action/DiskUsage.cc
  action/Encryption.cc
  action/Export.cc
  action/Feature.cc
  action/Flatten.cc
  action/Ggate.cc
  action/Group.cc
  action/ImageMeta.cc
  action/Import.cc
  action/Info.cc
  action/Journal.cc
  action/Kernel.cc
  action/List.cc
  action/Lock.cc
  action/MergeDiff.cc
  action/Migration.cc
  action/MirrorImage.cc
  action/MirrorPool.cc
  action/MirrorSnapshotSchedule.cc
  action/Namespace.cc
  action/Nbd.cc
  action/ObjectMap.cc
  action/Perf.cc
  action/PersistentCache.cc
  action/Pool.cc
  action/Remove.cc
  action/Rename.cc
  action/Resize.cc
  action/Snap.cc
  action/Sparsify.cc
  action/Status.cc
  action/TrashPurgeSchedule.cc
  action/Trash.cc
  action/Watch.cc
  action/Wnbd.cc)

add_executable(rbd ${rbd_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
set_target_properties(rbd PROPERTIES OUTPUT_NAME rbd)
target_link_libraries(rbd
  cls_journal_client
  cls_rbd_client
  rbd_types
  librbd
  journal
  libneorados
  librados
  ceph-common global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
if(CURSES_FOUND)
  target_compile_definitions(rbd PRIVATE HAVE_CURSES)
  target_link_libraries(rbd ${CURSES_LIBRARIES})
endif()
if(WITH_KRBD)
  target_link_libraries(rbd 
    krbd)
endif()

install(TARGETS rbd DESTINATION bin)
