set(rbd_srcs
  rbd.cc
  ArgumentTypes.cc
  IndentStream.cc
  OptionPrinter.cc
  Shell.cc
  Utils.cc
  action/Bench.cc
  action/Children.cc
  action/Clone.cc
  action/Copy.cc
  action/Create.cc
  action/Diff.cc
  action/DiskUsage.cc
  action/Export.cc
  action/ExportDiff.cc
  action/Feature.cc
  action/Flatten.cc
  action/Group.cc
  action/ImageMeta.cc
  action/Import.cc
  action/ImportDiff.cc
  action/Info.cc
  action/Journal.cc
  action/Kernel.cc
  action/List.cc
  action/Lock.cc
  action/MergeDiff.cc
  action/MirrorPool.cc
  action/MirrorImage.cc
  action/Nbd.cc
  action/ObjectMap.cc
  action/Remove.cc
  action/Rename.cc
  action/Resize.cc
  action/Snap.cc
  action/Status.cc
  action/Watch.cc)
add_executable(rbd ${rbd_srcs}
  $<TARGET_OBJECTS:common_util_obj>
  $<TARGET_OBJECTS:parse_secret_objs>
  $<TARGET_OBJECTS:common_texttable_obj>
  $<TARGET_OBJECTS:krbd_objs>)
set_target_properties(rbd PROPERTIES OUTPUT_NAME rbd)
target_link_libraries(rbd librbd librados
  cls_journal_client cls_rbd_client
  rbd_types
  journal
  common global
  keyutils udev
  ${Boost_REGEX_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
install(TARGETS rbd DESTINATION bin)
