get_filename_component(rgw_gc_test_dir "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
get_filename_component(rgw_gc_source_root "${rgw_gc_test_dir}/../../.." ABSOLUTE)
add_executable(unittest_cls_rgw_queued_gc_client
  "${rgw_gc_test_dir}/datum_client.cc"
  "${rgw_gc_test_dir}/../cls_rgw/bi_wire_main.cc")
target_link_libraries(unittest_cls_rgw_queued_gc_client PRIVATE
  cls_rgw_gc_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_queued_gc_client PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_queued_gc_client COMMAND unittest_cls_rgw_queued_gc_client)

# Compile the actual RGW wrapper against a controlled shared-queue boundary.
# The production plugin is built separately with the unchanged queue source.
add_executable(unittest_cls_rgw_queued_gc_module
  "${rgw_gc_test_dir}/datum_module.cc"
  "${rgw_gc_test_dir}/../cls_rgw/bi_wire_main.cc")
target_compile_definitions(unittest_cls_rgw_queued_gc_module PRIVATE
  RGW_GC_TEST_IMPLEMENTATION="${rgw_gc_source_root}/src/cls/rgw_gc/cls_rgw_gc.cc")
target_link_libraries(unittest_cls_rgw_queued_gc_module PRIVATE
  cls_rgw_gc_client Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_queued_gc_module PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_queued_gc_module COMMAND unittest_cls_rgw_queued_gc_module)
