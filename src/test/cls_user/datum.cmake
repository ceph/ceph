get_filename_component(user_test_dir "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
foreach(kind client module)
  add_executable(unittest_cls_user_${kind}
    "${user_test_dir}/datum_${kind}.cc"
    "${user_test_dir}/../cls_rgw/bi_wire_main.cc")
  target_link_libraries(unittest_cls_user_${kind} PRIVATE
    cls_user_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
  set_target_properties(unittest_cls_user_${kind} PROPERTIES
    CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
  add_test(NAME cls_user_${kind} COMMAND unittest_cls_user_${kind})
endforeach()
target_sources(unittest_cls_user_module PRIVATE "${user_test_dir}/../cls_rgw/module_loader.cc")
target_compile_definitions(unittest_cls_user_module PRIVATE TEST_CLS_RGW_MODULE="$<TARGET_FILE:cls_user>")
target_link_libraries(unittest_cls_user_module PRIVATE ${CMAKE_DL_LIBS})
set_target_properties(unittest_cls_user_module PROPERTIES ENABLE_EXPORTS ON)
add_dependencies(unittest_cls_user_module cls_user)
add_executable(unittest_cls_user_wire
  "${user_test_dir}/datum_wire.cc"
  "${user_test_dir}/../cls_rgw/bi_wire_main.cc")
target_link_libraries(unittest_cls_user_wire PRIVATE
  cls_user_client Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_user_wire PROPERTIES
  CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_user_wire COMMAND unittest_cls_user_wire)
