add_executable(ceph_rgw_jsonparser
  rgw_jsonparser.cc
  rgw_common.cc
  rgw_env.cc
  rgw_json_enc.cc)
target_link_libraries(ceph_rgw_jsonparser
  rgw_a
  global)

add_executable(ceph_rgw_multiparser
  rgw_multiparser.cc)
target_link_libraries(ceph_rgw_multiparser
  rgw_a
  global)

install(TARGETS
  ceph_rgw_jsonparser
  ceph_rgw_multiparser
  DESTINATION bin)
