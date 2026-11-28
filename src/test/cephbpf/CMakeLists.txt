# Unit tests for CephBPF (radostrace) struct compatibility
#
# These tests verify that data structures used by radostrace BPF tracing
# still have the expected field names and hierarchy. The tests run regardless
# of WITH_EBPF_TRACE to catch struct changes that would break BPF probes.

add_executable(unittest_radostrace_struct_compat
  test_radostrace_struct_compat.cc
)

target_include_directories(unittest_radostrace_struct_compat PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(unittest_radostrace_struct_compat
  osdc
  global
  GTest::GTest
  GTest::Main
)

add_ceph_unittest(unittest_radostrace_struct_compat)
