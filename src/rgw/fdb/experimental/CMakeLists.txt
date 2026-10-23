if(LIBFDB_EXPERIMENTAL_CONCURRENCY STREQUAL "OFF")
 return()
endif()

if(LIBFDB_EXPERIMENTAL_CONCURRENCY STREQUAL "NVIDIA_STDEXEC" AND NOT TARGET STDEXEC::stdexec)
 # Fetch only the headers; stdexec's own project requires a newer CMake.
 CPMAddPackage(
  NAME stdexec
  GITHUB_REPOSITORY NVIDIA/stdexec
  GIT_TAG 6d7ad689f4d4831c5136e4abe1c601f9a3b64e43
  DOWNLOAD_ONLY YES)

 add_library(ceph_stdexec INTERFACE)
 add_library(STDEXEC::stdexec ALIAS ceph_stdexec)
 target_include_directories(ceph_stdexec SYSTEM INTERFACE "${stdexec_SOURCE_DIR}/include")
 target_compile_features(ceph_stdexec INTERFACE cxx_std_20)
 target_compile_options(ceph_stdexec INTERFACE
  $<$<COMPILE_LANG_AND_ID:CXX,GNU>:-fcoroutines>
  $<$<COMPILE_LANG_AND_ID:CXX,GNU>:-fconcepts-diagnostics-depth=10>)
 target_link_libraries(ceph_stdexec INTERFACE Threads::Threads)
endif()

message(STATUS "Experimental libfdb concurrency: ${LIBFDB_EXPERIMENTAL_CONCURRENCY}")

add_library(rgw_fdb_execution INTERFACE)
target_compile_features(rgw_fdb_execution INTERFACE cxx_std_23)
target_compile_definitions(rgw_fdb_execution INTERFACE
 CEPH_LIBFDB_EXPERIMENTAL_${LIBFDB_EXPERIMENTAL_CONCURRENCY})
target_link_libraries(rgw_fdb_execution INTERFACE rgw_fdb)

if(LIBFDB_EXPERIMENTAL_CONCURRENCY STREQUAL "NVIDIA_STDEXEC")
 target_link_libraries(rgw_fdb_execution INTERFACE STDEXEC::stdexec)
endif()
