set(common_options_srcs build_options.cc)
set(legacy_options_headers)
set(options_yamls)

# to mimic the behavior of file(CONFIGURE ...)
file(GENERATE OUTPUT configure_file.cmake
  CONTENT "configure_file(\${input_file} \${output_file} @ONLY)")
function(file_configure input_file output_file)
  set(cmake_defs
    -D input_file=${input_file}
    -D output_file=${output_file})
  file(STRINGS ${input_file} subvars REGEX "@[^@]+@")
  foreach(line ${subvars})
    string(REGEX REPLACE ".*@([^@]+)@.*" "\\1"
      var "${line}")
    set(value ${${var}})
    list(APPEND cmake_defs -D ${var}=${value})
  endforeach()
  add_custom_command(OUTPUT ${output_file}
    COMMAND ${CMAKE_COMMAND} ${cmake_defs} -P configure_file.cmake
    DEPENDS ${input_file}
    VERBATIM)
endfunction()

function(add_options name)
  set(yaml_in_file ${CMAKE_CURRENT_SOURCE_DIR}/${name}.yaml.in)
  set(yaml_file ${CMAKE_CURRENT_BINARY_DIR}/${name}.yaml)
  file_configure("${yaml_in_file}"
    "${yaml_file}" @ONLY)
  list(APPEND options_yamls ${yaml_file})
  set(options_yamls ${options_yamls} PARENT_SCOPE)
  set(cc_file "${name}_options.cc")
  set(h_file "${PROJECT_BINARY_DIR}/include/${name}_legacy_options.h")
  add_custom_command(PRE_BUILD
    OUTPUT ${cc_file} ${h_file}
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/y2c.py
      --input ${yaml_file}
      --output ${cc_file}
      --legacy ${h_file}
      --name ${name}
      DEPENDS ${yaml_file})
  list(APPEND common_options_srcs ${cc_file})
  set(common_options_srcs ${common_options_srcs} PARENT_SCOPE)
  list(APPEND legacy_options_headers ${h_file})
  set(legacy_options_headers ${legacy_options_headers} PARENT_SCOPE)
endfunction()

set(osd_erasure_code_plugins "jerasure" "lrc")
if(WITH_EC_ISA_PLUGIN)
  list(APPEND osd_erasure_code_plugins "isa")
endif()
string(REPLACE ";" " " osd_erasure_code_plugins "${osd_erasure_code_plugins}")

set(keyring_paths
  "/etc/ceph/$cluster.$name.keyring"
  "/etc/ceph/$cluster.keyring"
  "/etc/ceph/keyring"
  "/etc/ceph/keyring.bin")
if(FREEBSD)
  list(APPEND keyring_paths
    "/usr/local/etc/ceph/$cluster.$name.keyring"
    "/usr/local/etc/ceph/$cluster.keyring"
    "/usr/local/etc/ceph/keyring"
    "/usr/local/etc/ceph/keyring.bin")
endif()
string(REPLACE ";" "," keyring_paths "${keyring_paths}")

set(ms_bind_retry_count 3)
set(ms_bind_retry_delay 5)
if(FREEBSD)
  # FreeBSD does not use SO_REAUSEADDR so allow for a bit more time per default
  set(ms_bind_retry_count 6)
  set(ms_bind_retry_delay 6)
endif()

set(mgr_disabled_modules "")
if(WITH_MGR)
  # https://tracker.ceph.com/issues/45147
  if(Python3_VERSION VERSION_EQUAL 3.8)
    set(mgr_disabled_modules "diskprediction_local")
    message(STATUS "mgr module disabled for ${Python3_VERSION}: ${mgr_disabled_modules}")
  endif()
endif()

add_options(global)
add_options(cephfs-mirror)
add_options(crimson)
add_options(mgr)
add_options(mds)
add_options(mds-client)
add_options(mon)
add_options(osd)
add_options(rbd)
add_options(rbd-mirror)
add_options(immutable-object-cache)
add_options(ceph-exporter)

# if set to empty string, system default luarocks package location (if exist) will be used
set(rgw_luarocks_location "")
if(WITH_RADOSGW_LUA_PACKAGES)
  set(rgw_luarocks_location "/tmp/luarocks")
endif()
add_options(rgw)

add_library(common-options-objs OBJECT
  ${common_options_srcs})
add_library(legacy-option-headers INTERFACE)
target_sources(legacy-option-headers
  PRIVATE
    ${legacy_options_headers})

include(AddCephTest)
add_ceph_test(validate-options
  ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/validate-options.py ${options_yamls})
