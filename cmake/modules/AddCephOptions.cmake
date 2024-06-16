function(add_ceph_options)
  # workaround for
  # https://gitlab.kitware.com/cmake/cmake/-/issues/18399
  if(CMAKE_VERSION VERSION_LESS "3.20")
    # see src/common/options/CMakeLists.txt
    set(all_options
      global
      cephfs-mirror
      crimson
      mgr
      mds
      mds-client
      mon
      osd
      rbd
      rbd-mirror
      immutable-object-cache
      ceph-exporter
      rgw)
    foreach(name ${all_options})
      message(STATUS "${PROJECT_BINARY_DIR}/include/${name}_legacy_options.h")
      set_property(SOURCE
        ${PROJECT_BINARY_DIR}/include/${name}_legacy_options.h
        PROPERTY GENERATED 1)
    endforeach()
  endif()
endfunction()
