function(build_fio)
  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  if(ALLOC_LIBS)
    get_target_property(alloc_lib_path
      ${ALLOC_LIBS} IMPORTED_LOCATION)
    get_filename_component(alloc_lib_dir
      ${alloc_lib_path} DIRECTORY)
    get_filename_component(alloc_lib_name
      ${alloc_lib_path} NAME)
    set(FIO_EXTLIBS "EXTLIBS='-L${alloc_lib_dir} -l:${alloc_lib_name}'")
  endif()

  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  set(source_dir ${CMAKE_BINARY_DIR}/src/fio)
  file(MAKE_DIRECTORY ${source_dir})
  ExternalProject_Add(fio_ext
    UPDATE_COMMAND "" # this disables rebuild on each run
    GIT_REPOSITORY "https://github.com/ceph/fio.git"
    GIT_CONFIG advice.detachedHead=false
    GIT_SHALLOW 1
    GIT_TAG "fio-3.27-cxx"
    SOURCE_DIR ${source_dir}
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND <SOURCE_DIR>/configure
    BUILD_COMMAND ${make_cmd} fio EXTFLAGS=-Wno-format-truncation "${FIO_EXTLIBS}"
    INSTALL_COMMAND cp <BINARY_DIR>/fio ${CMAKE_BINARY_DIR}/bin
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  add_library(fio INTERFACE IMPORTED)
  add_dependencies(fio fio_ext)
  set_target_properties(fio PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${source_dir}
    INTERFACE_COMPILE_OPTIONS "-include;${source_dir}/config-host.h;$<$<COMPILE_LANGUAGE:C>:-std=gnu99>")
endfunction()
