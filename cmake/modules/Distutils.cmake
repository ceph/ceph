include(CMakeParseArguments)

# ensure that we are using the exact python version specified by
# 'WITH_PYTHON3', in case some included 3rd party libraries call
# 'find_package(Python3 ...) without specifying the exact version number. if
# the building host happens to have a higher version of python3, that version
# would be picked up instead by find_package(Python3). and that is not want we
# expect.
find_package(Python3 ${WITH_PYTHON3} EXACT
  QUIET
  REQUIRED
  COMPONENTS Interpreter)

function(distutils_install_module name)
  set(py_srcs setup.py README.rst requirements.txt test-requirements.txt bin ${name})
  foreach(src ${py_srcs})
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${src})
      list(APPEND py_clone ${CMAKE_CURRENT_BINARY_DIR}/${src})
      add_custom_command(
        OUTPUT ${src}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src}
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/${src} ${src})
    endif()
  endforeach()
  if(NOT TARGET ${name}-clone)
    add_custom_target(${name}-clone ALL
      DEPENDS ${py_clone})
  endif()
  cmake_parse_arguments(DU "" "INSTALL_SCRIPT" "" ${ARGN})
  install(CODE "
    set(options --prefix=${CMAKE_INSTALL_PREFIX})
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        list(APPEND options --install-layout=deb)
      endif()
      list(APPEND options
        --root=\$ENV{DESTDIR}
        --single-version-externally-managed)
    endif()
    if(NOT \"${DU_INSTALL_SCRIPT}\" STREQUAL \"\")
      list(APPEND options --install-script=${DU_INSTALL_SCRIPT})
    endif()
    execute_process(
    COMMAND ${Python3_EXECUTABLE}
        setup.py install \${options}
    WORKING_DIRECTORY \"${CMAKE_CURRENT_BINARY_DIR}\")")
endfunction(distutils_install_module)

function(distutils_add_cython_module target name src)
  get_property(compiler_launcher GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
  get_property(link_launcher GLOBAL PROPERTY RULE_LAUNCH_LINK)
  # When using ccache, CMAKE_C_COMPILER is ccache executable absolute path
  # and the actual C compiler is CMAKE_C_COMPILER_ARG1.
  # However with a naive
  # set(PY_CC ${compiler_launcher} ${CMAKE_C_COMPILER} ${CMAKE_C_COMPILER_ARG1})
  # distutils tries to execve something like "/usr/bin/cmake gcc" and fails.
  # Removing the leading whitespace from CMAKE_C_COMPILER_ARG1 helps to avoid
  # the failure.
  string(STRIP "${CMAKE_C_COMPILER_ARG1}" c_compiler_arg1)
  string(STRIP "${CMAKE_CXX_COMPILER_ARG1}" cxx_compiler_arg1)
  # Note: no quotes, otherwise distutils will execute "/usr/bin/ccache gcc"
  # CMake's implicit conversion between strings and lists is wonderful, isn't it?
  set(PY_CFLAGS ${COMPILE_OPTIONS})
  cmake_parse_arguments(DU "DISABLE_VTA" "" "" ${ARGN})
  if(DU_DISABLE_VTA AND HAS_VTA)
    list(APPEND PY_CFLAGS -fno-var-tracking-assignments)
  endif()
  list(APPEND PY_CPPFLAGS -iquote${CMAKE_SOURCE_DIR}/src/include -w)
  # This little bit of magic wipes out __Pyx_check_single_interpreter()
  # Note: this is reproduced in distutils_install_cython_module
  list(APPEND PY_CPPFLAGS -D'void0=dead_function\(void\)')
  list(APPEND PY_CPPFLAGS -D'__Pyx_check_single_interpreter\(ARG\)=ARG\#\#0')
  set(PY_CC ${compiler_launcher} ${CMAKE_C_COMPILER} ${c_compiler_arg1})
  set(PY_CXX ${compiler_launcher} ${CMAKE_CXX_COMPILER} ${cxx_compiler_arg1})
  set(PY_LDSHARED ${link_launcher} ${CMAKE_C_COMPILER} ${c_compiler_arg1} "-shared")
  string(REPLACE " " ";" PY_LDFLAGS "${CMAKE_SHARED_LINKER_FLAGS}")
  list(APPEND PY_LDFLAGS -L${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

  execute_process(COMMAND "${Python3_EXECUTABLE}" -c
    "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE ext_suffix
    ERROR_VARIABLE error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "Unable to tell python extension's suffix: ${error}")
  endif()
  set(output_dir "${CYTHON_MODULE_DIR}/lib.3")
  set(setup_py ${CMAKE_CURRENT_SOURCE_DIR}/setup.py)
  if(DEFINED ENV{VERBOSE})
    set(maybe_verbose --verbose)
  endif()
  add_custom_command(
    OUTPUT ${output_dir}/${name}${ext_suffix}
    COMMAND
    env
    CC="${PY_CC}"
    CFLAGS="${PY_CFLAGS}"
    CPPFLAGS="${PY_CPPFLAGS}"
    CXX="${PY_CXX}"
    LDSHARED="${PY_LDSHARED}"
    OPT=\"-DNDEBUG -g -fwrapv -O2 -w\"
    LDFLAGS="${PY_LDFLAGS}"
    CYTHON_BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}
    CEPH_LIBDIR=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    ${Python3_EXECUTABLE} ${setup_py}
    build ${maybe_verbose} --build-base ${CYTHON_MODULE_DIR}
    --build-platlib ${output_dir}
    MAIN_DEPENDENCY ${src}
    DEPENDS ${setup_py}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  add_custom_target(${target} ALL
    DEPENDS ${output_dir}/${name}${ext_suffix})
endfunction(distutils_add_cython_module)

function(distutils_install_cython_module name)
  get_property(compiler_launcher GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
  get_property(link_launcher GLOBAL PROPERTY RULE_LAUNCH_LINK)
  set(PY_CC "${compiler_launcher} ${CMAKE_C_COMPILER}")
  set(PY_LDSHARED "${link_launcher} ${CMAKE_C_COMPILER} -shared")
  cmake_parse_arguments(DU "DISABLE_VTA" "" "" ${ARGN})
  if(DU_DISABLE_VTA AND HAS_VTA)
    set(CFLAG_DISABLE_VTA -fno-var-tracking-assignments)
  endif()
  if(DEFINED ENV{VERBOSE})
    set(maybe_verbose --verbose)
  endif()
  install(CODE "
    set(ENV{CC} \"${PY_CC}\")
    set(ENV{LDSHARED} \"${PY_LDSHARED}\")
    set(ENV{CPPFLAGS} \"-iquote${CMAKE_SOURCE_DIR}/src/include
                        -D'void0=dead_function\(void\)' \
                        -D'__Pyx_check_single_interpreter\(ARG\)=ARG\#\#0' \
                        ${CFLAG_DISABLE_VTA}\")
    set(ENV{LDFLAGS} \"${PY_LDFLAGS}\")
    set(ENV{CYTHON_BUILD_DIR} \"${CMAKE_CURRENT_BINARY_DIR}\")
    set(ENV{CEPH_LIBDIR} \"${CMAKE_LIBRARY_OUTPUT_DIRECTORY}\")

    set(options --prefix=${CMAKE_INSTALL_PREFIX})
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        list(APPEND options --install-layout=deb)
      endif()
      list(APPEND options --root=\$ENV{DESTDIR})
    else()
      list(APPEND options --root=/)
    endif()
    execute_process(
       COMMAND
           ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/setup.py
           build ${maybe_verbose} --build-base ${CYTHON_MODULE_DIR}
           --build-platlib ${CYTHON_MODULE_DIR}/lib.3
           build_ext --cython-c-in-temp --build-temp ${CMAKE_CURRENT_BINARY_DIR} --cython-include-dirs ${PROJECT_SOURCE_DIR}/src/pybind/rados
           install \${options} --single-version-externally-managed --record /dev/null
           egg_info --egg-base ${CMAKE_CURRENT_BINARY_DIR}
           ${maybe_verbose}
       WORKING_DIRECTORY \"${CMAKE_CURRENT_SOURCE_DIR}\"
       RESULT_VARIABLE install_res)
    if(NOT \"\${install_res}\" STREQUAL 0)
      message(FATAL_ERROR \"Failed to build and install ${name} python module\")
    endif()
  ")
endfunction(distutils_install_cython_module)
