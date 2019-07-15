include(CMakeParseArguments)

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
  add_custom_target(${name}-clone ALL
    DEPENDS ${py_clone})
  cmake_parse_arguments(DU "" INSTALL_SCRIPT "" ${ARGN})
  install(CODE "
    set(options --prefix=${CMAKE_INSTALL_PREFIX})
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        list(APPEND options --install-layout=deb)
      endif()
      list(APPEND options --root=\$ENV{DESTDIR})
      if(NOT \"${DU_INSTALL_SCRIPT}\" STREQUAL \"\")
        list(APPEND options --install-script=${DU_INSTALL_SCRIPT})
      endif()
    endif()
    execute_process(
    COMMAND ${PYTHON${PYTHON_VERSION}_EXECUTABLE}
        setup.py install \${options}
    WORKING_DIRECTORY \"${CMAKE_CURRENT_BINARY_DIR}\")")
endfunction(distutils_install_module)

function(distutils_add_cython_module name src)
  get_property(compiler_launcher GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
  get_property(link_launcher GLOBAL PROPERTY RULE_LAUNCH_LINK)
  # This little bit of magic wipes out __Pyx_check_single_interpreter()
  # Note: this is reproduced in distutils_install_cython_module
  list(APPEND cflags -D'void0=dead_function\(void\)')
  list(APPEND cflags -D'__Pyx_check_single_interpreter\(ARG\)=ARG \#\# 0')
  set(PY_CC \"${compiler_launcher} ${CMAKE_C_COMPILER}\")
  set(PY_CXX \"${compiler_launcher} ${CMAKE_CXX_COMPILER}\")
  set(PY_LDSHARED \"${link_launcher} ${CMAKE_C_COMPILER} -shared\")
  add_custom_target(${name} ALL
    COMMAND
    env
    CC=${PY_CC}
    CXX=${PY_CXX}
    LDSHARED=${PY_LDSHARED}
    OPT=\"-DNDEBUG -g -fwrapv -O2 -w\"
    LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CYTHON_BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}
    CEPH_LIBDIR=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CFLAGS=\"-iquote${CMAKE_SOURCE_DIR}/src/include -w\"
    ${PYTHON${PYTHON_VERSION}_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/setup.py
    build --verbose --build-base ${CYTHON_MODULE_DIR}
    --build-platlib ${CYTHON_MODULE_DIR}/lib.${PYTHON${PYTHON_VERSION}_VERSION_MAJOR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${src})
endfunction(distutils_add_cython_module)

function(distutils_install_cython_module name)
  get_property(compiler_launcher GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
  get_property(link_launcher GLOBAL PROPERTY RULE_LAUNCH_LINK)
  set(PY_CC "${compiler_launcher} ${CMAKE_C_COMPILER}")
  set(PY_LDSHARED "${link_launcher} ${CMAKE_C_COMPILER} -shared")
  install(CODE "
    set(ENV{CC} \"${PY_CC}\")
    set(ENV{LDSHARED} \"${PY_LDSHARED}\")
    set(ENV{CPPFLAGS} \"-iquote${CMAKE_SOURCE_DIR}/src/include
                        -D'void0=dead_function\(void\)' \
                        -D'__Pyx_check_single_interpreter\(ARG\)=ARG \#\# 0'\")
    set(ENV{LDFLAGS} \"-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}\")
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
           ${PYTHON${PYTHON_VERSION}_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/setup.py
           build --verbose --build-base ${CYTHON_MODULE_DIR}
           --build-platlib ${CYTHON_MODULE_DIR}/lib.${PYTHON${PYTHON_VERSION}_VERSION_MAJOR}
           build_ext --cython-c-in-temp --build-temp ${CMAKE_CURRENT_BINARY_DIR} --cython-include-dirs ${PROJECT_SOURCE_DIR}/src/pybind/rados
           install \${options} --single-version-externally-managed --record /dev/null
           egg_info --egg-base ${CMAKE_CURRENT_BINARY_DIR}
           --verbose
       WORKING_DIRECTORY \"${CMAKE_CURRENT_SOURCE_DIR}\"
       RESULT_VARIABLE install_res)
    if(NOT \"\${install_res}\" STREQUAL 0)
      message(FATAL_ERROR \"Failed to build and install ${name} python module\")
    endif()
  ")
endfunction(distutils_install_cython_module)
