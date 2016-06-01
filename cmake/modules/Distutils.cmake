include(CMakeParseArguments)

function(distutils_install_module name)
  set(py_srcs setup.py README.rst requirements.txt test-requirements.txt ${name})
  foreach(src ${py_srcs})
    list(APPEND py_clone ${CMAKE_CURRENT_BINARY_DIR}/${src})
    add_custom_command(
      OUTPUT ${src}
      DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src}
      COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/${src} ${src})
  endforeach()
  add_custom_target(${name}-clone ALL
    DEPENDS ${py_clone})
  cmake_parse_arguments(DU "" INSTALL_SCRIPT "" ${ARGN})
  install(CODE "
    set(options)
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        list(APPEND options --install-layout=deb)
      else()
        list(APPEND options --prefix=/usr)
      endif()
      list(APPEND options --root=\$ENV{DESTDIR})
      if(NOT \"${DU_INSTALL_SCRIPT}\" STREQUAL \"\")
        list(APPEND options --install-script=${DU_INSTALL_SCRIPT})
      endif()
    endif()
    execute_process(
    COMMAND ${PYTHON_EXECUTABLE}
        setup.py install \${options}
    WORKING_DIRECTORY \"${CMAKE_CURRENT_BINARY_DIR}\")")
endfunction(distutils_install_module)

function(distutils_add_cython_module name src)
  get_property(compiler_launcher GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
  get_property(link_launcher GLOBAL PROPERTY RULE_LAUNCH_LINK)
  set(PY_CC \"${compiler_launcher} ${CMAKE_C_COMPILER}\")
  set(PY_CXX \"${compiler_launcher} ${CMAKE_CXX_COMPILER}\")
  set(PY_LDSHARED \"${link_launcher} ${CMAKE_C_COMPILER} -shared\")
  add_custom_target(${name} ALL
    COMMAND
    env
    CC=${PY_CC}
    CXX=${PY_CXX}
    LDSHARED=${PY_LDSHARED}
    OPT=\"-DNDEBUG -g -fwrapv -O2 -Wall\"
    LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CYTHON_BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}
    CFLAGS=\"-iquote ${CMAKE_SOURCE_DIR}/src/include\"
    ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/setup.py build --build-base ${CYTHON_MODULE_DIR} --verbose
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${src})
endfunction(distutils_add_cython_module)

function(distutils_install_cython_module name)
  install(CODE "
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        set(options --install-layout=deb)
      else()
        set(options --prefix=/usr)
      endif()
      set(root --root=\$ENV{DESTDIR})
    else()
      set(options \"--prefix=${CMAKE_INSTALL_PREFIX}\")
      set(root --root=/)
    endif()
    execute_process(
       COMMAND env
           CYTHON_BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}
           CFLAGS=\"-iquote ${CMAKE_SOURCE_DIR}/src/include\"
           ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/setup.py
           build --build-base ${CYTHON_MODULE_DIR} --verbose
           install \${options} \${root} --verbose
           --single-version-externally-managed --record /dev/null
       WORKING_DIRECTORY \"${CMAKE_CURRENT_SOURCE_DIR}\"
       RESULT_VARIABLE install_res)
    if(NOT \"\${install_res}\" STREQUAL 0)
      message(FATAL_ERROR \"Failed to build and install ${name} python module\")
    endif()
  ")
endfunction(distutils_install_cython_module)
