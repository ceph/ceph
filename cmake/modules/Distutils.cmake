function(distutils_install_module name)
  if(DEFINED ENV{DESTDIR})
    get_filename_component(debian_version /etc/debian_version ABSOLUTE)
    if(EXISTS ${debian_version})
      set(options "--install-layout=deb")
    else()
      set(options "--prefix=/usr")
    endif()
  endif()

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
  install(CODE
    "execute_process(COMMAND ${PYTHON_EXECUTABLE} setup.py install ${options} --root=$DESTDIR
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
    set(options --prefix=/usr)
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        set(options --install-layout=deb)
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
