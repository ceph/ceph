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
