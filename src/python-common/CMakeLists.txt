if(WITH_PYTHON3)
  list(APPEND py_vers 3)
endif()
if(WITH_PYTHON2)
  list(APPEND py_vers 2)
endif()

include(Distutils)
foreach(python_version ${py_vers})
  distutils_install_module(ceph
    PYTHON_VERSION ${python_version})
endforeach()

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(python-common)
endif()
