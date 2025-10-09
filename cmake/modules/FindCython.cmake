#
# Cython
#

# Try to run Cython, to make sure it works:
execute_process(
  COMMAND ${Python3_EXECUTABLE} -m cython --version
  RESULT_VARIABLE cython_result
  ERROR_VARIABLE cython_output)
if(cython_result EQUAL 0)
  string(REGEX REPLACE "^Cython version ([0-9]+\\.[0-9]+).*" "\\1" CYTHON_VERSION "${cython_output}")
else()
  message(SEND_ERROR "Could not find cython${PYTHON_VERSION}: ${cython_output}")
endif()
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Cython${PYTHON_VERSION} DEFAULT_MSG CYTHON_VERSION)
