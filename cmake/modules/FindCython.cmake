#
# Cython
#

SET(Cython${PYTHON_VERSION}_FOUND FALSE)
# Try to run Cython, to make sure it works:
execute_process(
    COMMAND ${PYTHON${PYTHON_VERSION}_EXECUTABLE} -m cython --version
    RESULT_VARIABLE CYTHON_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
    )
if (CYTHON_RESULT EQUAL 0)
    SET(Cython${PYTHON_VERSION}_FOUND TRUE)
endif (CYTHON_RESULT EQUAL 0)


IF (Cython${PYTHON_VERSION}_FOUND)
    IF (NOT Cython_FIND_QUIETLY)
        MESSAGE(STATUS "Found cython${PYTHON_VERSION}")
    ENDIF (NOT Cython_FIND_QUIETLY)
ELSE (Cython${PYTHON_VERSION}_FOUND)
    IF (Cython_FIND_REQUIRED)
        MESSAGE(FATAL_ERROR "Could not find cython${PYTHON_VERSION}. Please install Cython.")
    ENDIF (Cython_FIND_REQUIRED)
ENDIF (Cython${PYTHON_VERSION}_FOUND)
