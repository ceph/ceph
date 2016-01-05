#
# Cython
#

# This finds the "cython" executable in your PATH, and then in some standard
# paths:
FIND_FILE(CYTHON_BIN cython /usr/bin /usr/local/bin)
SET(CYTHON_FLAGS --cplus --fast-fail)

SET(Cython_FOUND FALSE)
IF (CYTHON_BIN)
    # Try to run Cython, to make sure it works:
    execute_process(
        COMMAND ${CYTHON_BIN} ${CYTHON_FLAGS} ${CMAKE_MODULE_PATH}/cython_test.pyx
        RESULT_VARIABLE CYTHON_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
        )
    if (CYTHON_RESULT EQUAL 0)
        # Only if cython exits with the return code 0, we know that all is ok:
        SET(Cython_FOUND TRUE)
        SET(Cython_Compilation_Failed FALSE)
    else (CYTHON_RESULT EQUAL 0)
        SET(Cython_Compilation_Failed TRUE)
    endif (CYTHON_RESULT EQUAL 0)
ENDIF (CYTHON_BIN)


IF (Cython_FOUND)
        IF (NOT Cython_FIND_QUIETLY)
                MESSAGE(STATUS "Found CYTHON: ${CYTHON_BIN}")
        ENDIF (NOT Cython_FIND_QUIETLY)
ELSE (Cython_FOUND)
        IF (Cython_FIND_REQUIRED)
        if(Cython_Compilation_Failed)
            MESSAGE(STATUS "Found CYTHON: ${CYTHON_BIN}")
        else(Cython_Compilation_Failed)
            MESSAGE(FATAL_ERROR "Could not find Cython. Please install Cython.")
        endif(Cython_Compilation_Failed)
        ENDIF (Cython_FIND_REQUIRED)
ENDIF (Cython_FOUND)


# This allows to link Cython files
# Examples:
# 1) to compile assembly.pyx to assembly.so:
#   CYTHON_ADD_MODULE(assembly)
# 2) to compile assembly.pyx and something.cpp to assembly.so:
#   CYTHON_ADD_MODULE(assembly something.cpp)

if(NOT CYTHON_INCLUDE_DIRECTORIES)
    set(CYTHON_INCLUDE_DIRECTORIES .)
endif(NOT CYTHON_INCLUDE_DIRECTORIES)

# Cythonizes the .pyx files into .cpp file (but doesn't compile it)
macro(CYTHON_ADD_MODULE_PYX name)
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${name}.pxd)
        set(DEPENDS ${name}.pyx ${name}.pxd)
    else(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${name}.pxd)
        set(DEPENDS ${name}.pyx)
    endif(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${name}.pxd)
    # Allow the user to specify dependencies as optional arguments
    set(DEPENDS ${DEPENDS} ${ARGN})
    add_custom_command(
        OUTPUT ${name}.cpp
        COMMAND ${CYTHON_BIN}
        ARGS ${CYTHON_FLAGS} -I ${CYTHON_INCLUDE_DIRECTORIES} -o ${name}.cpp ${CMAKE_CURRENT_SOURCE_DIR}/${name}.pyx
        DEPENDS ${DEPENDS}
        COMMENT "Cythonizing ${name}.pyx")
endmacro(CYTHON_ADD_MODULE_PYX)

# Cythonizes and compiles a .pyx file
macro(CYTHON_ADD_MODULE name)
    CYTHON_ADD_MODULE_PYX(${name})
    # We need Python for this:
    find_package(Python REQUIRED)
    add_python_library(${name} ${name}.cpp ${ARGN})
endmacro(CYTHON_ADD_MODULE)

