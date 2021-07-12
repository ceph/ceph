# https://github.com/snikulov/cmake-modules/blob/master/FindThrift.cmake

# - Find Thrift (a cross platform RPC lib/tool)
# This module defines
#  thrift_VERSION_STRING, version string of ant if found
#  thrift_LIBRARIES, libraries to link
#  thrift_INCLUDE_DIR, where to find thrift headers
#  thrift_COMPILER, thrift compiler executable
#  thrift_FOUND, If false, do not try to use ant
# Function
#  thrift_gen_cpp(<path to thrift file> <output variable with file list>)
#
# Initial work was done by Cloudera https://github.com/cloudera/Impala
# 2014 - modified by snikulov

# prefer the thrift version supplied in thrift_HOME (cmake -Dthrift_HOME then environment)
find_path(thrift_INCLUDE_DIR
    NAMES
        thrift/Thrift.h
    HINTS
        ${thrift_HOME}
        ENV thrift_HOME
        /usr/local
        /opt/local
    PATH_SUFFIXES
        include
)

# prefer the thrift version supplied in thrift_HOME
find_library(thrift_LIBRARIES
    NAMES
        thrift libthrift
    HINTS
        ${thrift_HOME}
        ENV thrift_HOME
        /usr/local
        /opt/local
    PATH_SUFFIXES
        lib lib64
)

find_program(thrift_COMPILER
    NAMES
        thrift
    HINTS
        ${thrift_HOME}
        ENV thrift_HOME
        /usr/local
        /opt/local
    PATH_SUFFIXES
        bin bin64
)

if (thrift_COMPILER)
    exec_program(${thrift_COMPILER}
        ARGS -version OUTPUT_VARIABLE __thrift_OUT RETURN_VALUE thrift_RETURN)
    string(REGEX MATCH "[0-9]+.[0-9]+.[0-9]+-[a-z]+$" thrift_VERSION_STRING ${__thrift_OUT})

    # define utility function to generate cpp files
    function(thrift_gen_cpp thrift_file thrift_CPP_FILES_LIST thrift_GEN_INCLUDE_DIR)
        set(_res)
        set(_res_inc_path)
        if(EXISTS ${thrift_file})
            get_filename_component(_target_dir ${thrift_file} NAME_WE)
            message("thrif_gen_cpp: ${thrift_file}")

            if(NOT EXISTS ${CMAKE_BINARY_DIR}/${_target_dir})
                file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_target_dir})
            endif()
            exec_program(${thrift_COMPILER}
                ARGS -o "${CMAKE_BINARY_DIR}/${_target_dir}" --gen cpp ${thrift_file}
                OUTPUT_VARIABLE __thrift_OUT
                RETURN_VALUE thrift_RETURN)
            file(GLOB_RECURSE __result_src "${CMAKE_BINARY_DIR}/${_target_dir}/*.cpp")
            file(GLOB_RECURSE __result_hdr "${CMAKE_BINARY_DIR}/${_target_dir}/*.h")
            list(APPEND _res ${__result_src})
            list(APPEND _res ${__result_hdr})
            if(__result_hdr)
                list(GET __result_hdr 0 _res_inc_path)
                get_filename_component(_res_inc_path ${_res_inc_path} DIRECTORY)
            endif()
        else()
            message("thrift_gen_cpp: file ${thrift_file} does not exists")
        endif()
        set(${thrift_CPP_FILES_LIST} "${_res}" PARENT_SCOPE)
        set(${thrift_GEN_INCLUDE_DIR} "${_res_inc_path}" PARENT_SCOPE)
    endfunction()
endif ()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(thrift DEFAULT_MSG thrift_LIBRARIES thrift_INCLUDE_DIR thrift_COMPILER)
mark_as_advanced(thrift_LIBRARIES thrift_INCLUDE_DIR thrift_COMPILER thrift_VERSION_STRING)
