# This function is a helper that will merge static libraries.
# For example,
#
#    merge_static_libraries(mylib staticlibX staticlibY)
#
# mylib.a will generate a new static library mylib that is
# a combination of staticlibX and staticlibY
#
function(merge_static_libraries target)

    set(dummy_source ${CMAKE_CURRENT_BINARY_DIR}/${target}_dummy.c)
    add_library(${target} STATIC ${dummy_source})

    # remove duplicates
    set(libs ${ARGN})
    list(REMOVE_DUPLICATES libs)

    # validate that all libs are static
    foreach(lib ${libs})
        if (NOT TARGET ${lib})
            message(FATAL_ERROR "${lib} not a valid target")
        endif()

        get_target_property(libtype ${lib} TYPE)
        if(NOT libtype STREQUAL "STATIC_LIBRARY")
            message(FATAL_ERROR "${lib} not a static library")
        endif()

        # add a dependency on the lib
        add_dependencies(${target} ${lib})
    endforeach()

    # Force the merged Make the generated dummy source file depended on all static input
    # libs. If input lib changes,the source file is touched
    # which causes the desired effect (relink).
    add_custom_command(
        OUTPUT  ${dummy_source}
        COMMAND ${CMAKE_COMMAND} -E touch ${dummy_source}
        DEPENDS ${libs})

    # only LINUX is currently supported. OSX's libtool and windows lib.exe
    # have native support for merging static libraries, and support for them
    # can be easily added if required.
    if(LINUX)
        # generate a script to merge the static libraries in to the target
        # library. see https://sourceware.org/binutils/docs/binutils/ar-scripts.html
        set(mri_script "open $<TARGET_FILE:${target}>=")
        foreach(lib ${libs})
            # we use the generator expression TARGET_FILE to get the location
            # of the library. this will not be expanded until the script file
            # is written below
            set(mri_script "${mri_script} addlib $<TARGET_FILE:${lib}>=")
        endforeach()
        set(mri_script "${mri_script} save=end")

        add_custom_command(
            TARGET ${target} POST_BUILD
            COMMAND echo ${mri_script} | tr = \\\\n | ${CMAKE_AR} -M)
    endif(LINUX)

    message("-- MergeStaticLibraries: ${target}: merged ${libs}")

    # we want to set the target_link_libraries correctly for the new merged
    # static library. First we get the list of link libraries for each
    # of the libs we are merging
    set(link_libs)
    foreach(lib ${libs})
      get_property(trans TARGET ${lib} PROPERTY LINK_LIBRARIES)
      list(APPEND link_libs ${trans})
    endforeach()

    if (link_libs)
        # now remove the duplicates and any of the libraries we already merged
        list(REMOVE_DUPLICATES link_libs)
        foreach(lib ${libs})
            list(REMOVE_ITEM link_libs ${lib})
        endforeach()

        # set the target link libraries
        target_link_libraries(${target} ${link_libs})

        message("-- MergeStaticLibraries: ${target}: remaining ${link_libs}")
    endif()

endfunction()
