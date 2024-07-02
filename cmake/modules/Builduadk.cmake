function(build_uadk)
    set(configure_cmd ./conf.sh)

    include(ExternalProject)
    ExternalProject_Add(uadk_ext
	    UPDATE_COMMAND "" # this disables rebuild on each run
	    GIT_REPOSITORY "https://github.com/Linaro/uadk.git"
            GIT_CONFIG advice.detachedHead=false
            GIT_SHALLOW 1
            GIT_TAG "master"
            SOURCE_DIR "${CMAKE_BINARY_DIR}/src/uadk"
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND ./autogen.sh COMMAND ${configure_cmd}
            BUILD_COMMAND make
	    BUILD_BYPRODUCTS
	        /usr/local/lib/libwd_comp.so
		/usr/local/lib/libwd.so
            INSTALL_COMMAND make install
            LOG_CONFIGURE ON
            LOG_BUILD ON
            LOG_INSTALL ON
            LOG_MERGED_STDOUTERR ON
            LOG_OUTPUT_ON_FAILURE ON)

    ExternalProject_Get_Property(uadk_ext source_dir)
    set(UADK_INCLUDE_DIR "/usr/local/include")
    set(UADK_WD_COMP_LIBRARY "/usr/local/lib/libwd_comp.so")
    set(UADK_WD_LIBRARY "/usr/local/lib/libwd.so")
    set(UADK_INCLUDE_DIR ${UADK_INCLUDE_DIR} PARENT_SCOPE)
    set(UADK_WD_COMP_LIBRARY ${UADK_LIBRARIES} PARENT_SCOPE)
    set(UADK_WD_LIBRARY ${UADK_WD_LIBRARIES} PARENT_SCOPE)

    add_library(uadk::uadk UNKNOWN IMPORTED)
    add_library(uadk::uadkwd UNKNOWN IMPORTED)
    add_dependencies(uadk::uadk uadk_ext)
    add_dependencies(uadk::uadkwd uadk_ext)
    file(MAKE_DIRECTORY ${UADK_INCLUDE_DIR})
    set_target_properties(uadk::uadk PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${UADK_INCLUDE_DIR}
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
	IMPORTED_LOCATION "${UADK_WD_COMP_LIBRARY}")
    set_target_properties(uadk::uadkwd PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${UADK_INCLUDE_DIR}
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
	IMPORTED_LOCATION "${UADK_WD_LIBRARY}")
endfunction()
