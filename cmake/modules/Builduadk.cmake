function(build_uadk)
    set(UADK_INSTALL_DIR ${CMAKE_BINARY_DIR}/src/uadk/install)
    set(UADK_INCLUDE_DIR ${UADK_INSTALL_DIR}/include)
    set(UADK_LIBRARY_DIR ${UADK_INSTALL_DIR}/lib)
    set(UADK_WD_LIBRARY ${UADK_LIBRARY_DIR}/libwd.a)
    set(UADK_WD_COMP_LIBRARY ${UADK_LIBRARY_DIR}/libwd_comp.a)
    set(UADK_WD_ZIP_LIBRARY ${UADK_LIBRARY_DIR}/uadk/libhisi_zip.a)
    set(configure_cmd env ./configure --prefix=${UADK_INSTALL_DIR})
    list(APPEND configure_cmd --with-pic --enable-static --disable-shared --with-static_drv)

    include(ExternalProject)
    ExternalProject_Add(uadk_ext
	    UPDATE_COMMAND "" # this disables rebuild on each run
	    GIT_REPOSITORY "https://github.com/Linaro/uadk.git"
            GIT_CONFIG advice.detachedHead=false
            GIT_TAG 90fb6f227427f568e34337309075ed7a3f71bab9
            SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/uadk"
            BUILD_IN_SOURCE 1
            CMAKE_ARGS -DCMAKE_CXX_COMPILER=which g++
            CONFIGURE_COMMAND ./autogen.sh COMMAND ${configure_cmd}
            BUILD_COMMAND make
	    BUILD_BYPRODUCTS ${UADK_WD_LIBRARY} ${UADK_WD_COMP_LIBRARY} ${UADK_WD_ZIP_LIBRARY}
            INSTALL_COMMAND make install
            LOG_CONFIGURE ON
            LOG_BUILD ON
            LOG_INSTALL ON
            LOG_MERGED_STDOUTERR ON
            LOG_OUTPUT_ON_FAILURE ON)

    ExternalProject_Get_Property(uadk_ext source_dir)
    set(UADK_INCLUDE_DIR ${UADK_INCLUDE_DIR} PARENT_SCOPE)

    add_library(uadk::uadk UNKNOWN IMPORTED)
    add_library(uadk::uadkwd UNKNOWN IMPORTED)
    add_library(uadk::uadkzip UNKNOWN IMPORTED)
    add_dependencies(uadk::uadk uadk_ext)
    add_dependencies(uadk::uadkwd uadk_ext)
    add_dependencies(uadk::uadkzip uadk_ext)
    file(MAKE_DIRECTORY ${UADK_INCLUDE_DIR})
    set_target_properties(uadk::uadk PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${UADK_INCLUDE_DIR}
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
	IMPORTED_LOCATION "${UADK_WD_COMP_LIBRARY}")
    set_target_properties(uadk::uadkwd PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${UADK_INCLUDE_DIR}
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
	IMPORTED_LOCATION "${UADK_WD_LIBRARY}")
    set_target_properties(uadk::uadkzip PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${UADK_INCLUDE_DIR}
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
	IMPORTED_LOCATION "${UADK_WD_ZIP_LIBRARY}")
endfunction()
