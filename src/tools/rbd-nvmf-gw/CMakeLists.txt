#
# We postpone and build libspdk_bdev_rbd library from here and not
# from SPDK in order to avoid complicated dependencies: bdev_rbd.c
# depends on librbd and librados include paths as well.
#
include(ExternalProject)
ExternalProject_Add(spdk_bdev_rbd
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk/module/bdev/rbd
    BUILD_IN_SOURCE 1
    BUILD_COMMAND env CFLAGS=-I${CMAKE_SOURCE_DIR}/src/include V=1 $(MAKE)
    BUILD_ALWAYS 1
    DOWNLOAD_COMMAND ""
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    INSTALL_COMMAND "")
unset(make_cmd)

add_library(libspdk_bdev_rbd STATIC IMPORTED)
set_property(TARGET libspdk_bdev_rbd PROPERTY IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/src/spdk/build/lib/libspdk_bdev_rbd.a)
add_dependencies(libspdk_bdev_rbd librados librbd spdk_bdev_rbd)

set(rbd_nvmf_gw_src rbd-nvmf-gw.c)
add_executable(rbd-nvmf-gw ${rbd_nvmf_gw_src})
target_link_libraries(rbd-nvmf-gw -Wl,--whole-archive ${SPDK_LIBRARIES} libspdk_bdev_rbd librados librbd)

install(TARGETS rbd-nvmf-gw DESTINATION bin)
