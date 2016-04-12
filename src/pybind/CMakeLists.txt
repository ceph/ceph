set(CYTHON_MODULE_DIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules)

add_subdirectory(rados)
add_subdirectory(rbd)
add_subdirectory(cephfs)

add_custom_target(cython_modules ALL
    DEPENDS cython_rados cython_cephfs cython_rbd)
