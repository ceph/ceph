set(CYTHON_MODULE_DIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules)
get_property(compiler_launcher GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
get_property(link_launcher GLOBAL PROPERTY RULE_LAUNCH_LINK)
set(PY_CC \"${compiler_launcher} ${CMAKE_C_COMPILER}\")
set(PY_CXX \"${compiler_launcher} ${CMAKE_CXX_COMPILER}\")
set(PY_LDSHARED \"${link_launcher} ${CMAKE_C_COMPILER} -shared\")

add_subdirectory(rados)
add_subdirectory(rbd)
add_subdirectory(cephfs)

add_custom_target(cython_modules ALL
    DEPENDS cython_rados cython_cephfs cython_rbd)
