# Add CPM packages in a consistent, Ceph-friendly way:

include(${CMAKE_MODULE_PATH}/CPM.cmake)

# Two settings govern CPM's behavior with repsect to local packages:
# CPM_USE_LOCAL_PACKAGES and CPM_LOCAL_PACKAGES_ONLY. The first /prefers/ local
# packages; the second /requires/ them (e.g. for an offline-online build).
#
# This macro will save and restore the old global value for you (if 
# CPM_LOCAL_PACKAGES_ONLY is set, it will be honored).

macro(add_cpm PACKAGE_NAME USE_LOCAL_FLAG)
    set(_ORIG_CPM_LOCAL_PACKAGES_ONLY ${CPM_LOCAL_PACKAGES_ONLY})

    # Possibly force CPM to search only for local packages:
    if(${USE_LOCAL_FLAG})
        set(CPM_USE_LOCAL_PACKAGES ON)
    endif()

    CPMAddPackage(${ARGN})

    # Restore previous CPM state:
    set(CPM_USE_LOCAL_PACKAGES ${_ORIG_CPM_LOCAL_PACKAGES})
    
    message(STATUS "-- Enabled ${PACKAGE_NAME} support.")
endmacro()
