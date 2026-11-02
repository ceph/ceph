# Disable the "--target=<arch>-<vendor>-<sys>" flags which Boost.Build
# (b2 5.2.1, as shipped with Boost 1.87) passes to clang whenever
# <architecture> and <address-model> are known, even for native builds.
# A target triple that differs from clang's default triple hides the
# per-target libc++ include directory (containing __config_site).
#
# Usage (in the Boost source directory): cmake -P PatchBoostClangJam.cmake

set(jam tools/build/src/tools/clang.jam)

set(orig [=[
    init-flags-cross $(toolset) : $(condition) : arm x86 : 64 : darwin ;
    init-flags-cross $(toolset) : $(condition) : arm x86 : 64 32 : linux ;
]=])

set(patched [=[
    # disabled by Ceph, see cmake/modules/PatchBoostClangJam.cmake
    #init-flags-cross $(toolset) : $(condition) : arm x86 : 64 : darwin ;
    #init-flags-cross $(toolset) : $(condition) : arm x86 : 64 32 : linux ;
]=])

file(READ ${jam} content)

string(FIND "${content}" "${patched}" pos)
if(pos GREATER_EQUAL 0)
  # already patched
  return()
endif()

string(FIND "${content}" "${orig}" pos)
if(pos LESS 0)
  message(FATAL_ERROR "${jam}: init-flags-cross calls not found, please update ${CMAKE_CURRENT_LIST_FILE}")
endif()

string(REPLACE "${orig}" "${patched}" content "${content}")
file(WRITE ${jam} "${content}")

# Configuration check results cached by a previous b2 run with the
# unpatched clang.jam may be wrong, which would silently disable
# Boost libraries.
file(REMOVE bin.v2/project-cache.jam)
