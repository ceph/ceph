if(NOT Sanitizers_FIND_COMPONENTS)
  set(Sanitizers_FIND_COMPONENTS
    address undefined_behavior)
endif()
if(HAVE_JEMALLOC)
  message(WARNING "JeMalloc does not work well with sanitizers")
endif()

set(Sanitizers_COMPILE_OPTIONS)

foreach(component ${Sanitizers_FIND_COMPONENTS})
  if(component STREQUAL "address")
    set(Sanitizers_address_COMPILE_OPTIONS "-fsanitize=address")
  elseif(component STREQUAL "leak")
    set(Sanitizers_leak_COMPILE_OPTIONS "-fsanitize=leak")
  elseif(component STREQUAL "thread")
    if ("address" IN_LIST "${Sanitizers_FIND_COMPONENTS}" OR
        "leak" IN_LIST "${Sanitizers_FIND_COMPONENTS}")
      message(SEND_ERROR "Cannot combine -fsanitize-leak w/ -fsanitize-thread")
    elseif(NOT CMAKE_POSITION_INDEPENDENT_CODE)
      message(SEND_ERROR "TSan requires all code to be position independent")
    endif()
    set(Sanitizers_thread_COMPILE_OPTIONS "-fsanitize=thread")
  elseif(component STREQUAL "undefined_behavior")
    # https://gcc.gnu.org/bugzilla/show_bug.cgi?id=88684
    set(Sanitizers_undefined_behavior_COMPILE_OPTIONS "-fsanitize=undefined;-fno-sanitize=vptr")
  else()
    message(SEND_ERROR "Unsupported sanitizer: ${component}")
  endif()
  list(APPEND Sanitizers_COMPILE_OPTIONS "${Sanitizers_${component}_COMPILE_OPTIONS}")
endforeach()

if(Sanitizers_address_COMPILE_OPTIONS OR Sanitizers_leak_COMPILE_OPTIONS)
  # ASAN_LIBRARY will be read by ceph.in to preload the asan library
  find_library(ASAN_LIBRARY
    NAMES
      libasan.so.10
      libasan.so.9
      libasan.so.8
      libasan.so.7
      libasan.so.6
      libasan.so.5
      libasan.so.4
      libasan.so.3)
endif()

if(Sanitizers_COMPILE_OPTIONS)
  list(APPEND Sanitizers_COMPILE_OPTIONS
    "-fno-omit-frame-pointer")
endif()

include(CheckCXXSourceCompiles)
include(CMakePushCheckState)

cmake_push_check_state()
string (REPLACE ";" " " CMAKE_REQUIRED_FLAGS "${Sanitizers_COMPILE_OPTIONS}")
set(CMAKE_REQUIRED_LIBRARIES ${Sanitizers_COMPILE_OPTIONS})
check_cxx_source_compiles("int main() {}"
  Sanitizers_ARE_SUPPORTED)

file (READ ${CMAKE_CURRENT_LIST_DIR}/code_tests/Sanitizers_fiber_test.cc _sanitizers_fiber_test_code)
check_cxx_source_compiles ("${_sanitizers_fiber_test_code}" Sanitizers_FIBER_SUPPORT)
cmake_pop_check_state()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sanitizers
  REQUIRED_VARS
    Sanitizers_COMPILE_OPTIONS
    Sanitizers_ARE_SUPPORTED)

if(Sanitizers_FOUND)
  if(NOT TARGET Sanitizers::Sanitizers)
    add_library(Sanitizers::Sanitizers INTERFACE IMPORTED)
    set_target_properties(Sanitizers::Sanitizers PROPERTIES
      INTERFACE_COMPILE_OPTIONS "${Sanitizers_COMPILE_OPTIONS}"
      INTERFACE_LINK_LIBRARIES "${Sanitizers_COMPILE_OPTIONS}")
  endif()
  foreach(component ${Sanitizers_FIND_COMPONENTS})
    if(NOT TARGET Sanitizers::${component})
      set(target Sanitizers::${component})
      set(compile_option "${Sanitizers_${component}_COMPILE_OPTIONS}")
      add_library(${target} INTERFACE IMPORTED)
      set_target_properties(${target} PROPERTIES
        INTERFACE_COMPILE_OPTIONS "${compile_option}"
        INTERFACE_LINK_LIBRARIES "${compile_option}")
    endif()
  endforeach()
endif()
