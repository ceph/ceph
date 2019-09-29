# some platforms do not offer support for atomic primitive for all integer
# types, in that case we need to link against libatomic

include(CheckCXXSourceCompiles)
include(CMakePushCheckState)


function(check_cxx_atomics var)
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++11")
    check_cxx_source_compiles("
#include <atomic>
#include <cstdint>
int main() {
  std::atomic<uint8_t> w1;
  std::atomic<uint16_t> w2;
  std::atomic<uint32_t> w4;
  std::atomic<uint64_t> w8;
#ifdef __s390x__
  // Boost needs 16-byte atomics for tagged pointers.
  std::atomic<unsigned __int128> w16;
#else
  #define w16 0
#endif
  return w1 + w2 + w4 + w8 + w16;
}
" ${var})
endfunction(check_cxx_atomics)

cmake_push_check_state()
check_cxx_atomics(HAVE_CXX11_ATOMIC)
cmake_pop_check_state()

if(NOT HAVE_CXX11_ATOMIC)
  cmake_push_check_state()
  set(CMAKE_REQUIRED_LIBRARIES "atomic")
  check_cxx_atomics(HAVE_LIBATOMIC)
  cmake_pop_check_state()
  if(HAVE_LIBATOMIC)
    set(LIBATOMIC_LINK_FLAGS "-Wl,--as-needed -latomic")
  else()
    message(FATAL_ERROR
      "Host compiler ${CMAKE_CXX_COMPILER} requires libatomic, but it is not found")
  endif()
endif()
