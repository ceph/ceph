# some platforms do not offer support for atomic primitive for all integer
# types, in that case we need to link against libatomic

include(CheckCXXSourceCompiles)
include(CMakePushCheckState)


function(check_cxx_atomics var)
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++11")
    check_cxx_source_compiles("
#include <atomic>
#include <cstdint>
#include <cstddef>

#if defined(__SIZEOF_INT128__)
// Boost needs 16-byte atomics for tagged pointers.
// These are implemented via inline instructions on the platform
// if 16-byte alignment can be proven, and are delegated to libatomic
// library routines otherwise.  Whether or not alignment is provably
// OK for a std::atomic unfortunately depends on compiler version and
// optimization levels, and also on the details of the expression.
// We specifically test access via an otherwise unknown pointer here
// to ensure we get the most complex case.  If this access can be
// done without libatomic, then all accesses can be done.
struct tagged_ptr {
  int* ptr;
  std::size_t tag;
};

void atomic16(std::atomic<tagged_ptr> *ptr)
{
  tagged_ptr p{nullptr, 1};
  ptr->store(p);
  tagged_ptr f = ptr->load();
  tagged_ptr new_tag{nullptr, 0};
  ptr->compare_exchange_strong(f, new_tag);
}
#endif

int main() {
#if defined(__SIZEOF_INT128__)
  std::atomic<tagged_ptr> ptr;
  atomic16(&ptr);
#endif
  std::atomic<uint8_t> w1;
  std::atomic<uint16_t> w2;
  std::atomic<uint32_t> w4;
  std::atomic<uint64_t> w8;
  return w1 + w2 + w4 + w8;
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
