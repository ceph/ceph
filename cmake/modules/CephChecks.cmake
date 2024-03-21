if(CMAKE_COMPILER_IS_GNUCXX)
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8.1)
    message(FATAL_ERROR "GCC 8.1+ required due to C++17 requirements")
  endif()
endif()

#Check Includes
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)
include(CheckFunctionExists)

check_function_exists(memset_s HAVE_MEMSET_S)
check_function_exists(fallocate CEPH_HAVE_FALLOCATE)
check_function_exists(posix_fadvise HAVE_POSIX_FADVISE)
check_function_exists(posix_fallocate HAVE_POSIX_FALLOCATE)
check_function_exists(syncfs HAVE_SYS_SYNCFS)
check_function_exists(sync_file_range HAVE_SYNC_FILE_RANGE)
check_function_exists(pwritev HAVE_PWRITEV)
check_function_exists(splice CEPH_HAVE_SPLICE)
check_function_exists(getgrouplist HAVE_GETGROUPLIST)
if(NOT APPLE)
  check_function_exists(fdatasync HAVE_FDATASYNC)
endif()
check_function_exists(strerror_r HAVE_Strerror_R)
check_function_exists(name_to_handle_at HAVE_NAME_TO_HANDLE_AT)
check_function_exists(pipe2 HAVE_PIPE2)
check_function_exists(accept4 HAVE_ACCEPT4)
check_function_exists(sigdescr_np HAVE_SIGDESCR_NP)

include(CMakePushCheckState)
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_LIBRARIES pthread)
check_function_exists(pthread_spin_init HAVE_PTHREAD_SPINLOCK)
check_function_exists(pthread_set_name_np HAVE_PTHREAD_SET_NAME_NP)
check_function_exists(pthread_get_name_np HAVE_PTHREAD_GET_NAME_NP)
check_function_exists(pthread_setname_np HAVE_PTHREAD_SETNAME_NP)
check_function_exists(pthread_getname_np HAVE_PTHREAD_GETNAME_NP)
check_function_exists(pthread_rwlockattr_setkind_np HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP)
cmake_pop_check_state()

check_function_exists(eventfd HAVE_EVENTFD)
check_function_exists(getprogname HAVE_GETPROGNAME)
check_function_exists(gettid HAVE_GETTID)

CHECK_INCLUDE_FILES("linux/types.h" HAVE_LINUX_TYPES_H)
CHECK_INCLUDE_FILES("linux/version.h" HAVE_LINUX_VERSION_H)
CHECK_INCLUDE_FILES("arpa/nameser_compat.h" HAVE_ARPA_NAMESER_COMPAT_H)
CHECK_INCLUDE_FILES("sys/mount.h" HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILES("sys/param.h" HAVE_SYS_PARAM_H)
CHECK_INCLUDE_FILES("sys/types.h" HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES("sys/vfs.h" HAVE_SYS_VFS_H)
CHECK_INCLUDE_FILES("sys/prctl.h" HAVE_SYS_PRCTL_H)
CHECK_INCLUDE_FILES("execinfo.h" HAVE_EXECINFO_H)
if(LINUX)
  CHECK_INCLUDE_FILES("sched.h" HAVE_SCHED)
endif()
CHECK_INCLUDE_FILES("valgrind/helgrind.h" HAVE_VALGRIND_HELGRIND_H)

include(CheckTypeSize)
set(CMAKE_EXTRA_INCLUDE_FILES "linux/types.h" "netinet/in.h")
CHECK_TYPE_SIZE(__u8 __U8) 
CHECK_TYPE_SIZE(__u16 __U16) 
CHECK_TYPE_SIZE(__u32 __U32) 
CHECK_TYPE_SIZE(__u64 __U64) 
CHECK_TYPE_SIZE(__s8 __S8) 
CHECK_TYPE_SIZE(__s16 __S16) 
CHECK_TYPE_SIZE(__s32 __S32) 
CHECK_TYPE_SIZE(__s64 __S64)
CHECK_TYPE_SIZE(in_addr_t IN_ADDR_T)
unset(CMAKE_EXTRA_INCLUDE_FILES)

include(CheckSymbolExists)
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_LIBRARIES rt)
check_symbol_exists(_POSIX_TIMERS "unistd.h;time.h" HAVE_POSIX_TIMERS)
cmake_pop_check_state()
if(HAVE_POSIX_TIMERS)
  find_library(RT_LIBRARY NAMES rt)
endif()
check_symbol_exists(res_nquery "resolv.h" HAVE_RES_NQUERY)
check_symbol_exists(F_SETPIPE_SZ "linux/fcntl.h" CEPH_HAVE_SETPIPE_SZ)
check_symbol_exists(__func__ "" HAVE_FUNC)
check_symbol_exists(__PRETTY_FUNCTION__ "" HAVE_PRETTY_FUNC)
check_symbol_exists(getentropy "unistd.h" HAVE_GETENTROPY)

include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
  #include <string.h>
  int main() { char x = *strerror_r(0, &x, sizeof(x)); return 0; }
  " STRERROR_R_CHAR_P)

include(CheckStructHasMember)
CHECK_STRUCT_HAS_MEMBER("struct stat" st_mtim.tv_nsec sys/stat.h
  HAVE_STAT_ST_MTIM_TV_NSEC LANGUAGE C)
CHECK_STRUCT_HAS_MEMBER("struct stat" st_mtimespec.tv_nsec sys/stat.h
  HAVE_STAT_ST_MTIMESPEC_TV_NSEC LANGUAGE C)

if(NOT CMAKE_CROSSCOMPILING)
  include(CheckCXXSourceRuns)
  cmake_push_check_state()
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++17")
  if(WIN32)
    set(CMAKE_REQUIRED_LIBRARIES ws2_32)
  endif()

  check_cxx_source_runs("
#include <cstdint>
#include <iterator>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

uint32_t load(char* p, size_t offset)
{
  return *reinterpret_cast<uint32_t*>(p + offset);
}

bool good(uint32_t lhs, uint32_t big_endian)
{
  return lhs == ntohl(big_endian);
}

int main(int argc, char **argv)
{
  char a1[] = \"ABCDEFG\";
  uint32_t a2[] = {0x41424344,
                   0x42434445,
                   0x43444546,
                   0x44454647};
  for (size_t i = 0; i < std::size(a2); i++) {
    if (!good(load(a1, i), a2[i])) {
      return 1;
    }
  }
}"
    HAVE_UNALIGNED_ACCESS)
  cmake_pop_check_state()
  if(NOT HAVE_UNALIGNED_ACCESS)
    message(FATAL_ERROR "Unaligned access is required")
  endif()
else(NOT CMAKE_CROSSCOMPILING)
  message(STATUS "Assuming unaligned access is supported")
endif(NOT CMAKE_CROSSCOMPILING)

set(version_script_source "v1 { }; v2 { } v1;")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/version_script.txt "${version_script_source}")
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_FLAGS "-Werror -Wl,--version-script=${CMAKE_CURRENT_BINARY_DIR}/version_script.txt")
check_c_source_compiles("
__attribute__((__symver__ (\"func@v1\"))) void func_v1() {};
__attribute__((__symver__ (\"func@v2\"))) void func_v2() {};

int main() {}"
  HAVE_ATTR_SYMVER)
  if(CMAKE_CXX_COMPILER_ID STREQUAL GNU AND NOT HAVE_ATTR_SYMVER)
    if(CMAKE_CXX_FLAGS MATCHES "-flto" AND NOT CMAKE_CXX_FLAGS MATCHES "-flto-partition=none")
      # https://tracker.ceph.com/issues/40060, skip for clang
      message(FATAL_ERROR "please pass -flto-partition=none as part of CXXFLAGS")
    endif()
  endif()
set(CMAKE_REQUIRED_FLAGS -Wl,--version-script=${CMAKE_CURRENT_BINARY_DIR}/version_script.txt)
check_c_source_compiles("
void func_v1() {}
__asm__(\".symver func_v1, func@v1\");
void func_v2() {}
__asm__(\".symver func_v2, func@v2\");

int main() {}"
  HAVE_ASM_SYMVER)
file(REMOVE ${CMAKE_CURRENT_BINARY_DIR}/version_script.txt)
cmake_pop_check_state()

# should use LINK_OPTIONS instead of LINK_LIBRARIES, if we can use cmake v3.14+
try_compile(HAVE_LINK_VERSION_SCRIPT
  ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES ${CMAKE_CURRENT_LIST_DIR}/CephCheck_link.c
  LINK_LIBRARIES "-Wl,--version-script=${CMAKE_CURRENT_LIST_DIR}/CephCheck_link.map")

try_compile(HAVE_LINK_EXCLUDE_LIBS
  ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES ${CMAKE_CURRENT_LIST_DIR}/CephCheck_link.c
  LINK_LIBRARIES "-Wl,--exclude-libs,ALL")
