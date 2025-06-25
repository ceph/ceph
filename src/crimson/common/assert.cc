#include <cstdarg>
#include <iostream>

#include <seastar/util/backtrace.hh>
#include <seastar/core/reactor.hh>

#include "include/ceph_assert.h"

#include "crimson/common/log.h"

SET_SUBSYS(osd);

namespace ceph {
  [[gnu::cold]] void __ceph_assert_fail(const ceph::assert_data &ctx)
  {
    __ceph_assert_fail(ctx.assertion, ctx.file, ctx.line, ctx.function);
  }

  [[gnu::cold]] void __ceph_assert_fail(const char* assertion,
                                        const char* file, int line,
                                        const char* func)
  {
    GENERIC_ERROR("{}:{} : In function '{}', ceph_assert({})\n",
                  file, line, func, assertion);
    abort();
  }
  [[gnu::cold]] void __ceph_assertf_fail(const char *assertion,
                                         const char *file, int line,
                                         const char *func, const char* msg,
                                         ...)
  {
    char buf[8096];
    va_list args;
    va_start(args, msg);
    std::vsnprintf(buf, sizeof(buf), msg, args);
    va_end(args);

    GENERIC_ERROR("{}:{} : In function '{}', ceph_assert({})\n {}\n",
                 file, line, func, assertion, buf);
    abort();
  }

  [[gnu::cold]] void __ceph_abort(const char* file, int line,
                                  const char* func, const std::string& msg)
  {
    GENERIC_ERROR("{}:{} : In function '{}', abort({})\n", file, line, func, msg);
    abort();
  }

  [[gnu::cold]] void __ceph_abortf(const char* file, int line,
                                   const char* func, const char* fmt,
                                   ...)
  {
    char buf[8096];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    GENERIC_ERROR("{}:{} : In function '{}', abort()\n {}\n",
                  file, line, func, buf);
    std::cout << std::flush;
    abort();
  }
}
