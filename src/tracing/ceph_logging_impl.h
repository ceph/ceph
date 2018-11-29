#ifndef CEPH_LOGGING_IMPL_H
#define CEPH_LOGGING_IMPL_H

#include <string>
#include <stdarg.h>
#include "fmt/fmt/format.h"
#include "fmt/fmt/ostream.h"

#include "tracing/ceph_logging.h"

using fmt::format;

#define trace(ll, ss, fmt, ...) __trace(ll, ss, format(fmt, __VA_ARGS__))
#define trace_error(ss, fmt, ...) __trace(-1, ss, format(fmt, __VA_ARGS__))

//static inline void trace(int loglevel, string subsys, const char *fmt, void *arg, ...)
static inline void __trace(int loglevel, string subsys, string str)
{
  str = subsys + ": " + str;
  if (loglevel <= -1) {
    str = "Error: " + str;
    tracepoint(ceph_logging, log_error, (char*)str.c_str());
  } else if (loglevel <= 1) {
    tracepoint(ceph_logging, log_critical, (char*)str.c_str());
  } else if (loglevel <= 5) {
    tracepoint(ceph_logging, log_warning, (char*)str.c_str());
  } else if (loglevel <= 10) {
    tracepoint(ceph_logging, log_info, (char*)str.c_str());
  } else if (loglevel <= 15) {
    tracepoint(ceph_logging, log_debug, (char*)str.c_str());
  } else {
    tracepoint(ceph_logging, log_verbose, (char*)str.c_str());
  }
}

#endif // CEPH_LOGGING_IMPL_H
