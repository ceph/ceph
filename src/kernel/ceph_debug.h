#ifndef _FS_CEPH_DEBUG_H
#define _FS_CEPH_DEBUG_H

#include <linux/string.h>

#define _STRINGIFY(x) #x
#define STRINGIFY(x) _STRINGIFY(x)

#define FMT_PREFIX "%-26.26s: "
#define FMT_SUFFIX "%s"
#define LOG_ARGS __FILE__ ":" STRINGIFY(__LINE__)
#define TRAIL_PARAM ""

#define LOG_LINE FMT_PREFIX fmt, LOG_ARGS, args

#define _dout(fmt, args...) pr_debug(FMT_PREFIX fmt FMT_SUFFIX, LOG_ARGS, args);
#define dout(args...) _dout(args, TRAIL_PARAM)

#endif
