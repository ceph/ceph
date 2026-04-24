#ifndef __CEPH_SNAP_FMT_H
#define __CEPH_SNAP_FMT_H

#include "common/snap_types.h"

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
template <> struct fmt::formatter<SnapContext> : fmt::ostream_formatter {};
#endif

#endif
