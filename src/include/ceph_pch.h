// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Payload for target_precompile_headers(), applied by WITH_PCH to the targets
 * listed at the end of src/CMakeLists.txt that were measured to benefit.
 *
 * Never #include this from ordinary source: every translation unit must keep
 * including what it uses, so that the default WITH_PCH=OFF build still works.
 *
 * Contents are the std, fmt, boost and Ceph core headers that -ftime-trace
 * showed to be both near-universal and expensive across the consuming targets.
 * These are stable infrastructure, so the PCH is rarely invalidated.  No
 * target-specific headers (rgw_common.h, librbd/...): those are volatile, and
 * a target's own headers turned out to be a small share of its compile time.
 *
 * common/dout.h is safe here despite being macro-heavy: dout_subsys and
 * dout_prefix expand at the use site, so a TU redefining them still gets its
 * own values.
 *
 * An include guard rather than #pragma once, which ccache's precompiled-header
 * support is documented to mishandle.
 */
#ifndef CEPH_PCH_H
#define CEPH_PCH_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

#include <boost/variant.hpp>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/cmdparse.h"
#include "common/config_proxy.h"
#include "common/dout.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/denc.h"
#include "include/encoding.h"
#include "include/types.h"
#include "include/uuid.h"
#include "msg/msg_types.h"

#endif // CEPH_PCH_H
