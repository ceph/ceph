// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Crimson implementation of the erasure-code logging boundary (ec_log).
 *
 * Although the body looks the same as the classical implementation,
 * it is different because of how the `dout` macro expands under WITH_CRIMSON:
 * it routes to the per-shard seastar logger via crimson::common::local_conf().
 */

#include <cstdarg>
#include <boost/container/small_vector.hpp>

#include "erasure-code/ErasureCodeLog.h"

#include "common/ceph_context.h"
#include "common/config.h"
#include "crimson/common/config_proxy.h"
#include "common/debug.h"
#include "global/global_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

int ec_log(int level, const char *format, ...)
{
  size_t size = 256;
  va_list ap;
  while (1) {
    boost::container::small_vector<char, 256> buf(size);
    va_start(ap, format);
    int n = vsnprintf(buf.data(), size, format, ap);
    va_end(ap);
    constexpr size_t MAX_SIZE = 8196UL;
    if ((n > -1 && static_cast<size_t>(n) < size) || size > MAX_SIZE) {
      dout(ceph::dout::need_dynamic(level)) << buf.data() << dendl;
      return n;
    }
    size *= 2;
  }
}
