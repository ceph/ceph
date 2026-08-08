// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Classic implementation of the erasure-code logging boundary (ec_log).
 *
 * Linked into ceph-common, and thus into every classic consumer that loads an
 * erasure-code plugin (ceph-osd, ceph-mon, ceph-erasure-code-tool, tests). 
 * Differs from crimson in how the `dout` macro expands. 
 */

#include <cstdarg>
#include <boost/container/small_vector.hpp>

#include "erasure-code/ErasureCodeLog.h"

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "global/global_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

int ec_log(int level, const char *format, ...)
{
  if (!g_ceph_context ||
      !g_ceph_context->_conf->subsys.should_gather(dout_subsys, level)) {
    return 0;
  }
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
