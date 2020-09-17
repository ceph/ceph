// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// // vim: ts=8 sw=2 smarttab

#include "ReplicatedWriteLog.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/pwl/ImageCacheState.h"
#include "librbd/cache/pwl/LogEntry.h"
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ReplicatedWriteLog: " << this << " " \
                             <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
using namespace librbd::cache::pwl;

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(I &image_ctx, librbd::cache::pwl::ImageCacheState<I>* cache_state)
: AbstractWriteLog<I>(image_ctx, cache_state)
{ 
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ReplicatedWriteLog<librbd::ImageCtx>;
