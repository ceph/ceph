// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/Types.h"
#include "librbd/cache/pwl/LogOperation.h"
#include "librbd/cache/pwl/Request.h"
#include "librbd/cache/pwl/LogMap.h"
#include "AbstractWriteLog.h"
#include <functional>
#include <list>

class Context;
class SafeTimer;

namespace librbd {

struct ImageCtx;

namespace cache {

namespace pwl {

template <typename ImageCtxT>
class ReplicatedWriteLog : public AbstractWriteLog<ImageCtxT> {
public:
  typedef io::Extent Extent;
  typedef io::Extents Extents;

  ReplicatedWriteLog(ImageCtxT &image_ctx, librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state);
  ~ReplicatedWriteLog();
  ReplicatedWriteLog(const ReplicatedWriteLog&) = delete;
  ReplicatedWriteLog &operator=(const ReplicatedWriteLog&) = delete;

private:
  using This = AbstractWriteLog<ImageCtxT>;
  using C_WriteRequestT = pwl::C_WriteRequest<This>;
  using C_BlockIORequestT = pwl::C_BlockIORequest<This>;
  using C_FlushRequestT = pwl::C_FlushRequest<This>;
  using C_DiscardRequestT = pwl::C_DiscardRequest<This>;
  using C_WriteSameRequestT = pwl::C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = pwl::C_CompAndWriteRequest<This>;

};

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
