// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_SSD_WRITE_LOG
#define CEPH_LIBRBD_CACHE_PWL_SSD_WRITE_LOG

#include "AbstractWriteLog.h"
#include "blk/BlockDevice.h"
#include "common/AsyncOpTracker.h"
#include "common/Checksummer.h"
#include "common/environment.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "librbd/BlockGuard.h"
#include "librbd/Utils.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/Types.h"
#include "librbd/cache/pwl/LogMap.h"
#include "librbd/cache/pwl/LogOperation.h"
#include "librbd/cache/pwl/Request.h"
#include "librbd/cache/pwl/SSDTypes.h"
#include <functional>
#include <list>

namespace librbd {

struct ImageCtx;

namespace cache {

namespace pwl {

template <typename ImageCtxT>
class SSDWriteLog : public AbstractWriteLog<ImageCtxT> {
public:
  SSDWriteLog(ImageCtxT &image_ctx,
              librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state);
  ~SSDWriteLog() {}
  SSDWriteLog(const SSDWriteLog&) = delete;
  SSDWriteLog &operator=(const SSDWriteLog&) = delete;

  using This = AbstractWriteLog<ImageCtxT>;
  using C_BlockIORequestT = pwl::C_BlockIORequest<This>;

  //TODO: Implement below functions in later PR
  bool alloc_resources(C_BlockIORequestT *req) override { return false; }
  void setup_schedule_append(
      pwl::GenericLogOperationsVector &ops, bool do_early_flush) override {}

protected:
  using AbstractWriteLog<ImageCtxT>::m_lock;
  using AbstractWriteLog<ImageCtxT>::m_log_entries;
  using AbstractWriteLog<ImageCtxT>::m_image_ctx;
  using AbstractWriteLog<ImageCtxT>::m_cache_state;
  using AbstractWriteLog<ImageCtxT>::m_first_free_entry;
  using AbstractWriteLog<ImageCtxT>::m_first_valid_entry;

  void initialize_pool(Context *on_finish, pwl::DeferredContexts &later) override;
  //TODO: Implement below functions in later PR
  void process_work() override {}
  void append_scheduled_ops(void) override {}
  void schedule_append_ops(pwl::GenericLogOperations &ops) override {}
  void remove_pool_file() override {}

private:
  uint64_t m_log_pool_ring_buffer_size; /* Size of ring buffer */

  //classes and functions to faciliate block device operations
  class AioTransContext {
  public:
    Context *on_finish;
    ::IOContext ioc;
    explicit AioTransContext(CephContext* cct, Context *cb)
      :on_finish(cb), ioc(cct, this) {
    }
    ~AioTransContext(){}

    void aio_finish() {
      on_finish->complete(ioc.get_return_value());
      delete this;
    }
  }; //class AioTransContext

  BlockDevice *bdev = nullptr;
  uint64_t pool_size;
  pwl::WriteLogPoolRoot pool_root;

  int update_pool_root_sync(std::shared_ptr<pwl::WriteLogPoolRoot> root);

  static void aio_cache_cb(void *priv, void *priv2) {
    AioTransContext *c = static_cast<AioTransContext*>(priv2);
    c->aio_finish();
  }
};//class SSDWriteLog

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::SSDWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PWL_SSD_WRITE_LOG
