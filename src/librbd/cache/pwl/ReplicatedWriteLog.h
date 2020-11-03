// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

#include <libpmemobj.h>
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
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
  ReplicatedWriteLog(
      ImageCtxT &image_ctx, librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state);
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

  PMEMobjpool *m_log_pool = nullptr;
  const char* m_pwl_pool_layout_name;

  void remove_pool_file();
  void load_existing_entries(pwl::DeferredContexts &later);
  void alloc_op_log_entries(pwl::GenericLogOperations &ops);
  int append_op_log_entries(pwl::GenericLogOperations &ops);
  void flush_then_append_scheduled_ops(void);
  void enlist_op_flusher();
  void flush_op_log_entries(pwl::GenericLogOperationsVector &ops);
  template <typename V>
  void flush_pmem_buffer(V& ops);

protected:
  using AbstractWriteLog<ImageCtxT>::m_lock;
  using AbstractWriteLog<ImageCtxT>::m_log_entries;
  using AbstractWriteLog<ImageCtxT>::m_image_ctx;
  using AbstractWriteLog<ImageCtxT>::m_perfcounter;
  using AbstractWriteLog<ImageCtxT>::m_ops_to_flush;
  using AbstractWriteLog<ImageCtxT>::m_cache_state;
  using AbstractWriteLog<ImageCtxT>::m_first_free_entry;
  using AbstractWriteLog<ImageCtxT>::m_first_valid_entry;

  void process_work() override;
  void copy_pmem(C_BlockIORequestT *req) override;
  void schedule_append_ops(pwl::GenericLogOperations &ops) override;
  void append_scheduled_ops(void) override;
  void reserve_pmem(C_BlockIORequestT *req, bool &alloc_succeeds, bool &no_space) override;
  bool retire_entries(const unsigned long int frees_per_tx) override;
  void persist_last_flushed_sync_gen() override;
  bool alloc_resources(C_BlockIORequestT *req) override;
  void schedule_flush_and_append(pwl::GenericLogOperationsVector &ops) override;
  void setup_schedule_append(
      pwl::GenericLogOperationsVector &ops, bool do_early_flush) override;
  Context *construct_flush_entry_ctx(
        const std::shared_ptr<pwl::GenericLogEntry> log_entry) override;
  void initialize_pool(Context *on_finish, pwl::DeferredContexts &later) override;
  void write_data_to_buffer(
      std::shared_ptr<pwl::WriteLogEntry> ws_entry,
      pwl::WriteLogPmemEntry *pmem_entry) override;
};

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
