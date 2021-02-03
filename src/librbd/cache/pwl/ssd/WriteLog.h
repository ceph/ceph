// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_SSD_WRITE_LOG
#define CEPH_LIBRBD_CACHE_PWL_SSD_WRITE_LOG

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
#include "librbd/cache/pwl/AbstractWriteLog.h"
#include "librbd/cache/pwl/LogMap.h"
#include "librbd/cache/pwl/LogOperation.h"
#include "librbd/cache/pwl/Request.h"
#include "librbd/cache/pwl/ssd/Builder.h"
#include "librbd/cache/pwl/ssd/Types.h"
#include <functional>
#include <list>

namespace librbd {

struct ImageCtx;

namespace cache {
namespace pwl {
namespace ssd {

template <typename ImageCtxT>
class WriteLog : public AbstractWriteLog<ImageCtxT> {
public:
  WriteLog(ImageCtxT &image_ctx,
           librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state,
           cache::ImageWritebackInterface& image_writeback,
           plugin::Api<ImageCtxT>& plugin_api);
  ~WriteLog();
  WriteLog(const WriteLog&) = delete;
  WriteLog &operator=(const WriteLog&) = delete;

  typedef io::Extent Extent;
  using This = AbstractWriteLog<ImageCtxT>;
  using C_BlockIORequestT = pwl::C_BlockIORequest<This>;
  using C_WriteRequestT = pwl::C_WriteRequest<This>;
  using C_WriteSameRequestT = pwl::C_WriteSameRequest<This>;

  bool alloc_resources(C_BlockIORequestT *req) override;
  void setup_schedule_append(
      pwl::GenericLogOperationsVector &ops, bool do_early_flush,
      C_BlockIORequestT *req) override;
  void complete_user_request(Context *&user_req, int r) override;

protected:
  using AbstractWriteLog<ImageCtxT>::m_lock;
  using AbstractWriteLog<ImageCtxT>::m_log_entries;
  using AbstractWriteLog<ImageCtxT>::m_image_ctx;
  using AbstractWriteLog<ImageCtxT>::m_cache_state;
  using AbstractWriteLog<ImageCtxT>::m_first_free_entry;
  using AbstractWriteLog<ImageCtxT>::m_first_valid_entry;
  using AbstractWriteLog<ImageCtxT>::m_bytes_allocated;

  void initialize_pool(Context *on_finish,
                       pwl::DeferredContexts &later) override;
  void process_work() override;
  void append_scheduled_ops(void) override;
  void schedule_append_ops(pwl::GenericLogOperations &ops) override;
  void remove_pool_file() override;
  void release_ram(std::shared_ptr<GenericLogEntry> log_entry) override;

private:
 class AioTransContext {
   public:
     Context *on_finish;
     ::IOContext ioc;
     explicit AioTransContext(CephContext* cct, Context *cb)
       : on_finish(cb), ioc(cct, this) {}

     ~AioTransContext(){}

     void aio_finish() {
       on_finish->complete(ioc.get_return_value());
       delete this;
     }
 }; //class AioTransContext

 struct WriteLogPoolRootUpdate {
    std::shared_ptr<pwl::WriteLogPoolRoot> root;
    Context *ctx;
    WriteLogPoolRootUpdate(std::shared_ptr<pwl::WriteLogPoolRoot> r,
                           Context* c)
      : root(r), ctx(c) {}
  };

  using WriteLogPoolRootUpdateList = std::list<std::shared_ptr<WriteLogPoolRootUpdate>>;
  WriteLogPoolRootUpdateList m_poolroot_to_update; /* pool root list to update to SSD */
  bool m_updating_pool_root = false;

  uint64_t m_log_pool_ring_buffer_size; /* Size of ring buffer */
  std::atomic<int> m_async_update_superblock = {0};
  BlockDevice *bdev = nullptr;
  uint64_t pool_size;
  pwl::WriteLogPoolRoot pool_root;
  Builder<This> *m_builderobj;

  Builder<This>* create_builder();
  void load_existing_entries(pwl::DeferredContexts &later);
  void collect_read_extents(
      uint64_t read_buffer_offset, LogMapEntry<GenericWriteLogEntry> map_entry,
      std::vector<WriteLogCacheEntry*> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, uint64_t entry_hit_length,
      Extent hit_extent, pwl::C_ReadRequest *read_ctx) override;
  void complete_read(
      std::vector<WriteLogCacheEntry*> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, Context *ctx) override;
  void enlist_op_appender();
  bool retire_entries(const unsigned long int frees_per_tx);
  bool has_sync_point_logs(GenericLogOperations &ops);
  void append_op_log_entries(GenericLogOperations &ops);
  void alloc_op_log_entries(GenericLogOperations &ops);
  Context* construct_flush_entry_ctx(
      std::shared_ptr<GenericLogEntry> log_entry);
  void append_ops(GenericLogOperations &ops, Context *ctx,
                  uint64_t* new_first_free_entry,
                  uint64_t &bytes_allocated);
  void write_log_entries(GenericLogEntriesVector log_entries,
                         AioTransContext *aio);
  void schedule_update_root(std::shared_ptr<WriteLogPoolRoot> root,
                            Context *ctx);
  void enlist_op_update_root();
  void update_root_scheduled_ops();
  int update_pool_root_sync(std::shared_ptr<pwl::WriteLogPoolRoot> root);
  void update_pool_root(std::shared_ptr<WriteLogPoolRoot> root,
                                          AioTransContext *aio);
  void pre_io_check(WriteLogCacheEntry *log_entry, uint64_t &length);
  void aio_read_data_block(WriteLogCacheEntry *log_entry, bufferlist *bl,
                           Context *ctx);
  void aio_read_data_block(std::vector<WriteLogCacheEntry*> &log_entries,
                           std::vector<bufferlist *> &bls, Context *ctx);
  static void aio_cache_cb(void *priv, void *priv2) {
    AioTransContext *c = static_cast<AioTransContext*>(priv2);
    c->aio_finish();
  }
};//class WriteLog

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::ssd::WriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PWL_SSD_WRITE_LOG
