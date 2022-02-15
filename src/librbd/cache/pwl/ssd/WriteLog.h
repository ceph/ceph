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

  bool initialize_pool(Context *on_finish,
                       pwl::DeferredContexts &later) override;
  void process_work() override;
  void append_scheduled_ops(void) override;
  void schedule_append_ops(pwl::GenericLogOperations &ops, C_BlockIORequestT *req) override;
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

 struct WriteLogSuperblockUpdate {
    std::shared_ptr<pwl::WriteLogSuperblock> superblock;
    Context *ctx;
    WriteLogSuperblockUpdate(std::shared_ptr<pwl::WriteLogSuperblock> r,
                           Context* c)
      : superblock(r), ctx(c) {}
  };

  using WriteLogSuperblockUpdateList = std::list<std::shared_ptr<WriteLogSuperblockUpdate>>;
  WriteLogSuperblockUpdateList m_superblock_to_update; /* superblock list to update to SSD */
  bool m_updating_superblock = false;

  std::atomic<int> m_async_update_superblock = {0};
  BlockDevice *bdev = nullptr;
  pwl::WriteLogSuperblock m_superblock;
  Builder<This> *m_builderobj = nullptr;

  Builder<This>* create_builder();
  int create_and_open_bdev();
  void load_existing_entries(pwl::DeferredContexts &later);
  void inc_allocated_cached_bytes(
      std::shared_ptr<pwl::GenericLogEntry> log_entry) override;
  void collect_read_extents(
      uint64_t read_buffer_offset, LogMapEntry<GenericWriteLogEntry> map_entry,
      std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, uint64_t entry_hit_length,
      Extent hit_extent, pwl::C_ReadRequest *read_ctx) override;
  void complete_read(
      std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, Context *ctx) override;
  void enlist_op_appender();
  bool retire_entries(const unsigned long int frees_per_tx);
  bool has_sync_point_logs(GenericLogOperations &ops);
  void append_op_log_entries(GenericLogOperations &ops);
  void alloc_op_log_entries(GenericLogOperations &ops);
  void construct_flush_entries(pwl::GenericLogEntries entires_to_flush,
				DeferredContexts &post_unlock,
				bool has_write_entry) override;
  void append_ops(GenericLogOperations &ops, Context *ctx,
                  uint64_t* new_first_free_entry);
  void write_log_entries(GenericLogEntriesVector log_entries,
                         AioTransContext *aio, uint64_t *pos);
  void schedule_update_superblock(std::shared_ptr<WriteLogSuperblock> superblock,
                                  Context *ctx);
  void enlist_op_update_superblock();
  void update_superblock_scheduled_ops();
  int update_superblock_sync(std::shared_ptr<pwl::WriteLogSuperblock> superblock);
  void update_superblock(std::shared_ptr<WriteLogSuperblock> superblock,
                         AioTransContext *aio);
  void aio_read_data_block(std::shared_ptr<GenericWriteLogEntry> log_entry,
                           bufferlist *bl, Context *ctx);
  void aio_read_data_blocks(std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries,
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
