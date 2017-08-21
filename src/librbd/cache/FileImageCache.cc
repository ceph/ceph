// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FileImageCache.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/file/ImageStore.h"
#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/file/StupidPolicy.h"
#include "librbd/cache/file/Types.h"
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::FileImageCache: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::file;

namespace {

typedef std::list<bufferlist> Buffers;
typedef std::map<uint64_t, bufferlist> ExtentBuffers;
typedef std::function<void(uint64_t)> ReleaseBlock;

static const uint32_t BLOCK_SIZE = 4096;

bool is_block_aligned(const ImageCache::Extents &image_extents) {
  for (auto &extent : image_extents) {
    if (extent.first % BLOCK_SIZE != 0 || extent.second % BLOCK_SIZE != 0) {
      return false;
    }
  }
  return true;
}

class ThreadPoolSingleton : public ThreadPool {
public:
  ContextWQ *pcache_op_work_queue;

  explicit ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "librbd::cache::thread_pool", "tp_librbd_cache", 32,
                 "pcache_threads"),
      pcache_op_work_queue(new ContextWQ("librbd::pcache_op_work_queue",
                                  cct->_conf->rbd_op_thread_timeout,
                                  this)) {
    start();
  }
  ~ThreadPoolSingleton() override {
    pcache_op_work_queue->drain();
    delete pcache_op_work_queue;

    stop();
  }
};

struct C_ReleaseBlockGuard : public BlockGuard::C_BlockIORequest {
  BlockGuard::C_BlockRequest *block_request;
  BlockGuard::BlockIO block_io;

  C_ReleaseBlockGuard(CephContext *cct,
                      BlockGuard::C_BlockRequest *block_request,
		      BlockGuard::BlockIO &block_io)
    : C_BlockIORequest(cct, nullptr),
      block_request(block_request), block_io(block_io) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name()
                  << " , this req is " << this
                  << " , next req is " << next_block_request
                  << " , block_io = " << block_io.block_info->block
                  << dendl;
    auto block_info = block_io.block_info;
    block_info->lock.lock();
    if(next_block_request == nullptr 
	&& block_info->tail_block_io_request == this) {
      block_info->tail_block_io_request = nullptr;
      block_info->in_process = false;
    }
    finish(0);
    block_info->lock.unlock();
    if(next_block_request != nullptr) {
      next_block_request->send();
    }
  }
  virtual const char *get_name() const override {
    return "C_ReleaseBlockGuard";
  }

  virtual void finish(int r) override {
    ldout(cct, 20) << "(" << get_name() << "): block_io = " << block_io.block_info->block << ", r=" << r << dendl;

    // complete block request
    block_request->complete_request(r);
  }
};

template <typename I>
struct C_PromoteToCache : public BlockGuard::C_BlockIORequest {
  ImageStore<I> &image_store;
  Policy* policy;
  uint64_t block;
  const bufferlist &bl;

  C_PromoteToCache(CephContext *cct, ImageStore<I> &image_store, Policy *policy, 
                   uint64_t block,
                   const bufferlist &bl, C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), block(block), bl(bl), policy(policy) {
  }

  virtual void send() override {
    ldout(cct, 1) << "(" << get_name() << "): "
                   << "block=" << block << dendl;
    // promote the clean block to the cache
    bufferlist sub_bl;
    sub_bl.append(bl);
    image_store.write_block(policy->block_to_offset(block), {{0, BLOCK_SIZE}}, std::move(sub_bl),
                            this);
  }
  virtual const char *get_name() const override {
    return "C_PromoteToCache";
  }
};

template <typename I>
struct C_DemoteFromCache : public BlockGuard::C_BlockIORequest {
  ImageStore<I> &image_store;
  MetaStore<I> *meta_store;
  Policy* policy;
  uint64_t block;

  C_DemoteFromCache(CephContext *cct, ImageStore<I> &image_store,
                    MetaStore<I> *meta_store,
                    Policy* policy, uint64_t block,
                    C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), meta_store(meta_store), 
      policy(policy), block(block) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;
    meta_store->update(block, policy->get_loc(block));
    image_store.discard_block(block, this);
  }
  virtual const char *get_name() const override {
    return "C_DemoteFromCache";
  }

};

template <typename I>
struct C_ReadFromCacheRequest : public BlockGuard::C_BlockIORequest {
  ImageStore<I> &image_store;
  Policy *policy;
  BlockGuard::BlockIO block_io;
  ExtentBuffers *extent_buffers;

  C_ReadFromCacheRequest(CephContext *cct, ImageStore<I> &image_store,
                         Policy* policy,
                         BlockGuard::BlockIO &&block_io,
                         ExtentBuffers *extent_buffers,
                         C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), block_io(block_io),
      extent_buffers(extent_buffers), policy(policy) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;
    C_Gather *ctx = new C_Gather(cct, this);
    for (auto &extent : block_io.extents) {
      image_store.read_block(policy->block_to_offset(block_io.block_info->block),
                             {{extent.block_offset, extent.block_length}},
                             &(*extent_buffers)[extent.buffer_offset],
                             ctx->new_sub());
    }
    ctx->activate();
  }
  virtual const char *get_name() const override {
    return "C_ReadFromCacheRequest";
  }
};

template <typename I>
struct C_ReadFromImageRequest : public BlockGuard::C_BlockIORequest {
  ImageWriteback<I> &image_writeback;
  BlockGuard::BlockIO block_io;
  ExtentBuffers *extent_buffers;

  C_ReadFromImageRequest(CephContext *cct, ImageWriteback<I> &image_writeback,
                         BlockGuard::BlockIO &&block_io,
                         ExtentBuffers *extent_buffers,
                         C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_writeback(image_writeback), block_io(block_io),
      extent_buffers(extent_buffers) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    // TODO improve scatter/gather to include buffer offsets
    uint64_t image_offset = block_io.block_info->block * BLOCK_SIZE;
    C_Gather *ctx = new C_Gather(cct, this);
    for (auto &extent : block_io.extents) {
      image_writeback.aio_read({{image_offset + extent.block_offset,
                                 extent.block_length}},
                               &(*extent_buffers)[extent.buffer_offset],
                               0, ctx->new_sub());
    }
    ctx->activate();
  }
  virtual const char *get_name() const override {
    return "C_ReadFromImageRequest";
  }
};

template <typename I>
struct C_WriteToMetaRequest : public BlockGuard::C_BlockIORequest {
  MetaStore<I> *meta_store;
  uint64_t cache_block_id;
  Policy *policy;

  C_WriteToMetaRequest(CephContext *cct, MetaStore<I> *meta_store,
		                   uint64_t cache_block_id, Policy *policy,
                       C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
    meta_store(meta_store),
    cache_block_id(cache_block_id), policy(policy) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "cache_block_id=" << cache_block_id << dendl;

    meta_store->update(cache_block_id, policy->get_loc(cache_block_id));
    complete(0);
  }

  virtual const char *get_name() const override {
    return "C_WriteToMetaRequest";
  }
};

template <typename I>
struct C_WriteToImageRequest : public BlockGuard::C_BlockIORequest {
  ImageWriteback<I> &image_writeback;
  BlockGuard::BlockIO block_io;
  const bufferlist &bl;

  C_WriteToImageRequest(CephContext *cct, ImageWriteback<I> &image_writeback,
                        BlockGuard::BlockIO &&block_io, const bufferlist &bl,
                        C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_writeback(image_writeback), block_io(block_io), bl(bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    uint64_t image_offset = block_io.block_info->block * BLOCK_SIZE;

    ImageCache::Extents image_extents;
    bufferlist scatter_bl;
    for (auto &extent : block_io.extents) {
      image_extents.emplace_back(image_offset + extent.block_offset,
                                 extent.block_length);

      bufferlist sub_bl;
      sub_bl.substr_of(bl, extent.buffer_offset, extent.block_length);
      scatter_bl.claim_append(sub_bl);
    }

    image_writeback.aio_write(std::move(image_extents), std::move(scatter_bl),
                              0, this);
  }
  virtual const char *get_name() const override {
    return "C_WriteToImageRequest";
  }
};

template <typename I>
struct C_ReadBlockFromCacheRequest : public BlockGuard::C_BlockIORequest {
  ImageStore<I> &image_store;
  uint64_t block;
  uint32_t block_size;
  bufferlist *block_bl;

  C_ReadBlockFromCacheRequest(CephContext *cct, ImageStore<I> &image_store,
                              uint64_t block, uint32_t block_size,
                               bufferlist *block_bl,
                              C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), block(block), block_size(block_size),
      block_bl(block_bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;
    image_store.read_block(block, {{0, block_size}}, block_bl, this);
  }
  virtual const char *get_name() const override {
    return "C_ReadBlockFromCacheRequest";
  }
};

template <typename I>
struct C_ReadBlockFromImageRequest : public BlockGuard::C_BlockIORequest {
  ImageWriteback<I> &image_writeback;
  uint64_t block;
  bufferlist *block_bl;

  C_ReadBlockFromImageRequest(CephContext *cct,
                              ImageWriteback<I> &image_writeback,
                              uint64_t block, bufferlist *block_bl,
                              C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_writeback(image_writeback), block(block), block_bl(block_bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;

    uint64_t image_offset = block * BLOCK_SIZE;
    image_writeback.aio_read({{image_offset, BLOCK_SIZE}}, block_bl, 0, this);
  }
  virtual const char *get_name() const override {
    return "C_ReadBlockFromImageRequest";
  }
};

struct C_CopyFromBlockBuffer : public BlockGuard::C_BlockIORequest {
  BlockGuard::BlockIO block_io;
  const bufferlist &block_bl;
  ExtentBuffers *extent_buffers;

  C_CopyFromBlockBuffer(CephContext *cct, const BlockGuard::BlockIO &block_io,
                        const bufferlist &block_bl,
                        ExtentBuffers *extent_buffers,
                        C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      block_io(block_io), block_bl(block_bl), extent_buffers(extent_buffers) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    for (auto &extent : block_io.extents) {
      bufferlist &sub_bl = (*extent_buffers)[extent.buffer_offset];
      sub_bl.substr_of(block_bl, extent.block_offset, extent.block_length);
    }
    complete(0);
  }
  virtual const char *get_name() const override {
    return "C_CopyFromBlockBuffer";
  }
};

struct C_ModifyBlockBuffer : public BlockGuard::C_BlockIORequest {
  BlockGuard::BlockIO block_io;
  const bufferlist &bl;
  bufferlist *block_bl;

  C_ModifyBlockBuffer(CephContext *cct, const BlockGuard::BlockIO &block_io,
                      const bufferlist &bl, bufferlist *block_bl,
                      C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      block_io(block_io), bl(bl), block_bl(block_bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    for (auto &extent : block_io.extents) {
      bufferlist modify_bl;
      if (extent.block_offset > 0) {
        bufferlist sub_bl;
        sub_bl.substr_of(*block_bl, 0, extent.block_offset);
        modify_bl.claim_append(sub_bl);
      }
      if (extent.block_length > 0) {
        bufferlist sub_bl;
        sub_bl.substr_of(bl, extent.buffer_offset, extent.block_length);
        modify_bl.claim_append(sub_bl);
      }
      uint32_t remaining_offset = extent.block_offset + extent.block_length;
      if (remaining_offset < block_bl->length()) {
        bufferlist sub_bl;
        sub_bl.substr_of(*block_bl, remaining_offset,
                         block_bl->length() - remaining_offset);
        modify_bl.claim_append(sub_bl);
      }
      std::swap(*block_bl, modify_bl);
    }
    complete(0);
  }
  virtual const char *get_name() const override {
    return "C_ModifyBlockBuffer";
  }
};

template <typename I>
struct C_ReadBlockRequest : public BlockGuard::C_BlockRequest {
  I &image_ctx;
  FileImageCache<I> *cache_ctx;
  bufferlist *bl;

  ExtentBuffers extent_buffers;
  Buffers promote_buffers;

  C_ReadBlockRequest(I &image_ctx,
                     FileImageCache<I> *cache_ctx,
                     bufferlist *bl,
                     Context *on_finish)
    : C_BlockRequest(on_finish), image_ctx(image_ctx),
      cache_ctx(cache_ctx), bl(bl) {
  }

  virtual void remap(PolicyMapResult policy_map_result,
                     BlockGuard::BlockIO &&block_io) {
    CephContext *cct = image_ctx.cct;

    // TODO: consolidate multiple reads into a single request (i.e. don't
    // have 1024 4K requests to read a single object)

    // NOTE: block guard active -- must be released after IO completes
    BlockGuard::C_BlockIORequest *req = new C_ReleaseBlockGuard(cct, this, block_io);
    BlockGuard::C_BlockIORequest *orig_tail_block_io_req = nullptr;
    auto block_info = block_io.block_info;
    //Mutex::Locker locker(block_info->lock);
    std::lock_guard<std::mutex> lock(block_info->lock);
    if(block_info->tail_block_io_request != nullptr) {
      orig_tail_block_io_req = (BlockGuard::C_BlockIORequest*)block_info->tail_block_io_request;
    }
    block_info->tail_block_io_request = (void*)req;

    switch (policy_map_result) {
    case POLICY_MAP_RESULT_HIT:
      req = new C_ReadFromCacheRequest<I>(cct, *cache_ctx->m_image_store, 
                                          cache_ctx->m_policy,
                                          std::move(block_io),
                                          &extent_buffers, req);
      break;
    case POLICY_MAP_RESULT_HIT_IN_BASE:
      PolicyMapResult parent_policy_map_result;
      cache_ctx->m_parent_policy->map(IO_TYPE_READ, block_io.block_info->block,
                                      false, &parent_policy_map_result);
      switch (parent_policy_map_result) {
      case POLICY_MAP_RESULT_HIT:
        req = new C_ReadFromCacheRequest<I>(cct, *cache_ctx->m_parent_image_store,
                                            cache_ctx->m_policy,
                                            std::move(block_io),
                                            &extent_buffers, req);
        break;
      case POLICY_MAP_RESULT_NEW:
        promote_buffers.emplace_back();
        req = new C_WriteToMetaRequest<I>(cct, cache_ctx->m_parent_meta_store, block_io.block_info->block,
                                          cache_ctx->m_parent_policy, req);
        req = new C_CopyFromBlockBuffer(cct, block_io, promote_buffers.back(),
                                        &extent_buffers, req);
        req = new C_PromoteToCache<I>(cct, *cache_ctx->m_parent_image_store,
                                      cache_ctx->m_parent_policy,
                                      block_io.block_info->block,
                                      promote_buffers.back(), req);
        req = new C_ReadBlockFromImageRequest<I>(cct, cache_ctx->m_parent_snap_image_writeback,
                                                 block_io.block_info->block,
                                                 &promote_buffers.back(), req);
        break;
      default:
        assert(false);
        break;
      }
      break;
    case POLICY_MAP_RESULT_MISS:
      req = new C_ReadFromImageRequest<I>(cct, cache_ctx->m_image_writeback,
                                          std::move(block_io), &extent_buffers,
                                          req);
      break;
    case POLICY_MAP_RESULT_NEW:
      promote_buffers.emplace_back();
      req = new C_WriteToMetaRequest<I>(cct, cache_ctx->m_meta_store, block_io.block_info->block,
                                        cache_ctx->m_policy, req);
      req = new C_CopyFromBlockBuffer(cct, block_io, promote_buffers.back(),
                                      &extent_buffers, req);
      req = new C_PromoteToCache<I>(cct, *cache_ctx->m_image_store,
                                    cache_ctx->m_policy,
                                    block_io.block_info->block,
                                    promote_buffers.back(), req);
      req = new C_ReadBlockFromImageRequest<I>(cct, cache_ctx->m_image_writeback,
                                               block_io.block_info->block,
                                               &promote_buffers.back(), req);
      break;
    default:
      assert(false);
    }
    if(orig_tail_block_io_req != nullptr) {
      orig_tail_block_io_req->next_block_request = req;
    }
    if(!block_info->in_process) {
      ldout(cct, 10) << "block_io: "<< block_io << " is not in process, will schedule" << dendl;
      block_info->in_process = true;
      //schedule send on another tread
      cache_ctx->pcache_op_work_queue->queue(new FunctionContext(
        [req](int r) {
	        req->send();
	      }), 0);
    }else{
      ldout(cct, 10) << "block_io: "<< block_io << " is in process, skip schedule" << dendl;
    }
  }

  virtual void finish(int r) override {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadBlockRequest): r=" << r << dendl;

    if (r < 0) {
      C_BlockRequest::finish(r);
      return;
    }

    ldout(cct, 20) << "assembling read extents" << dendl;
    for (auto &extent_bl : extent_buffers) {
      ldout(cct, 20) << extent_bl.first << "~" << extent_bl.second.length()
                     << dendl;
      bl->claim_append(extent_bl.second);
    }
    C_BlockRequest::finish(0);
  }
};

template <typename I>
struct C_WriteBlockRequest : BlockGuard::C_BlockRequest {
  I &image_ctx;
  FileImageCache<I> *cache_ctx;
  bufferlist bl;
  uint32_t block_size;

  Buffers promote_buffers;

  C_WriteBlockRequest(I &image_ctx, FileImageCache<I> *cache_ctx,
                      bufferlist &&bl, uint32_t block_size, Context *on_finish)
    : C_BlockRequest(on_finish),
      image_ctx(image_ctx),
      cache_ctx(cache_ctx),
      bl(std::move(bl)), block_size(block_size) {
  }

  virtual void remap(PolicyMapResult policy_map_result,
                     BlockGuard::BlockIO &&block_io) {
    CephContext *cct = image_ctx.cct;

    // TODO: consolidate multiple writes into a single request (i.e. don't
    // have 1024 4K requests to read a single object)

    // NOTE: block guard active -- must be released after IO completes
    BlockGuard::C_BlockIORequest *req = new C_ReleaseBlockGuard(cct, this, block_io);
    auto block_info = block_io.block_info;
    std::lock_guard<std::mutex> lock(block_info->lock);
    BlockGuard::C_BlockIORequest *orig_tail_block_io_req = nullptr;
    if(block_info->tail_block_io_request!=nullptr) {
      orig_tail_block_io_req = (BlockGuard::C_BlockIORequest*)block_info->tail_block_io_request;
    }
    block_info->tail_block_io_request = (void*)req;

    if (policy_map_result == POLICY_MAP_RESULT_HIT) {
      req = new C_DemoteFromCache<I>(cct, *cache_ctx->m_image_store,
                                     cache_ctx->m_meta_store,
                                     cache_ctx->m_policy,
                                     block_io.block_info->block, req);
    }
    req = new C_WriteToImageRequest<I>(cct, cache_ctx->m_image_writeback,
                                     std::move(block_io), bl, req);
    if (orig_tail_block_io_req != nullptr)
      orig_tail_block_io_req->next_block_request = req;
    if(!block_info->in_process) {
      ldout(cct, 10) << "block_io: "<< block_info->block << " is not in process, will schedule" << dendl;
      ldout(cct, 20) << "block_io: "<< block_info->block << " orig_req: " << orig_tail_block_io_req << "next_req: " << req << dendl;
      block_info->in_process = true;
      //schedule send on another tread
      cache_ctx->pcache_op_work_queue->queue(new FunctionContext(
        [req](int r) {
          req->send();
      }), 0);
    }else{
      ldout(cct, 10) << "block_io: "<< block_info->block << " is in process, will skip schedule" << dendl;
    }
  }
};

} // anonymous namespace

template <typename I>
FileImageCache<I>::FileImageCache(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_image_writeback(image_ctx),
    p_cache_size(get_min(m_image_ctx.ssd_cache_size, m_image_ctx.size)),
    m_block_guard(image_ctx.cct, image_ctx.size, BLOCK_SIZE),
    m_lock("librbd::cache::FileImageCache::m_lock"),
    m_parent_snap_image_writeback(*image_ctx.parent) {
  CephContext *cct = m_image_ctx.cct;
  m_image_ctx.ssd_cache_size = p_cache_size;
  //create threadpool for parallel cache process
  ThreadPoolSingleton *thread_pool_singleton;
  cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
    thread_pool_singleton, "librbd::cache::thread_pool");
  pcache_op_work_queue = thread_pool_singleton->pcache_op_work_queue; 
  m_policy = new StupidPolicy<I>(image_ctx, p_cache_size);
  m_block_guard.set_block_map(m_policy->get_block_map());
}

template <typename I>
FileImageCache<I>::~FileImageCache() {
  delete m_policy;
}

template <typename I>
void FileImageCache<I>::aio_read(Extents &&image_extents, bufferlist *bl,
                                 int fadvise_flags, Context *on_finish) {
  // TODO handle fadvise flags
  /*BlockGuard::C_BlockRequest *req = new C_ReadBlockRequest<I>(
    m_image_ctx, m_image_writeback, *m_image_store, *m_parent_image_store, bl,
    on_finish);*/
  BlockGuard::C_BlockRequest *req = new C_ReadBlockRequest<I>(
    m_image_ctx, this, bl, on_finish);
  map_blocks(IO_TYPE_READ, std::move(image_extents), req);
}

template <typename I>
void FileImageCache<I>::aio_write(Extents &&image_extents,
                                  bufferlist&& bl,
                                  int fadvise_flags,
                                  Context *on_finish) {
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  // TODO handle fadvise flags
  BlockGuard::C_BlockRequest *req = new C_WriteBlockRequest<I>(
    m_image_ctx, this, std::move(bl), BLOCK_SIZE, on_finish);
  map_blocks(IO_TYPE_WRITE, std::move(image_extents), req);
}

template <typename I>
void FileImageCache<I>::aio_discard(uint64_t offset, uint64_t length,
                                    bool skip_partial_discard, Context *on_finish) {
    //Mutex::Locker locker(block_info->lock);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (!is_block_aligned({{offset, length}})) {
    // For clients that don't use LBA extents, re-align the discard request
    // to work with the cache
    ldout(cct, 20) << "aligning discard to block size" << dendl;

    // TODO: for non-aligned extents, invalidate the associated block-aligned
    // regions in the cache (if any), send the aligned extents to the cache
    // and the un-aligned extents directly to back to librbd
  }

  // TODO invalidate discard blocks until writethrough/back support added
  C_Gather *ctx = new C_Gather(cct, on_finish);
  Context *invalidate_done_ctx = ctx->new_sub();

  m_image_writeback.aio_discard(offset, length, skip_partial_discard, ctx->new_sub());

  ctx->activate();

  Context *invalidate_ctx = new FunctionContext(
    [this, offset, length, invalidate_done_ctx](int r) {
      invalidate({{offset, length}}, invalidate_done_ctx);
    });
  flush(invalidate_ctx);
}

template <typename I>
void FileImageCache<I>::aio_flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "on_finish=" << on_finish << dendl;
  m_image_writeback.aio_flush(on_finish);
}

template <typename I>
void FileImageCache<I>::aio_writesame(uint64_t offset, uint64_t length,
                                             bufferlist&& bl, int fadvise_flags,
                                             Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "data_len=" << bl.length() << ", "
                 << "on_finish=" << on_finish << dendl;

  m_image_writeback.aio_writesame(offset, length, std::move(bl), fadvise_flags,
                                  on_finish);
}

template <typename I>
void FileImageCache<I>::aio_compare_and_write(Extents &&image_extents,
                                                     bufferlist&& cmp_bl,
                                                     bufferlist&& bl,
                                                     uint64_t *mismatch_offset,
                                                     int fadvise_flags,
                                                     Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  m_image_writeback.aio_compare_and_write(
    std::move(image_extents), std::move(cmp_bl), std::move(bl), mismatch_offset,
    fadvise_flags, on_finish);
}

template <typename I>
void FileImageCache<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool has_parent = m_image_ctx.parent!=nullptr?true:false;
  Context *ctx = on_finish;
  // step1: init parent meta and cachestore if parent exists
  if (has_parent) {
    if_cloned_volume = true;
    ldout(cct, 20) << "parent_snap_id: "
      << m_image_ctx.parent->snap_id << ", parent_id: "
      << m_image_ctx.parent->id << dendl;

    m_parent_policy = new StupidPolicy<I>(*(m_image_ctx.parent), m_image_ctx.parent->size);
    m_parent_image_store = new ImageStore<I>(m_image_ctx, m_image_ctx.parent->size, m_image_ctx.parent->id);
    //bool parent_cache_exists = m_parent_image_store->check_exists();
    m_parent_meta_store = new MetaStore<I>(*(m_image_ctx.parent), (m_parent_policy->get_block_count()));
    bool parent_meta_exists = m_parent_meta_store->check_exists();

    ctx = new FunctionContext(
      [this, on_finish, parent_meta_exists](int r) {
        if (r < 0) {
          on_finish->complete(r);
          return;
        } else {
          if (!parent_meta_exists) {
            m_parent_meta_store->load(NOT_IN_CACHE);
          }
          uint32_t* parent_meta_map = new uint32_t[m_parent_policy->get_block_count()]();
          m_parent_meta_store->get_loc_map(parent_meta_map);
          m_parent_policy->set_loc(parent_meta_map);
          delete parent_meta_map;
          m_parent_image_store->init(on_finish);
        }
    });
    ctx = new FunctionContext(
      [this, on_finish, ctx](int r) {
        if (r < 0) {
          on_finish->complete(r);
          return;
        } else {
          m_parent_meta_store->init(ctx);
        }
    });
  }

  // step2: init meta and cachestore
  m_image_store = new ImageStore<I>(m_image_ctx, m_image_ctx.ssd_cache_size, m_image_ctx.id);
  //bool parent_cache_exists = m_parent_image_store->check_exists();
  m_meta_store = new MetaStore<I>(m_image_ctx, (m_image_ctx.size/BLOCK_SIZE));
  bool meta_exists = m_meta_store->check_exists();
  ctx = new FunctionContext(
    [this, on_finish, ctx, meta_exists, has_parent](int r) {
      if (r < 0) {
        on_finish->complete(r);
        return;
      } else {
        if (!meta_exists) {
          if (has_parent) {
            m_meta_store->load(LOCATE_IN_BASE_CACHE);
          } else {
            m_meta_store->load(NOT_IN_CACHE);
          }
        }
        uint32_t* meta_map = new uint32_t[m_policy->get_block_count()]();
        m_meta_store->get_loc_map(meta_map);
        m_policy->set_loc(meta_map);
        delete meta_map;
        m_image_store->init(ctx);
      }
  });
  m_meta_store->init(ctx);
}

template <typename I>
void FileImageCache<I>::remove(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      on_finish->complete(r);
  });
  ctx = new FunctionContext(
    [this, on_finish, ctx](int r) {
      if (r < 0) {
        on_finish->complete(r);
      } else {
        m_meta_store->remove(ctx);
      }
    });
  m_image_store->remove(ctx);
}

template <typename I>
void FileImageCache<I>::set_parent() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  if (m_image_ctx.parent) {
    ldout(cct, 1) << "parent exists, fix meta" << dendl;
    m_meta_store->load(LOCATE_IN_BASE_CACHE);
  }
  return;
}

template <typename I>
void FileImageCache<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO flush all in-flight IO and pending writeback prior to shut down

  // chain the shut down of the image, and meta stores
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      delete m_image_store;
      delete m_meta_store;
      on_finish->complete(r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [r, ctx](int _r) {
            ctx->complete(r);
          });
      }
      m_meta_store->shut_down(next_ctx);
    });
  if (m_image_ctx.parent != nullptr) {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        Context *next_ctx = ctx;
        if (r < 0) {
          next_ctx = new FunctionContext(
            [r, ctx](int _r) {
              ctx->complete(r);
            });
        }
        m_parent_image_store->shut_down(next_ctx);
      });
  }
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [r, ctx](int _r) {
            ctx->complete(r);
          });
      }
      m_image_store->shut_down(next_ctx);
    });

  {
    Mutex::Locker locker(m_lock);
    m_async_op_tracker.wait(m_image_ctx, ctx);
  }
}

template <typename I>
void FileImageCache<I>::invalidate(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  invalidate({{0, m_image_ctx.size}}, on_finish);
}

template <typename I>
void FileImageCache<I>::map_blocks(IOType io_type, Extents &&image_extents,
                                   BlockGuard::C_BlockRequest *block_request) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  BlockGuard::BlockIOs block_ios;
  m_block_guard.create_block_ios(io_type, image_extents, &block_ios,
                                 block_request);

  // map block IO requests to the cache or backing image based upon policy
  for (auto &block_io : block_ios) {
    map_block(std::move(block_io));
  }

  // advance the policy statistics
  m_policy->tick();
  block_request->activate();
}

template <typename I>
void FileImageCache<I>::map_block(BlockGuard::BlockIO &&block_io) {
  CephContext *cct = m_image_ctx.cct;

  int r;
  IOType io_type = static_cast<IOType>(block_io.io_type);

  PolicyMapResult policy_map_result;
  //if this volume has parent snap

  r = m_policy->map(io_type, block_io.block_info->block, block_io.partial_block,
                    &policy_map_result);
  if (r < 0) {
    lderr(cct) << "failed to map block via cache policy: " << cpp_strerror(r)
               << dendl;
    block_io.block_request->fail(r);
    return;
  }

  block_io.block_request->remap(policy_map_result, std::move(block_io));
}

/*template <typename I>
void FileImageCache<I>::load_meta_to_policy() {
  uint32_t* dest_map = new uint32_t[m_policy->get_block_count()]();
  m_meta_store->get_loc_map(dest_map);
  m_policy->set_loc(dest_map);
}

template <typename I>
void FileImageCache<I>::load_snap_as_base(Context* on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  int r;
  //create a parent_snap image_writeback
  PolicyMapResult policy_map_result;
  uint64_t replace_cache_block;

  uint64_t need_to_promote_blocks = m_policy->get_block_count() - 1;
  BlockGuard::C_BlockIORequest *complete_check = new C_LoadBaseCompleteCheck(cct, on_finish, need_to_promote_blocks);
  ldout(cct, 20) << "get_block_count: " << need_to_promote_blocks + 1 << dendl;

  for(uint64_t block_id = 0; block_id <= need_to_promote_blocks; block_id++){
    m_policy->set_to_base_cache(block_id);
    BlockGuard::C_BlockIORequest *req = new C_PromoteBaseToCacheComplete(cct, complete_check);
    Buffers *buffer_ptr = (Buffers*) req->get_buffer_ptr();
    buffer_ptr->emplace_back();
    req = new C_WriteToMetaRequest<I>(cct, m_meta_store, block_id,
                                      m_policy, req);
    req = new C_PromoteToCache<I>(cct, *m_parent_image_store,
                                  m_policy,
                                  block_id,
                                  buffer_ptr->back(), req);
    req = new C_ReadBlockFromImageRequest<I>(cct, m_parent_snap_image_writeback,
                                             block_id,
                                             &buffer_ptr->back(), req);
    complete_check->inflight_requests_count++;
    while (complete_check->inflight_requests_count > 1024) {
      usleep(1000);
    }
    req->send();
  }
}*/

template <typename I>
void FileImageCache<I>::invalidate(Extents&& image_extents,
                                   Context *on_finish) {
  // TODO
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint64_t block = m_policy->offset_to_block(image_offset);
      uint32_t block_start_offset = image_offset % BLOCK_SIZE;
      uint32_t block_end_offset = MIN(block_start_offset + image_length,
                                      BLOCK_SIZE);
      uint32_t block_length = block_end_offset - block_start_offset;

      m_policy->invalidate(block);
      m_meta_store->update(block, m_policy->get_loc(block));

      image_offset += block_length;
      image_length -= block_length;
    }
  }

  // dump specific extents within the cache
  on_finish->complete(0);
}

template <typename I>
void FileImageCache<I>::flush(Context *on_finish) {
  on_finish->complete(0);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::FileImageCache<librbd::ImageCtx>;
