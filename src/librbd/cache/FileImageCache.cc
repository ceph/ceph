// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FileImageCache.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/file/ImageStore.h"
#include "librbd/cache/file/JournalStore.h"
#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/file/StupidPolicy.h"
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

static const uint32_t BLOCK_SIZE = 4096;

bool is_block_aligned(const ImageCache::Extents &image_extents) {
  for (auto &extent : image_extents) {
    if (extent.first % BLOCK_SIZE != 0 || extent.second % BLOCK_SIZE != 0) {
      return false;
    }
  }
  return true;
}

struct C_ReadRequest;

struct C_ReadGatherBase : public Context {
  virtual void add_request(C_ReadRequest *request) = 0;
  virtual void complete_request(int r, uint64_t buffer_offset,
                                bufferlist &&bl) = 0;
};

struct C_ReadRequest : public Context {
  // TODO use gather API
  C_ReadGatherBase *read_gather;
  uint64_t buffer_offset;
  bufferlist bl;

  C_ReadRequest(C_ReadGatherBase *read_gather, uint64_t buffer_offset)
    : read_gather(read_gather), buffer_offset(buffer_offset) {
    read_gather->add_request(this);
  }

  virtual void finish(int r) {
    read_gather->complete_request(r, buffer_offset, std::move(bl));
  }
};

template <typename I>
struct C_PromoteAndReadRequest : public C_ReadRequest {
  ImageStore<I> &image_store;
  uint64_t block;
  uint32_t block_offset;
  uint32_t block_length;
  bool promoted = false;

  C_PromoteAndReadRequest(ImageStore<I> &image_store,
                          C_ReadGatherBase *read_gather, uint64_t buffer_offset,
                          uint64_t block, uint32_t block_offset,
                          uint32_t block_length)
    : C_ReadRequest(read_gather, buffer_offset),
      image_store(image_store), block(block), block_offset(block_offset),
      block_length(block_length) {
  }

  virtual void complete(int r) {
    if (r < 0) {
      C_ReadRequest::complete(r);
      return;
    }

    if (!promoted) {
      // promote the clean block to the cache
      promoted = true;
      assert(bl.length() == BLOCK_SIZE);

      bufferlist sub_bl;
      sub_bl.substr_of(bl, block_offset, block_length);
      std::swap(sub_bl, bl);

      image_store.write_block(block, {{0, BLOCK_SIZE}}, std::move(sub_bl),
                              this);
      return;
    }

    // complete the read from in-memory (sub-)bl
    C_ReadRequest::complete(0);
  }

};

template <typename I>
struct C_ReadGather : public C_ReadGatherBase {
  typedef std::vector<C_ReadRequest*> ReadRequests;
  typedef std::map<uint64_t, bufferlist> ExtentBuffers;

  Mutex lock;

  I &image_ctx;
  ImageWriteback<I> &image_writeback;
  ImageStore<I> &image_store;

  bufferlist *bl;
  Context *on_finish;
  ReadRequests read_requests;
  uint64_t buffer_offset = 0;
  int ret_val = 0;
  bool activated = false;

  ExtentBuffers extent_buffers;

  C_ReadGather(I &image_ctx, ImageWriteback<I> &image_writeback,
               ImageStore<I> &image_store, bufferlist *bl, size_t hint_count,
               Context *on_finish)
    : lock("C_ReadGather"), image_ctx(image_ctx),
      image_writeback(image_writeback), image_store(image_store), bl(bl),
      on_finish(on_finish) {
    read_requests.reserve(hint_count);
  }

  void replace_and_read_from_cache(uint64_t replace_block, int64_t block,
                                   uint32_t block_offset,
                                   uint32_t block_length) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadGather): "
                   << "replace_block=" << replace_block << ", "
                   << "block=" << block << ", "
                   << "block_offset=" << block_offset << ", "
                   << "block_length=" << block_length << dendl;
    // TODO need guarded IO
  }

  void promote_and_read_from_cache(uint64_t block, uint32_t block_offset,
                                   uint32_t block_length) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadGather): "
                   << "block=" << block << ", "
                   << "block_offset=" << block_offset << ", "
                   << "block_length=" << block_length << dendl;

    C_PromoteAndReadRequest<I> *req =
      new C_PromoteAndReadRequest<I>(image_store, this, buffer_offset, block,
                                     block_offset, block_length);
    buffer_offset += block_length;

    uint64_t block_start = (block * BLOCK_SIZE);
    image_writeback.aio_read({{block_start, BLOCK_SIZE}}, &req->bl, 0, req);
  }

  void read_from_cache(uint64_t block, uint32_t block_offset,
                       uint32_t block_length) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadGather): "
                   << "block=" << block << ", "
                   << "block_offset=" << block_offset << ", "
                   << "block_length=" << block_length << dendl;

    C_ReadRequest *req = new C_ReadRequest(this, buffer_offset);
    buffer_offset += block_length;

    image_store.read_block(block, {{block_offset, block_length}}, &req->bl,
                           req);
  }

  void read_from_image(uint64_t offset, uint64_t length) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadGather): "
                   << "offset=" << offset << ", "
                   << "length=" << length << dendl;

    C_ReadRequest *req = new C_ReadRequest(this, buffer_offset);
    buffer_offset += length;

    image_writeback.aio_read({{offset, length}}, &req->bl, 0, req);
  }

  virtual void add_request(C_ReadRequest *req) {
    Mutex::Locker locker(lock);
    read_requests.push_back(req);
  }

  virtual void complete_request(int r, uint64_t buffer_offset,
                                bufferlist &&req_bl) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadGather): "
                   << "r=" << r << ", "
                   << "buffer_offset=" << buffer_offset << ", "
                   << "pending=" << (read_requests.size() - 1) << dendl;

    {
      Mutex::Locker locker(lock);
      if (ret_val == 0 && r < 0) {
        ret_val = r;
      }

      extent_buffers[buffer_offset] = std::move(req_bl);

      read_requests.pop_back();
      if (!activated || !read_requests.empty()) {
        return;
      }
    }

    ldout(cct, 20) << "assembling read extents" << dendl;
    for (auto &extent_bl : extent_buffers) {
      ldout(cct, 20) << extent_bl.first << "~" << extent_bl.second.length()
                     << dendl;
      bl->claim_append(extent_bl.second);
    }

    complete(ret_val);
  }

  void activate() {
    Mutex::Locker locker(lock);
    activated = true;
    if (!read_requests.empty()) {
      return;
    }

    complete(ret_val);
  }

  virtual void finish(int r) {
    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
FileImageCache<I>::FileImageCache(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_image_writeback(image_ctx),
    m_policy(new StupidPolicy<I>(m_image_ctx)) {
}

template <typename I>
FileImageCache<I>::~FileImageCache() {
  delete m_policy;
}

template <typename I>
void FileImageCache<I>::aio_read(Extents &&image_extents, bufferlist *bl,
                                 int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  // TODO map image extents to block extents
  // TODO handle fadvise flags
  // iterate through each block within the provided extents
  C_ReadGather<I> *ctx = new C_ReadGather<I>(m_image_ctx, m_image_writeback,
                                             *m_image_store, bl,
                                             image_extents.size(), on_finish);
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint64_t block = m_meta_store->offset_to_block(image_offset);
      bool partial_block = (image_offset % BLOCK_SIZE != 0 ||
                            image_length != BLOCK_SIZE);

      // TODO allocate prison cell for block -- potentially defer extent

      Policy::MapResult map_result;
      uint64_t replace_cache_block;
      int r = m_policy->map(Policy::OP_TYPE_READ, block, partial_block,
                            &map_result, &replace_cache_block);

      if (r < 0) {
        // TODO
        on_finish->complete(r);
        return;
      }

      uint32_t block_start_offset = image_offset % BLOCK_SIZE;
      uint32_t block_end_offset = MIN(block_start_offset + image_length,
                                      BLOCK_SIZE);
      uint32_t block_length = block_end_offset - block_start_offset;

      // TODO
      switch (map_result) {
      case Policy::MAP_RESULT_HIT:
        {
          ctx->read_from_cache(block, block_start_offset, block_length);
        }
        break;
      case Policy::MAP_RESULT_MISS:
        ctx->read_from_image(image_offset, image_length);
        break;
      case Policy::MAP_RESULT_NEW:
      case Policy::MAP_RESULT_REPLACE:
        // TODO replace needs to demote first
        ctx->promote_and_read_from_cache(block, block_start_offset,
                                         block_length);
        break;
      default:
        assert(false);
      }

      image_offset += block_length;
      image_length -= block_length;
    }
  }

  // advance the policy statistics
  m_policy->tick();
  ctx->activate();
}

template <typename I>
void FileImageCache<I>::aio_write(Extents &&image_extents,
                                  bufferlist&& bl,
                                  int fadvise_flags,
                                  Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  if (!is_block_aligned(image_extents)) {
    // For clients that don't use LBA extents, re-align the write request
    // to work with the cache
    ldout(cct, 20) << "aligning write to block size" << dendl;

    // TODO: for non-aligned extents, invalidate the associated block-aligned
    // regions in the cache (if any), send the aligned extents to the cache
    // and the un-aligned extents directly to back to librbd
  }

  // TODO invalidate written blocks until writethrough/back support added
  C_Gather *ctx = new C_Gather(cct, on_finish);
  Extents invalidate_extents(image_extents);
  invalidate(std::move(invalidate_extents), ctx->new_sub());

  m_image_writeback.aio_write(std::move(image_extents), std::move(bl),
                              fadvise_flags, ctx->new_sub());
  ctx->activate();
}

template <typename I>
void FileImageCache<I>::aio_discard(uint64_t offset, uint64_t length,
                                    bool skip_partial_discard, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "on_finish=" << on_finish << dendl;

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
  invalidate({{offset, length}}, ctx->new_sub());

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

  // chain the initialization of the meta, image, and journal stores
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r >= 0) {
        // TODO need to support dynamic image resizes
        m_policy->set_block_count(
          m_meta_store->offset_to_block(m_image_ctx.size));
      }
      on_finish->complete(r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      if (r < 0) {
        ctx->complete(r);
        return;
      }

      // TODO: do not enable journal store if writeback disabled
      m_journal_store = new JournalStore<I>(m_image_ctx, *m_meta_store);
      m_journal_store->init(ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      if (r < 0) {
        ctx->complete(r);
        return;
      }

      m_image_store = new ImageStore<I>(m_image_ctx, *m_meta_store);
      m_image_store->init(ctx);
    });
  m_meta_store = new MetaStore<I>(m_image_ctx, BLOCK_SIZE);
  m_meta_store->init(ctx);
}

template <typename I>
void FileImageCache<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // chain the shut down of the journal, image, and meta stores
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      delete m_journal_store;
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
  m_journal_store->shut_down(ctx);
}

template <typename I>
void FileImageCache<I>::invalidate(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  invalidate({{0, m_image_ctx.size}}, on_finish);
}

template <typename I>
void FileImageCache<I>::invalidate(Extents&& image_extents,
                                   Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": image_extents=" << image_extents << dendl;

  // TODO
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint64_t block = m_meta_store->offset_to_block(image_offset);
      uint32_t block_start_offset = image_offset % BLOCK_SIZE;
      uint32_t block_end_offset = MIN(block_start_offset + image_length,
                                      BLOCK_SIZE);
      uint32_t block_length = block_end_offset - block_start_offset;

      m_policy->invalidate(block);

      image_offset += block_length;
      image_length -= block_length;
    }
  }

  // dump specific extents within the cache
  on_finish->complete(0);
}

template <typename I>
void FileImageCache<I>::flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO

  // internal flush -- nothing to writeback but make sure
  // in-flight IO is flushed
  aio_flush(on_finish);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::FileImageCache<librbd::ImageCtx>;
