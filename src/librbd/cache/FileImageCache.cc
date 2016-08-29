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

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::FileImageCache: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

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

} // anonymous namespace

using namespace librbd::cache::file;

template <typename I>
FileImageCache<I>::FileImageCache(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_image_writeback(image_ctx),
    m_policy(new StupidPolicy<I>()) {
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
  // iterate through each block within the provided extents
  for (auto &extent : image_extents) {
    for (uint64_t offset = 0; offset < extent.second; offset += BLOCK_SIZE) {
      uint64_t block = m_meta_store->offset_to_block(extent.first + offset);
      bool partial_block = (extent.first % BLOCK_SIZE != 0 ||
                            extent.second != BLOCK_SIZE);

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

      // TODO
      switch (map_result) {
      case Policy::MAP_RESULT_HIT:
        // TODO read extent from cache
        break;
      case Policy::MAP_RESULT_MISS:
        // TODO add extent to defered list
        break;
      default:
        assert(false);
      }
    }
  }

  // advance the policy statistics
  m_policy->tick();

  // TODO
  m_image_writeback.aio_read(std::move(image_extents), bl, fadvise_flags,
                             on_finish);
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
    C_Gather *ctx = new C_Gather(cct, on_finish);
    Extents invalidate_extents(image_extents);
    invalidate(std::move(invalidate_extents), ctx->new_sub());

    m_image_writeback.aio_write(std::move(image_extents), std::move(bl),
                                fadvise_flags, ctx->new_sub());

    ctx->activate();
    return;
  }

  m_image_writeback.aio_write(std::move(image_extents), std::move(bl),
                              fadvise_flags, on_finish);
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
    C_Gather *ctx = new C_Gather(cct, on_finish);
    invalidate({{offset, length}}, ctx->new_sub());

    m_image_writeback.aio_discard(offset, length, skip_partial_discard, ctx->new_sub());

    ctx->activate();
    return;
  }

  m_image_writeback.aio_discard(offset, length, skip_partial_discard, on_finish);
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
      if (r < 0) {
        on_finish->complete(r);
        return;
      }

      // TODO: do not enable journal store if writeback disabled
      m_journal_store = new JournalStore<I>(m_image_ctx, *m_meta_store);
      m_journal_store->init(on_finish);
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

  // dump cache contents (don't have anything)
  on_finish->complete(0);
}

template <typename I>
void FileImageCache<I>::invalidate(Extents&& image_extents,
                                   Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": image_extents=" << image_extents << dendl;

  // TODO

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
