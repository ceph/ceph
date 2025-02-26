// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/ImageWriteback.h"
#include "LogEntry.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::WriteLogEntry: " \
                           << this << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

void WriteLogEntry::writeback(
    librbd::cache::ImageWritebackInterface &image_writeback, Context *ctx) {
  /* Pass a copy of the pmem buffer to ImageWriteback (which may hang on to the
   * bl even after flush()). */
  bufferlist entry_bl;
  buffer::list entry_bl_copy;
  copy_cache_bl(&entry_bl_copy);
  entry_bl_copy.begin(0).copy(write_bytes(), entry_bl);
  image_writeback.aio_write({{ram_entry.image_offset_bytes,
                              ram_entry.write_bytes}},
                            std::move(entry_bl), 0, ctx);
}

void WriteLogEntry::init_cache_bp() {
  ceph_assert(!this->cache_bp.have_raw());
  cache_bp = buffer::ptr(buffer::create_static(this->write_bytes(),
                                               (char*)this->cache_buffer));
}

void WriteLogEntry::init_bl(buffer::ptr &bp, buffer::list &bl) {
  if(!is_writesame) {
    bl.append(bp);
    return;
  }
  for (uint64_t i = 0; i < ram_entry.write_bytes / ram_entry.ws_datalen; i++) {
    bl.append(bp);
  }
  int trailing_partial = ram_entry.write_bytes % ram_entry.ws_datalen;
  if (trailing_partial) {
    bl.append(bp, 0, trailing_partial);
  }
}

void WriteLogEntry::init_cache_buffer(
    std::vector<WriteBufferAllocation>::iterator allocation) {
  this->ram_entry.write_data = allocation->buffer_oid;
  ceph_assert(!TOID_IS_NULL(this->ram_entry.write_data));
  cache_buffer = D_RW(this->ram_entry.write_data);
}

buffer::list& WriteLogEntry::get_cache_bl() {
  if (0 == bl_refs) {
    std::lock_guard locker(m_entry_bl_lock);
    if (0 == bl_refs) {
      //init pmem bufferlist
      cache_bl.clear();
      init_cache_bp();
      ceph_assert(cache_bp.have_raw());
      int before_bl = cache_bp.raw_nref();
      this->init_bl(cache_bp, cache_bl);
      int after_bl = cache_bp.raw_nref();
      bl_refs = after_bl - before_bl;
    }
    ceph_assert(0 != bl_refs);
  }
  return cache_bl;
}

void WriteLogEntry::copy_cache_bl(bufferlist *out_bl) {
  this->get_cache_bl();
  // cache_bp is now initialized
  ceph_assert(cache_bp.length() == cache_bp.raw_length());
  buffer::ptr cloned_bp = cache_bp.begin_deep().get_ptr(cache_bp.length());
  out_bl->clear();
  this->init_bl(cloned_bp, *out_bl);
}

unsigned int WriteLogEntry::reader_count() const {
  if (cache_bp.have_raw()) {
    return (cache_bp.raw_nref() - bl_refs - 1);
  } else {
    return 0;
  }
}

void WriteSameLogEntry::writeback(
    librbd::cache::ImageWritebackInterface &image_writeback, Context *ctx) {
  bufferlist entry_bl;
  buffer::list entry_bl_copy;
  copy_cache_bl(&entry_bl_copy);
  entry_bl_copy.begin(0).copy(write_bytes(), entry_bl);
  image_writeback.aio_writesame(ram_entry.image_offset_bytes,
                                ram_entry.write_bytes,
                                std::move(entry_bl), 0, ctx);
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd
