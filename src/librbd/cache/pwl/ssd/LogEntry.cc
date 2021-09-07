// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/pwl/ssd/LogEntry.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ssd::WriteLogEntry: " \
                           << this << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

void WriteLogEntry::init_cache_bl(
    bufferlist &src_bl, uint64_t off, uint64_t len) {
  cache_bl.clear();
  cache_bl.substr_of(src_bl, off, len);
}

buffer::list& WriteLogEntry::get_cache_bl() {
  return cache_bl;
}

void  WriteLogEntry::copy_cache_bl(bufferlist *out) {
  std::lock_guard locker(m_entry_bl_lock);
  *out = cache_bl;
}

void WriteLogEntry::remove_cache_bl() {
    std::lock_guard locker(m_entry_bl_lock);
    cache_bl.clear();
}

unsigned int WriteLogEntry::get_aligned_data_size() const {
  if (cache_bl.length()) {
    return round_up_to(cache_bl.length(), MIN_WRITE_ALLOC_SSD_SIZE);
  }
  return round_up_to(write_bytes(), MIN_WRITE_ALLOC_SSD_SIZE);
}

void WriteLogEntry::writeback_bl(
    librbd::cache::ImageWritebackInterface &image_writeback,
    Context *ctx, ceph::bufferlist&& bl) {
    image_writeback.aio_write({{ram_entry.image_offset_bytes,
                                ram_entry.write_bytes}},
                               std::move(bl), 0, ctx);
}

void WriteSameLogEntry::writeback_bl(
    librbd::cache::ImageWritebackInterface &image_writeback,
    Context *ctx, ceph::bufferlist &&bl) {
    image_writeback.aio_writesame(ram_entry.image_offset_bytes,
                                  ram_entry.write_bytes,
                                  std::move(bl), 0, ctx);
}

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd
