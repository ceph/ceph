// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "LogEntry.h"
#include "librbd/cache/ImageWriteback.h"

#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::LogEntry: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {

namespace cache {

namespace rwl {

std::ostream& GenericLogEntry::format(std::ostream &os) const {
  os << "ram_entry=[" << ram_entry << "], "
     << "pmem_entry=" << (void*)pmem_entry << ", "
     << "log_entry_index=" << log_entry_index << ", "
     << "completed=" << completed;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const GenericLogEntry &entry) {
  return entry.format(os);
}

std::ostream& SyncPointLogEntry::format(std::ostream &os) const {
  os << "(Sync Point) ";
  GenericLogEntry::format(os);
  os << ", "
     << "writes=" << writes << ", "
     << "bytes=" << bytes << ", "
     << "writes_completed=" << writes_completed << ", "
     << "writes_flushed=" << writes_flushed << ", "
     << "prior_sync_point_flushed=" << prior_sync_point_flushed << ", "
     << "next_sync_point_entry=" << next_sync_point_entry;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const SyncPointLogEntry &entry) {
  return entry.format(os);
}

bool GenericWriteLogEntry::can_writeback() const {
  return (this->completed &&
          (ram_entry.sequenced ||
           (sync_point_entry &&
            sync_point_entry->completed)));
}

std::ostream& GenericWriteLogEntry::format(std::ostream &os) const {
  GenericLogEntry::format(os);
  os << ", "
     << "sync_point_entry=[";
  if (sync_point_entry) {
    os << *sync_point_entry;
  } else {
    os << "nullptr";
  }
  os << "], "
     << "referring_map_entries=" << referring_map_entries;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const GenericWriteLogEntry &entry) {
  return entry.format(os);
}

void WriteLogEntry::init(bool has_data, std::vector<WriteBufferAllocation>::iterator allocation,
                         uint64_t current_sync_gen, uint64_t last_op_sequence_num, bool persist_on_flush) {
  ram_entry.has_data = 1;
  ram_entry.write_data = allocation->buffer_oid;
  ceph_assert(!TOID_IS_NULL(ram_entry.write_data));
  pmem_buffer = D_RW(ram_entry.write_data);
  ram_entry.sync_gen_number = current_sync_gen;
  if (persist_on_flush) {
    /* Persist on flush. Sequence #0 is never used. */
    ram_entry.write_sequence_number = 0;
  } else {
     /* Persist on write */
     ram_entry.write_sequence_number = last_op_sequence_num;
     ram_entry.sequenced = 1;
  }
  ram_entry.sync_point = 0;
  ram_entry.discard = 0;
}

void WriteLogEntry::init_pmem_bp() {
  ceph_assert(!pmem_bp.have_raw());
  pmem_bp = buffer::ptr(buffer::create_static(this->write_bytes(), (char*)pmem_buffer));
}

void WriteLogEntry::init_pmem_bl() {
  pmem_bl.clear();
  init_pmem_bp();
  ceph_assert(pmem_bp.have_raw());
  int before_bl = pmem_bp.raw_nref();
  this->init_bl(pmem_bp, pmem_bl);
  int after_bl = pmem_bp.raw_nref();
  bl_refs = after_bl - before_bl;
}

unsigned int WriteLogEntry::reader_count() const {
  if (pmem_bp.have_raw()) {
    return (pmem_bp.raw_nref() - bl_refs - 1);
  } else {
    return 0;
  }
}

/* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
buffer::list& WriteLogEntry::get_pmem_bl() {
  if (0 == bl_refs) {
    std::lock_guard locker(m_entry_bl_lock);
    if (0 == bl_refs) {
      init_pmem_bl();
    }
    ceph_assert(0 != bl_refs);
  }
  return pmem_bl;
}

/* Constructs a new bl containing copies of pmem_bp */
void WriteLogEntry::copy_pmem_bl(bufferlist *out_bl) {
  this->get_pmem_bl();
  /* pmem_bp is now initialized */
  buffer::ptr cloned_bp(pmem_bp.clone());
  out_bl->clear();
  this->init_bl(cloned_bp, *out_bl);
}

void WriteLogEntry::writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                              Context *ctx) {
  /* Pass a copy of the pmem buffer to ImageWriteback (which may hang on to the bl even after flush()). */
  bufferlist entry_bl;
  buffer::list entry_bl_copy;
  copy_pmem_bl(&entry_bl_copy);
  entry_bl_copy.begin(0).copy(write_bytes(), entry_bl);
  image_writeback.aio_write({{ram_entry.image_offset_bytes, ram_entry.write_bytes}},
                            std::move(entry_bl), 0, ctx);
}

std::ostream& WriteLogEntry::format(std::ostream &os) const {
  os << "(Write) ";
  GenericWriteLogEntry::format(os);
  os << ", "
     << "pmem_buffer=" << (void*)pmem_buffer << ", ";
  os << "pmem_bp=" << pmem_bp << ", ";
  os << "pmem_bl=" << pmem_bl << ", ";
  os << "bl_refs=" << bl_refs;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const WriteLogEntry &entry) {
  return entry.format(os);
}

void DiscardLogEntry::writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                                Context *ctx) {
  image_writeback.aio_discard(ram_entry.image_offset_bytes, ram_entry.write_bytes,
                              m_discard_granularity_bytes, ctx);
}

void DiscardLogEntry::init(uint64_t current_sync_gen, bool persist_on_flush, uint64_t last_op_sequence_num) {
  ram_entry.sync_gen_number = current_sync_gen;
  if (persist_on_flush) {
    /* Persist on flush. Sequence #0 is never used. */
    ram_entry.write_sequence_number = 0;
  } else {
    /* Persist on write */
    ram_entry.write_sequence_number = last_op_sequence_num;
    ram_entry.sequenced = 1;
  }
}

std::ostream &DiscardLogEntry::format(std::ostream &os) const {
  os << "(Discard) ";
  GenericWriteLogEntry::format(os);
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const DiscardLogEntry &entry) {
  return entry.format(os);
}

void WriteSameLogEntry::init_bl(buffer::ptr &bp, buffer::list &bl) {
  for (uint64_t i = 0; i < ram_entry.write_bytes / ram_entry.ws_datalen; i++) {
    bl.append(bp);
  }
  int trailing_partial = ram_entry.write_bytes % ram_entry.ws_datalen;
  if (trailing_partial) {
    bl.append(bp, 0, trailing_partial);
  }
}

void WriteSameLogEntry::writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                                  Context *ctx) {
  bufferlist entry_bl;
  buffer::list entry_bl_copy;
  copy_pmem_bl(&entry_bl_copy);
  entry_bl_copy.begin(0).copy(write_bytes(), entry_bl);
  image_writeback.aio_writesame(ram_entry.image_offset_bytes, ram_entry.write_bytes,
                                std::move(entry_bl), 0, ctx);
}

std::ostream &WriteSameLogEntry::format(std::ostream &os) const {
  os << "(WriteSame) ";
  WriteLogEntry::format(os);
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const WriteSameLogEntry &entry) {
  return entry.format(os);
}

} // namespace rwl
} // namespace cache
} // namespace librbd
