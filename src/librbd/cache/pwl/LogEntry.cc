// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "LogEntry.h"
#include "librbd/cache/ImageWriteback.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::LogEntry: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {

std::ostream& GenericLogEntry::format(std::ostream &os) const {
  os << "ram_entry=[" << ram_entry
     << "], cache_entry=" << (void*)cache_entry
     << ", log_entry_index=" << log_entry_index
     << ", completed=" << completed;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const GenericLogEntry &entry) {
  return entry.format(os);
}

std::ostream& SyncPointLogEntry::format(std::ostream &os) const {
  os << "(Sync Point) ";
  GenericLogEntry::format(os);
  os << ", writes=" << writes
     << ", bytes=" << bytes
     << ", writes_completed=" << writes_completed
     << ", writes_flushed=" << writes_flushed
     << ", prior_sync_point_flushed=" << prior_sync_point_flushed
     << ", next_sync_point_entry=" << next_sync_point_entry;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const SyncPointLogEntry &entry) {
  return entry.format(os);
}

bool GenericWriteLogEntry::can_writeback() const {
  return (this->completed &&
          (ram_entry.is_sequenced() ||
           (sync_point_entry &&
            sync_point_entry->completed)));
}

std::ostream& GenericWriteLogEntry::format(std::ostream &os) const {
  GenericLogEntry::format(os);
  os << ", sync_point_entry=[";
  if (sync_point_entry) {
    os << *sync_point_entry;
  } else {
    os << "nullptr";
  }
  os << "], referring_map_entries=" << referring_map_entries;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const GenericWriteLogEntry &entry) {
  return entry.format(os);
}

void WriteLogEntry::init(bool has_data,
                         uint64_t current_sync_gen,
                         uint64_t last_op_sequence_num, bool persist_on_flush) {
  ram_entry.set_has_data(has_data);
  ram_entry.sync_gen_number = current_sync_gen;
  if (persist_on_flush) {
    /* Persist on flush. Sequence #0 is never used. */
    ram_entry.write_sequence_number = 0;
  } else {
     /* Persist on write */
     ram_entry.write_sequence_number = last_op_sequence_num;
     ram_entry.set_sequenced(true);
  }
  ram_entry.set_sync_point(false);
  ram_entry.set_discard(false);
}

std::ostream& WriteLogEntry::format(std::ostream &os) const {
  os << "(Write) ";
  GenericWriteLogEntry::format(os);
  os << ", cache_buffer=" << (void*)cache_buffer;
  os << ", cache_bp=" << cache_bp;
  os << ", bl_refs=" << bl_refs;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const WriteLogEntry &entry) {
  return entry.format(os);
}

void DiscardLogEntry::writeback(
    librbd::cache::ImageWritebackInterface &image_writeback, Context *ctx) {
  image_writeback.aio_discard(ram_entry.image_offset_bytes,
                              ram_entry.write_bytes,
                              m_discard_granularity_bytes, ctx);
}

void DiscardLogEntry::init(uint64_t current_sync_gen, bool persist_on_flush,
                           uint64_t last_op_sequence_num) {
  ram_entry.sync_gen_number = current_sync_gen;
  if (persist_on_flush) {
    /* Persist on flush. Sequence #0 is never used. */
    ram_entry.write_sequence_number = 0;
  } else {
    /* Persist on write */
    ram_entry.write_sequence_number = last_op_sequence_num;
    ram_entry.set_sequenced(true);
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

} // namespace pwl
} // namespace cache
} // namespace librbd
