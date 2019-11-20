// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "LogEntry.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::LogEntry: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {

namespace cache {

namespace rwl {

bool GenericLogEntry::is_sync_point() {
  return ram_entry.is_sync_point();
}

bool GenericLogEntry::is_discard() {
  return ram_entry.is_discard();
}

bool GenericLogEntry::is_writesame() {
  return ram_entry.is_writesame();
}

bool GenericLogEntry::is_write() {
  return ram_entry.is_write();
}

bool GenericLogEntry::is_writer() {
  return ram_entry.is_writer();
}

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
};

std::ostream &operator<<(std::ostream &os,
                                const SyncPointLogEntry &entry) {
  return entry.format(os);
}

std::ostream& GeneralWriteLogEntry::format(std::ostream &os) const {
  GenericLogEntry::format(os);
  os << ", "
     << "sync_point_entry=[";
  if (sync_point_entry) {
    os << *sync_point_entry;
  } else {
    os << "nullptr";
  }
  os << "], "
     << "referring_map_entries=" << referring_map_entries << ", "
     << "flushing=" << flushing << ", "
     << "flushed=" << flushed;
  return os;
};

std::ostream &operator<<(std::ostream &os,
                                const GeneralWriteLogEntry &entry) {
  return entry.format(os);
}

void WriteLogEntry::init_pmem_bp() {
  assert(!pmem_bp.have_raw());
  pmem_bp = buffer::ptr(buffer::create_static(this->write_bytes(), (char*)pmem_buffer));
}

void WriteLogEntry::init_pmem_bl() {
  pmem_bl.clear();
  init_pmem_bp();
  assert(pmem_bp.have_raw());
  int before_bl = pmem_bp.raw_nref();
  this->init_bl(pmem_bp, pmem_bl);
  int after_bl = pmem_bp.raw_nref();
  bl_refs = after_bl - before_bl;
}

unsigned int WriteLogEntry::reader_count() {
  if (pmem_bp.have_raw()) {
    return (pmem_bp.raw_nref() - bl_refs - 1);
  } else {
    return 0;
  }
}

/* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
buffer::list& WriteLogEntry::get_pmem_bl(ceph::mutex &entry_bl_lock) {
  if (0 == bl_refs) {
    std::lock_guard locker(entry_bl_lock);
    if (0 == bl_refs) {
      init_pmem_bl();
    }
    ceph_assert(0 != bl_refs);
  }
  return pmem_bl;
};

/* Constructs a new bl containing copies of pmem_bp */
void WriteLogEntry::copy_pmem_bl(ceph::mutex &entry_bl_lock, bufferlist *out_bl) {
  this->get_pmem_bl(entry_bl_lock);
  /* pmem_bp is now initialized */
  buffer::ptr cloned_bp(pmem_bp.clone());
  out_bl->clear();
  this->init_bl(cloned_bp, *out_bl);
}

std::ostream& WriteLogEntry::format(std::ostream &os) const {
  os << "(Write) ";
  GeneralWriteLogEntry::format(os);
  os << ", "
     << "pmem_buffer=" << (void*)pmem_buffer << ", ";
  os << "pmem_bp=" << pmem_bp << ", ";
  os << "pmem_bl=" << pmem_bl << ", ";
  os << "bl_refs=" << bl_refs;
  return os;
};

std::ostream &operator<<(std::ostream &os,
                                const WriteLogEntry &entry) {
  return entry.format(os);
}

} // namespace rwl
} // namespace cache
} // namespace librbd
