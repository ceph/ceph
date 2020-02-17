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

std::ostream &format(std::ostream &os, const GenericLogEntry &entry) {
  os << "ram_entry=[" << entry.ram_entry << "], "
     << "pmem_entry=" << (void*)entry.pmem_entry << ", "
     << "log_entry_index=" << entry.log_entry_index << ", "
     << "completed=" << entry.completed;
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const GenericLogEntry &entry) {
  return entry.format(os, entry);
}

} // namespace rwl
} // namespace cache
} // namespace librbd
