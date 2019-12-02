// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "Types.h"
#include "common/ceph_context.h"
#include "include/Context.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::Types: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {

namespace cache {

namespace rwl {

DeferredContexts::~DeferredContexts() {
  finish_contexts(nullptr, contexts, 0);
}

void DeferredContexts::add(Context* ctx) {
    contexts.push_back(ctx);
}

bool WriteLogPmemEntry::is_sync_point() {
  return sync_point;
}

bool WriteLogPmemEntry::is_discard() {
  return discard;
}

bool WriteLogPmemEntry::is_writesame() {
  return writesame;
}

bool WriteLogPmemEntry::is_write() {
  /* Log entry is a basic write */
  return !is_sync_point() && !is_discard() && !is_writesame();
}

bool WriteLogPmemEntry::is_writer() {
  /* Log entry is any type that writes data */
  return is_write() || is_discard() || is_writesame();
}

const uint64_t WriteLogPmemEntry::get_offset_bytes() {
  return image_offset_bytes;
}

const uint64_t WriteLogPmemEntry::get_write_bytes() {
  return write_bytes;
}

std::ostream& operator<<(std::ostream& os,
                         const WriteLogPmemEntry &entry) {
  os << "entry_valid=" << (bool)entry.entry_valid << ", "
     << "sync_point=" << (bool)entry.sync_point << ", "
     << "sequenced=" << (bool)entry.sequenced << ", "
     << "has_data=" << (bool)entry.has_data << ", "
     << "discard=" << (bool)entry.discard << ", "
     << "writesame=" << (bool)entry.writesame << ", "
     << "sync_gen_number=" << entry.sync_gen_number << ", "
     << "write_sequence_number=" << entry.write_sequence_number << ", "
     << "image_offset_bytes=" << entry.image_offset_bytes << ", "
     << "write_bytes=" << entry.write_bytes << ", "
     << "ws_datalen=" << entry.ws_datalen << ", "
     << "entry_index=" << entry.entry_index;
  return os;
};

} // namespace rwl
} // namespace cache
} // namespace librbd
