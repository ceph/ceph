// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LogOperation.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ssd::LogOperation: " \
                           << this << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

void DiscardLogOperation::init_op(
    uint64_t current_sync_gen, bool persist_on_flush,
    uint64_t last_op_sequence_num, Context *write_persist,
    Context *write_append) {
  log_entry->init(current_sync_gen, persist_on_flush, last_op_sequence_num);
  if (persist_on_flush) {
    this->on_write_append = new LambdaContext(
        [write_persist, write_append] (int r) {
        write_append->complete(r);
        write_persist->complete(r);
        });
  } else {
    this->on_write_append = write_append;
    this->on_write_persist = write_persist;
  }
}

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd
