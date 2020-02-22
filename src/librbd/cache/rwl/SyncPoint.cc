// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SyncPoint.h"

#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::SyncPoint: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

SyncPoint::SyncPoint(uint64_t sync_gen_num, CephContext *cct)
  : log_entry(std::make_shared<SyncPointLogEntry>(sync_gen_num)), m_cct(cct) {
  prior_log_entries_persisted = new C_Gather(cct, nullptr);
  sync_point_persist = new C_Gather(cct, nullptr);
  on_sync_point_appending.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  on_sync_point_persisted.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  ldout(m_cct, 20) << "sync point " << sync_gen_num << dendl;
}

SyncPoint::~SyncPoint() {
  ceph_assert(on_sync_point_appending.empty());
  ceph_assert(on_sync_point_persisted.empty());
  ceph_assert(!earlier_sync_point);
}

std::ostream &operator<<(std::ostream &os,
                         const SyncPoint &p) {
  os << "log_entry=[" << *p.log_entry << "], "
     << "earlier_sync_point=" << p.earlier_sync_point << ", "
     << "later_sync_point=" << p.later_sync_point << ", "
     << "final_op_sequence_num=" << p.final_op_sequence_num << ", "
     << "prior_log_entries_persisted=" << p.prior_log_entries_persisted << ", "
     << "prior_log_entries_persisted_complete=" << p.prior_log_entries_persisted_complete << ", "
     << "append_scheduled=" << p.append_scheduled << ", "
     << "appending=" << p.appending << ", "
     << "on_sync_point_appending=" << p.on_sync_point_appending.size() << ", "
     << "on_sync_point_persisted=" << p.on_sync_point_persisted.size() << "";
  return os;
}

} // namespace rwl
} // namespace cache
} // namespace librbd
