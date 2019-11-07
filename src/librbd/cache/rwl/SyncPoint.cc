// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SyncPoint.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::SyncPoint: " << this << " " \
			   <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

template <typename T>
SyncPoint<T>::SyncPoint(T &rwl, const uint64_t sync_gen_num)
  : rwl(rwl), log_entry(std::make_shared<SyncPointLogEntry>(sync_gen_num)) {
  prior_log_entries_persisted = new C_Gather(rwl.m_image_ctx.cct, nullptr);
  sync_point_persist = new C_Gather(rwl.m_image_ctx.cct, nullptr);
  on_sync_point_appending.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  on_sync_point_persisted.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "sync point " << sync_gen_num << dendl;
  }
}

template <typename T>
SyncPoint<T>::~SyncPoint() {
  assert(on_sync_point_appending.empty());
  assert(on_sync_point_persisted.empty());
  assert(!earlier_sync_point);
}

template <typename T>
std::ostream &SyncPoint<T>::format(std::ostream &os) const {
  os << "log_entry=[" << *log_entry << "], "
     << "earlier_sync_point=" << earlier_sync_point << ", "
     << "later_sync_point=" << later_sync_point << ", "
     << "final_op_sequence_num=" << final_op_sequence_num << ", "
     << "prior_log_entries_persisted=" << prior_log_entries_persisted << ", "
     << "prior_log_entries_persisted_complete=" << prior_log_entries_persisted_complete << ", "
     << "append_scheduled=" << append_scheduled << ", "
     << "appending=" << appending << ", "
     << "on_sync_point_appending=" << on_sync_point_appending.size() << ", "
     << "on_sync_point_persisted=" << on_sync_point_persisted.size() << "";
  return os;
};

} // namespace rwl
} // namespace cache
} // namespace librbd
