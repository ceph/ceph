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
  m_prior_log_entries_persisted = new C_Gather(cct, nullptr);
  m_sync_point_persist = new C_Gather(cct, nullptr);
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
     << "m_final_op_sequence_num=" << p.m_final_op_sequence_num << ", "
     << "m_prior_log_entries_persisted=" << p.m_prior_log_entries_persisted << ", "
     << "m_prior_log_entries_persisted_complete=" << p.m_prior_log_entries_persisted_complete << ", "
     << "m_append_scheduled=" << p.m_append_scheduled << ", "
     << "appending=" << p.appending << ", "
     << "on_sync_point_appending=" << p.on_sync_point_appending.size() << ", "
     << "on_sync_point_persisted=" << p.on_sync_point_persisted.size() << "";
  return os;
}

void SyncPoint::persist_gather_set_finisher(Context *ctx) {
  m_append_scheduled = true;
  /* All prior sync points that are still in this list must already be scheduled for append */
  std::shared_ptr<SyncPoint> previous = earlier_sync_point;
  while (previous) {
    ceph_assert(previous->m_append_scheduled);
    previous = previous->earlier_sync_point;
  }

  m_sync_point_persist->set_finisher(ctx);
}

void SyncPoint::persist_gather_activate() {
  m_sync_point_persist->activate();
}

Context* SyncPoint::persist_gather_new_sub() {
  return m_sync_point_persist->new_sub();
}

void SyncPoint::prior_persisted_gather_activate() {
  m_prior_log_entries_persisted->activate();
}

Context* SyncPoint::prior_persisted_gather_new_sub() {
  return m_prior_log_entries_persisted->new_sub();
}

void SyncPoint::prior_persisted_gather_set_finisher() {
  Context *sync_point_persist_ready = persist_gather_new_sub();
  std::shared_ptr<SyncPoint> sp = shared_from_this();
  m_prior_log_entries_persisted->
    set_finisher(new LambdaContext([this, sp, sync_point_persist_ready](int r) {
      ldout(m_cct, 20) << "Prior log entries persisted for sync point =["
                       << sp << "]" << dendl;
      sp->m_prior_log_entries_persisted_result = r;
      sp->m_prior_log_entries_persisted_complete = true;
      sync_point_persist_ready->complete(r);
    }));
}

void SyncPoint::add_in_on_persisted_ctxs(Context* ctx) {
  on_sync_point_persisted.push_back(ctx);
}

void SyncPoint::add_in_on_appending_ctxs(Context* ctx) {
  on_sync_point_appending.push_back(ctx);
}

void SyncPoint::setup_earlier_sync_point(std::shared_ptr<SyncPoint> sync_point,
                                         uint64_t last_op_sequence_num) {
    earlier_sync_point = sync_point;
    log_entry->prior_sync_point_flushed = false;
    earlier_sync_point->log_entry->next_sync_point_entry = log_entry;
    earlier_sync_point->later_sync_point = shared_from_this();
    earlier_sync_point->m_final_op_sequence_num = last_op_sequence_num;
    if (!earlier_sync_point->appending) {
      /* Append of new sync point deferred until old sync point is appending */
      earlier_sync_point->add_in_on_appending_ctxs(prior_persisted_gather_new_sub());
    }
}

} // namespace rwl
} // namespace cache
} // namespace librbd
