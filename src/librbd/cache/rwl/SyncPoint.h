// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_SYNC_POINT_H
#define CEPH_LIBRBD_CACHE_RWL_SYNC_POINT_H 

#include "librbd/ImageCtx.h"
#include "librbd/cache/rwl/LogEntry.h"
#include "librbd/cache/rwl/Types.h"

namespace librbd {
namespace cache {
namespace rwl {

class SyncPoint {
public:
  std::shared_ptr<SyncPointLogEntry> log_entry;
  /* Use lock for earlier/later links */
  std::shared_ptr<SyncPoint> earlier_sync_point; /* NULL if earlier has completed */
  std::shared_ptr<SyncPoint> later_sync_point;
  uint64_t final_op_sequence_num = 0;
  /* A sync point can't appear in the log until all the writes bearing
   * it and all the prior sync points have been appended and
   * persisted.
   *
   * Writes bearing this sync gen number and the prior sync point will be
   * sub-ops of this Gather. This sync point will not be appended until all
   * these complete to the point where their persist order is guaranteed. */
  C_Gather *prior_log_entries_persisted;
  int prior_log_entries_persisted_result = 0;
  int prior_log_entries_persisted_complete = false;
  /* The finisher for this will append the sync point to the log.  The finisher
   * for m_prior_log_entries_persisted will be a sub-op of this. */
  C_Gather *sync_point_persist;
  bool append_scheduled = false;
  bool appending = false;
  /* Signal these when this sync point is appending to the log, and its order
   * of appearance is guaranteed. One of these is is a sub-operation of the
   * next sync point's m_prior_log_entries_persisted Gather. */
  std::vector<Context*> on_sync_point_appending;
  /* Signal these when this sync point is appended and persisted. User
   * aio_flush() calls are added to this. */
  std::vector<Context*> on_sync_point_persisted;

  SyncPoint(uint64_t sync_gen_num, CephContext *cct);
  ~SyncPoint();
  SyncPoint(const SyncPoint&) = delete;
  SyncPoint &operator=(const SyncPoint&) = delete;

private:
  CephContext *m_cct;
  friend std::ostream &operator<<(std::ostream &os,
                                  const SyncPoint &p);
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_RWL_SYNC_POINT_H
