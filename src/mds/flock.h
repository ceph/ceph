// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MDS_FLOCK_H
#define CEPH_MDS_FLOCK_H

#include "include/ceph_fs.h" // for ceph_filelock
#include "include/client_t.h"

#include <map>
#include <vector>
#include <cstdint>
#include <iosfwd>

std::ostream& operator<<(std::ostream& out, const ceph_filelock& l);

inline bool ceph_filelock_owner_equal(const ceph_filelock& l, const ceph_filelock& r)
{
  if (l.client != r.client || l.owner != r.owner)
    return false;
  // The file lock is from old client if the most significant bit of
  // 'owner' is not set. Old clients use both 'owner' and 'pid' to
  // identify the owner of lock.
  if (l.owner & (1ULL << 63))
    return true;
  return l.pid == r.pid;
}

inline int ceph_filelock_owner_compare(const ceph_filelock& l, const ceph_filelock& r)
{
  if (l.client != r.client)
    return l.client > r.client ? 1 : -1;
  if (l.owner != r.owner)
    return l.owner > r.owner ? 1 : -1;
  if (l.owner & (1ULL << 63))
    return 0;
  if (l.pid != r.pid)
    return l.pid > r.pid ? 1 : -1;
  return 0;
}

inline int ceph_filelock_compare(const ceph_filelock& l, const ceph_filelock& r)
{
  int ret = ceph_filelock_owner_compare(l, r);
  if (ret)
    return ret;
  if (l.start != r.start)
    return l.start > r.start ? 1 : -1;
  if (l.length != r.length)
    return l.length > r.length ? 1 : -1;
  if (l.type != r.type)
    return l.type > r.type ? 1 : -1;
  return 0;
}

inline bool operator<(const ceph_filelock& l, const ceph_filelock& r)
{
  return ceph_filelock_compare(l, r) < 0;
}

inline bool operator==(const ceph_filelock& l, const ceph_filelock& r) {
  return ceph_filelock_compare(l, r) == 0;
}

inline bool operator!=(const ceph_filelock& l, const ceph_filelock& r) {
  return ceph_filelock_compare(l, r) != 0;
}

class ceph_lock_state_t {
public:
  explicit ceph_lock_state_t(CephContext *cct_, int type_) : cct(cct_), type(type_) {}
  ceph_lock_state_t() : cct(NULL), type(0) {}
  ~ceph_lock_state_t();
  /**
   * Check if a lock is on the waiting_locks list.
   *
   * @param fl The filelock to check for
   * @returns True if the lock is waiting, false otherwise
   */
  bool is_waiting(const ceph_filelock &fl) const;
  /**
   * Remove a lock from the waiting_locks list
   *
   * @param fl The filelock to remove
   */
  void remove_waiting(const ceph_filelock& fl);
  /*
   * Try to set a new lock. If it's blocked and wait_on_fail is true,
   * add the lock to waiting_locks.
   * The lock needs to be of type CEPH_LOCK_EXCL or CEPH_LOCK_SHARED.
   * This may merge previous locks, or convert the type of already-owned
   * locks.
   *
   * @param new_lock The lock to set
   * @param wait_on_fail whether to wait until the lock can be set.
   * Otherwise it fails immediately when blocked.
   *
   * @returns true if set, false if not set.
   */
  bool add_lock(ceph_filelock& new_lock, bool wait_on_fail, bool replay,
		bool *deadlock);
  /**
   * See if a lock is blocked by existing locks. If the lock is blocked,
   * it will be set to the value of the first blocking lock. Otherwise,
   * it will be returned unchanged, except for setting the type field
   * to CEPH_LOCK_UNLOCK.
   *
   * @param testing_lock The lock to check for conflicts on.
   */
  void look_for_lock(ceph_filelock& testing_lock);

  /*
   * Remove lock(s) described in old_lock. This may involve splitting a
   * previous lock or making a previous lock smaller.
   *
   * @param removal_lock The lock to remove
   */
  void remove_lock(const ceph_filelock removal_lock);

  bool remove_all_from(client_t client);

  void encode(ceph::bufferlist& bl) const;
  void decode(ceph::bufferlist::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::vector<ceph_lock_state_t> generate_test_instances();
  bool empty() const {
    return held_locks.empty() && waiting_locks.empty() &&
	   client_held_lock_counts.empty() &&
	   client_waiting_lock_counts.empty();
  }

  std::multimap<uint64_t, ceph_filelock> held_locks;    // current locks
  std::multimap<uint64_t, ceph_filelock> waiting_locks; // locks waiting for other locks
  // both of the above are keyed by starting offset
  std::map<client_t, int> client_held_lock_counts;
  std::map<client_t, int> client_waiting_lock_counts;

private:
  using lock_map = std::multimap<uint64_t, ceph_filelock>;
  using lock_iterator = lock_map::iterator;
  struct lock_overlaps;

  static const unsigned MAX_DEADLK_DEPTH = 5;

  /**
   * Check if adding the lock causes deadlock
   *
   * @param fl The blocking filelock 
   * @param overlapping_locks all overlapping locks, classified by owner
   * @param first_fl first lock in the deadlock search
   * @param depth recursion depth
   */
  bool is_deadlock(const ceph_filelock& fl,
                   const lock_overlaps& overlapping_locks,
                   const ceph_filelock *first_fl = nullptr,
                   unsigned depth = 0) const;

  /**
   * Add a lock to the waiting_locks list
   *
   * @param fl The filelock to add
   */
  void add_waiting(const ceph_filelock& fl);

  /**
   * Adjust old locks owned by a single process so that process can set
   * a new lock of different type. Handle any changes needed to the old locks
   * (and the new lock) so that once the new lock is inserted into the 
   * held_locks list the process has a coherent, non-fragmented set of lock
   * ranges. Make sure any overlapping locks are combined, trimmed, and removed
   * as needed.
   * This function should only be called once you know the lock will be
   * inserted, as it DOES adjust new_lock. You can call this function
   * with no overlaps, in which case it does nothing. The iterators in
   * overlaps may be invalidated and must not be used after this call.
   *
   * @param overlaps locks owned by the same process that overlap or neighbor
   *    new_lock
   * @param new_lock The new lock the process has requested.
   */
  void adjust_locks(const lock_overlaps& overlaps, ceph_filelock& new_lock);

  // Get the latest-starting lock that covers last_offset:
  lock_iterator get_last_before(uint64_t last_offset, lock_map& locks);

  /*
   * See if an iterator's lock covers any of the same bounds as a given range
   * Rules: locks cover "length" bytes from "start", so the last covered
   * byte is at start + length - 1.
   * If the length is 0, the lock covers from "start" to the end of the file.
   */
  bool share_space(const lock_iterator& iter, uint64_t start,
                   uint64_t last_offset);

  bool share_space(const lock_iterator& iter,
                   const ceph_filelock& requested_lock) {
    const auto last_offset = requested_lock.length
      ? requested_lock.start + requested_lock.length - 1
      : uint64_t(-1);

    return share_space(iter, requested_lock.start, last_offset);
  }

  /**
   * Get all locks overlapping with the given lock's range, classified by
   * owner. Neighboring locks are collected only when requested.
   *
   * @param requested_lock the lock to compare with
   */
  lock_overlaps get_overlapping_locks(const ceph_filelock& requested_lock,
                                      bool collect_neighbors = false);

  CephContext *cct;
  int type;
};
WRITE_CLASS_ENCODER(ceph_lock_state_t)

std::ostream& operator<<(std::ostream &out, const ceph_lock_state_t &l);

#endif
