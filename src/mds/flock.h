// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDS_FLOCK_H
#define CEPH_MDS_FLOCK_H

#include <errno.h>

#include "common/debug.h"
#include "mdstypes.h"


inline ostream& operator<<(ostream& out, const ceph_filelock& l) {
  out << "start: " << l.start << ", length: " << l.length
      << ", client: " << l.client << ", owner: " << l.owner
      << ", pid: " << l.pid << ", type: " << (int)l.type
      << std::endl;
  return out;
}

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
  CephContext *cct;
  int type;
public:
  explicit ceph_lock_state_t(CephContext *cct_, int type_) : cct(cct_), type(type_) {}
  ~ceph_lock_state_t();
  multimap<uint64_t, ceph_filelock> held_locks;    // current locks
  multimap<uint64_t, ceph_filelock> waiting_locks; // locks waiting for other locks
  // both of the above are keyed by starting offset
  map<client_t, int> client_held_lock_counts;
  map<client_t, int> client_waiting_lock_counts;

  /**
   * Check if a lock is on the waiting_locks list.
   *
   * @param fl The filelock to check for
   * @returns True if the lock is waiting, false otherwise
   */
  bool is_waiting(const ceph_filelock &fl);
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
   * @param activated_locks A return parameter, holding activated wait locks.
   */
  void remove_lock(const ceph_filelock removal_lock,
                   list<ceph_filelock>& activated_locks);

  bool remove_all_from(client_t client);
private:
  static const unsigned MAX_DEADLK_DEPTH = 5;

  /**
   * Check if adding the lock causes deadlock
   *
   * @param fl The blocking filelock 
   * @param overlapping_locks list of all overlapping locks 
   * @param first_fl 
   * @depth recursion call depth
   */
  bool is_deadlock(const ceph_filelock& fl,
		   list<multimap<uint64_t, ceph_filelock>::iterator>&
		      overlapping_locks,
		   const ceph_filelock *first_fl=NULL, unsigned depth=0);

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
   * on an empty list, in which case it does nothing.
   * This function does not remove elements from old_locks, so regard the list
   * as bad information following function invocation.
   *
   * @param new_lock The new lock the process has requested.
   * @param old_locks list of all locks currently held by same
   *    client/process that overlap new_lock.
   * @param neighbor_locks locks owned by same process that neighbor new_lock on
   *    left or right side.
   */
  void adjust_locks(list<multimap<uint64_t, ceph_filelock>::iterator> old_locks,
                    ceph_filelock& new_lock,
                    list<multimap<uint64_t, ceph_filelock>::iterator>
                      neighbor_locks);

  //get last lock prior to start position
  multimap<uint64_t, ceph_filelock>::iterator
  get_lower_bound(uint64_t start,
                  multimap<uint64_t, ceph_filelock>& lock_map);
  //get latest-starting lock that goes over the byte "end"
  multimap<uint64_t, ceph_filelock>::iterator
  get_last_before(uint64_t end,
                  multimap<uint64_t, ceph_filelock>& lock_map);

  /*
   * See if an iterator's lock covers any of the same bounds as a given range
   * Rules: locks cover "length" bytes from "start", so the last covered
   * byte is at start + length - 1.
   * If the length is 0, the lock covers from "start" to the end of the file.
   */
  bool share_space(multimap<uint64_t, ceph_filelock>::iterator& iter,
		   uint64_t start, uint64_t end);
  
  bool share_space(multimap<uint64_t, ceph_filelock>::iterator& iter,
                   const ceph_filelock &lock) {
    uint64_t end = lock.start;
    if (lock.length) {
      end += lock.length - 1;
    } else { // zero length means end of file
      end = uint64_t(-1);
    }
    return share_space(iter, lock.start, end);
  }
  /*
   *get a list of all locks overlapping with the given lock's range
   * lock: the lock to compare with.
   * overlaps: an empty list, to be filled.
   * Returns: true if at least one lock overlaps.
   */
  bool get_overlapping_locks(const ceph_filelock& lock,
                             list<multimap<uint64_t,
                                 ceph_filelock>::iterator> & overlaps,
                             list<multimap<uint64_t,
                                 ceph_filelock>::iterator> *self_neighbors);

  
  bool get_overlapping_locks(const ceph_filelock& lock,
			     list<multimap<uint64_t, ceph_filelock>::iterator>& overlaps) {
    return get_overlapping_locks(lock, overlaps, NULL);
  }

  /**
   * Get a list of all waiting locks that overlap with the given lock's range.
   * lock: specifies the range to compare with
   * overlaps: an empty list, to be filled
   * Returns: true if at least one waiting_lock overlaps
   */
  bool get_waiting_overlaps(const ceph_filelock& lock,
                            list<multimap<uint64_t,
                                ceph_filelock>::iterator>& overlaps);
  /*
   * split a list of locks up by whether they're owned by same
   * process as given lock
   * owner: the owning lock
   * locks: the list of locks (obtained from get_overlapping_locks, probably)
   *        Will have all locks owned by owner removed
   * owned_locks: an empty list, to be filled with the locks owned by owner
   */
  void split_by_owner(const ceph_filelock& owner,
		      list<multimap<uint64_t,
		          ceph_filelock>::iterator> & locks,
		      list<multimap<uint64_t,
		          ceph_filelock>::iterator> & owned_locks);

  ceph_filelock *contains_exclusive_lock(list<multimap<uint64_t,
                                         ceph_filelock>::iterator>& locks);

public:
  void encode(bufferlist& bl) const {
    ::encode(held_locks, bl);
    ::encode(waiting_locks, bl);
    ::encode(client_held_lock_counts, bl);
    ::encode(client_waiting_lock_counts, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(held_locks, bl);
    ::decode(waiting_locks, bl);
    ::decode(client_held_lock_counts, bl);
    ::decode(client_waiting_lock_counts, bl);
  }
  void clear() {
    held_locks.clear();
    waiting_locks.clear();
    client_held_lock_counts.clear();
    client_waiting_lock_counts.clear();
  }
  bool empty() const {
    return held_locks.empty() && waiting_locks.empty() &&
	   client_held_lock_counts.empty() &&
	   client_waiting_lock_counts.empty();
  }
};
WRITE_CLASS_ENCODER(ceph_lock_state_t)


inline ostream& operator<<(ostream& out, ceph_lock_state_t& l) {
  out << "ceph_lock_state_t. held_locks.size()=" << l.held_locks.size()
      << ", waiting_locks.size()=" << l.waiting_locks.size()
      << ", client_held_lock_counts -- " << l.client_held_lock_counts
      << "\n client_waiting_lock_counts -- " << l.client_waiting_lock_counts
      << "\n held_locks -- ";
    for (multimap<uint64_t, ceph_filelock>::iterator iter = l.held_locks.begin();
         iter != l.held_locks.end();
         ++iter)
      out << iter->second;
    out << "\n waiting_locks -- ";
    for (multimap<uint64_t, ceph_filelock>::iterator iter =l.waiting_locks.begin();
         iter != l.waiting_locks.end();
         ++iter)
      out << iter->second << "\n";
  return out;
}

#endif
