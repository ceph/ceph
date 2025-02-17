// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSD_INTERNAL_TYPES_H
#define CEPH_OSD_INTERNAL_TYPES_H

#include "osd_types.h"
#include "OpRequest.h"
#include "object_state.h"

/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size, snapset,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */

struct SnapSetContext {
  hobject_t oid;
  SnapSet snapset;
  int ref;
  bool registered : 1;
  bool exists : 1;

  explicit SnapSetContext(const hobject_t& o) :
    oid(o), ref(0), registered(false), exists(true) { }
};

inline std::ostream& operator<<(std::ostream& out, const SnapSetContext& ssc)
{
  return out << "ssc(" << ssc.oid << " snapset: " << ssc.snapset
             << " ref: " << ssc.ref << " registered: "
             << ssc.registered << " exists: " << ssc.exists << ")";
}

struct ObjectContext;
typedef std::shared_ptr<ObjectContext> ObjectContextRef;

struct ObjectContext {
  ObjectState obs;

  SnapSetContext *ssc;  // may be null

  Context *destructor_callback;

public:

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  std::map<std::pair<uint64_t, entity_name_t>, WatchRef> watchers;

  // attr cache
  std::map<std::string, ceph::buffer::list, std::less<>> attr_cache;

  RWState rwstate;
  std::list<OpRequestRef> waiters;  ///< ops waiting on state change
  bool get_read(OpRequestRef& op) {
    if (rwstate.get_read_lock()) {
      return true;
    } // else
      // Now we really need to bump up the ref-counter.
    waiters.emplace_back(op);
    rwstate.inc_waiters();
    return false;
  }
  bool get_write(OpRequestRef& op, bool greedy=false) {
    if (rwstate.get_write_lock(greedy)) {
      return true;
    } // else
    if (op) {
      waiters.emplace_back(op);
      rwstate.inc_waiters();
    }
    return false;
  }
  bool get_excl(OpRequestRef& op) {
    if (rwstate.get_excl_lock()) {
      return true;
    } // else
    if (op) {
      waiters.emplace_back(op);
      rwstate.inc_waiters();
    }
    return false;
  }
  void wake(std::list<OpRequestRef> *requeue) {
    rwstate.release_waiters();
    requeue->splice(requeue->end(), waiters);
  }
  void put_read(std::list<OpRequestRef> *requeue) {
    if (rwstate.put_read()) {
      wake(requeue);
    }
  }
  void put_write(std::list<OpRequestRef> *requeue) {
    if (rwstate.put_write()) {
      wake(requeue);
    }
  }
  void put_excl(std::list<OpRequestRef> *requeue) {
    if (rwstate.put_excl()) {
      wake(requeue);
    }
  }
  bool empty() const { return rwstate.empty(); }

  bool get_lock_type(OpRequestRef& op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_write(op);
    case RWState::RWREAD:
      return get_read(op);
    case RWState::RWEXCL:
      return get_excl(op);
    default:
      ceph_abort_msg("invalid lock type");
      return true;
    }
  }
  bool get_write_greedy(OpRequestRef& op) {
    return get_write(op, true);
  }
  bool get_snaptrimmer_write(bool mark_if_unsuccessful) {
    return rwstate.get_snaptrimmer_write(mark_if_unsuccessful);
  }
  bool get_recovery_read() {
    return rwstate.get_recovery_read();
  }
  bool try_get_read_lock() {
    return rwstate.get_read_lock();
  }
  void drop_recovery_read(std::list<OpRequestRef> *ls) {
    ceph_assert(rwstate.recovery_read_marker);
    put_read(ls);
    rwstate.recovery_read_marker = false;
  }
  void put_lock_type(
    RWState::State type,
    std::list<OpRequestRef> *to_wake,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    switch (type) {
    case RWState::RWWRITE:
      put_write(to_wake);
      break;
    case RWState::RWREAD:
      put_read(to_wake);
      break;
    case RWState::RWEXCL:
      put_excl(to_wake);
      break;
    default:
      ceph_abort_msg("invalid lock type");
    }
    if (rwstate.empty() && rwstate.recovery_read_marker) {
      rwstate.recovery_read_marker = false;
      *requeue_recovery = true;
    }
    if (rwstate.empty() && rwstate.snaptrimmer_write_marker) {
      rwstate.snaptrimmer_write_marker = false;
      *requeue_snaptrimmer = true;
    }
  }
  bool is_request_pending() {
    return !rwstate.empty();
  }

  ObjectContext()
    : ssc(NULL),
      destructor_callback(0),
      blocked(false), requeue_scrub_on_unblock(false) {}

  ~ObjectContext() {
    ceph_assert(rwstate.empty());
    if (destructor_callback)
      destructor_callback->complete(0);
  }

  void start_block() {
    ceph_assert(!blocked);
    blocked = true;
  }
  void stop_block() {
    ceph_assert(blocked);
    blocked = false;
  }
  bool is_blocked() const {
    return blocked;
  }

  /// in-progress copyfrom ops for this object
  bool blocked;
  bool requeue_scrub_on_unblock;    // true if we need to requeue scrub on unblock
};

inline std::ostream& operator<<(std::ostream& out, const ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline std::ostream& operator<<(std::ostream& out, const ObjectContext& obc)
{
  return out << "obc(" << obc.obs << " " << obc.rwstate << ")";
}

class ObcLockManager {
  struct ObjectLockState {
    ObjectContextRef obc;
    RWState::State type;
    ObjectLockState(
      ObjectContextRef obc,
      RWState::State type)
      : obc(std::move(obc)), type(type) {}
  };
  std::map<hobject_t, ObjectLockState> locks;
public:
  ObcLockManager() = default;
  ObcLockManager(ObcLockManager &&) = default;
  ObcLockManager(const ObcLockManager &) = delete;
  ObcLockManager &operator=(ObcLockManager &&) = default;
  bool empty() const {
    return locks.empty();
  }
  bool get_lock_type(
    RWState::State type,
    const hobject_t &hoid,
    ObjectContextRef& obc,
    OpRequestRef& op) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->get_lock_type(op, type)) {
      locks.insert(std::make_pair(hoid, ObjectLockState(obc, type)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock, ignore starvation
  bool take_write_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->rwstate.take_write_lock()) {
      locks.insert(
	std::make_pair(
	  hoid, ObjectLockState(obc, RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock for snap trim
  bool get_snaptrimmer_write(
    const hobject_t &hoid,
    ObjectContextRef obc,
    bool mark_if_unsuccessful) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->get_snaptrimmer_write(mark_if_unsuccessful)) {
      locks.insert(
	std::make_pair(
	  hoid, ObjectLockState(obc, RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock greedy
  bool get_write_greedy(
    const hobject_t &hoid,
    ObjectContextRef obc,
    OpRequestRef op) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->get_write_greedy(op)) {
      locks.insert(
	std::make_pair(
	  hoid, ObjectLockState(obc, RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }

  /// try get read lock
  bool try_get_read_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->try_get_read_lock()) {
      locks.insert(
	std::make_pair(
	  hoid,
	  ObjectLockState(obc, RWState::RWREAD)));
      return true;
    } else {
      return false;
    }
  }

  void put_locks(
    std::list<std::pair<ObjectContextRef, std::list<OpRequestRef> > > *to_requeue,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    for (auto& p: locks) {
      std::list<OpRequestRef> _to_requeue;
      p.second.obc->put_lock_type(
	p.second.type,
	&_to_requeue,
	requeue_recovery,
	requeue_snaptrimmer);
      if (to_requeue) {
        // We can safely std::move here as the whole `locks` is going
        // to die just after the loop.
	to_requeue->emplace_back(std::move(p.second.obc),
				 std::move(_to_requeue));
      }
    }
    locks.clear();
  }
  ~ObcLockManager() {
    ceph_assert(locks.empty());
  }
};



#endif
