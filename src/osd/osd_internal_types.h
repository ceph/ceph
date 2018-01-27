// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSD_INTERNAL_TYPES_H
#define CEPH_OSD_INTERNAL_TYPES_H

#include "osd_types.h"
#include "OpRequest.h"

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

struct ObjectContext;

struct ObjectState {
  object_info_t oi;
  bool exists;         ///< the stored object exists (i.e., we will remember the object_info_t)

  ObjectState() : exists(false) {}

  ObjectState(const object_info_t &oi_, bool exists_)
    : oi(oi_), exists(exists_) {}
};

typedef ceph::shared_ptr<ObjectContext> ObjectContextRef;

struct ObjectContext {
  ObjectState obs;

  SnapSetContext *ssc;  // may be null

  Context *destructor_callback;

public:

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  // attr cache
  map<string, bufferlist> attr_cache;

  struct RWState {
    enum State {
      RWNONE,
      RWREAD,
      RWWRITE,
      RWEXCL,
    };
    static const char *get_state_name(State s) {
      switch (s) {
      case RWNONE: return "none";
      case RWREAD: return "read";
      case RWWRITE: return "write";
      case RWEXCL: return "excl";
      default: return "???";
      }
    }
    const char *get_state_name() const {
      return get_state_name(state);
    }

    list<OpRequestRef> waiters;  ///< ops waiting on state change
    int count;              ///< number of readers or writers

    State state:4;               ///< rw state
    /// if set, restart backfill when we can get a read lock
    bool recovery_read_marker:1;
    /// if set, requeue snaptrim on lock release
    bool snaptrimmer_write_marker:1;

    RWState()
      : count(0),
	state(RWNONE),
	recovery_read_marker(false),
	snaptrimmer_write_marker(false)
    {}
    bool get_read(OpRequestRef op) {
      if (get_read_lock()) {
	return true;
      } // else
      waiters.push_back(op);
      return false;
    }
    /// this function adjusts the counts if necessary
    bool get_read_lock() {
      // don't starve anybody!
      if (!waiters.empty()) {
	return false;
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWREAD;
	// fall through
      case RWREAD:
	count++;
	return true;
      case RWWRITE:
	return false;
      case RWEXCL:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }

    bool get_write(OpRequestRef op, bool greedy=false) {
      if (get_write_lock(greedy)) {
	return true;
      } // else
      if (op)
	waiters.push_back(op);
      return false;
    }
    bool get_write_lock(bool greedy=false) {
      if (!greedy) {
	// don't starve anybody!
	if (!waiters.empty() ||
	    recovery_read_marker) {
	  return false;
	}
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWWRITE;
	// fall through
      case RWWRITE:
	count++;
	return true;
      case RWREAD:
	return false;
      case RWEXCL:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }
    bool get_excl_lock() {
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWEXCL;
	count = 1;
	return true;
      case RWWRITE:
	return false;
      case RWREAD:
	return false;
      case RWEXCL:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }
    bool get_excl(OpRequestRef op) {
      if (get_excl_lock()) {
	return true;
      } // else
      if (op)
	waiters.push_back(op);
      return false;
    }
    /// same as get_write_lock, but ignore starvation
    bool take_write_lock() {
      if (state == RWWRITE) {
	count++;
	return true;
      }
      return get_write_lock();
    }
    void dec(list<OpRequestRef> *requeue) {
      assert(count > 0);
      assert(requeue);
      count--;
      if (count == 0) {
	state = RWNONE;
	requeue->splice(requeue->end(), waiters);
      }
    }
    void put_read(list<OpRequestRef> *requeue) {
      assert(state == RWREAD);
      dec(requeue);
    }
    void put_write(list<OpRequestRef> *requeue) {
      assert(state == RWWRITE);
      dec(requeue);
    }
    void put_excl(list<OpRequestRef> *requeue) {
      assert(state == RWEXCL);
      dec(requeue);
    }
    bool empty() const { return state == RWNONE; }
  } rwstate;

  bool get_read(OpRequestRef op) {
    return rwstate.get_read(op);
  }
  bool get_write(OpRequestRef op) {
    return rwstate.get_write(op, false);
  }
  bool get_excl(OpRequestRef op) {
    return rwstate.get_excl(op);
  }
  bool get_lock_type(OpRequestRef op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_write(op);
    case RWState::RWREAD:
      return get_read(op);
    case RWState::RWEXCL:
      return get_excl(op);
    default:
      assert(0 == "invalid lock type");
      return true;
    }
  }
  bool get_write_greedy(OpRequestRef op) {
    return rwstate.get_write(op, true);
  }
  bool get_snaptrimmer_write(bool mark_if_unsuccessful) {
    if (rwstate.get_write_lock()) {
      return true;
    } else {
      if (mark_if_unsuccessful)
	rwstate.snaptrimmer_write_marker = true;
      return false;
    }
  }
  bool get_recovery_read() {
    rwstate.recovery_read_marker = true;
    if (rwstate.get_read_lock()) {
      return true;
    }
    return false;
  }
  bool try_get_read_lock() {
    return rwstate.get_read_lock();
  }
  void drop_recovery_read(list<OpRequestRef> *ls) {
    assert(rwstate.recovery_read_marker);
    rwstate.put_read(ls);
    rwstate.recovery_read_marker = false;
  }
  void put_lock_type(
    ObjectContext::RWState::State type,
    list<OpRequestRef> *to_wake,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    switch (type) {
    case ObjectContext::RWState::RWWRITE:
      rwstate.put_write(to_wake);
      break;
    case ObjectContext::RWState::RWREAD:
      rwstate.put_read(to_wake);
      break;
    case ObjectContext::RWState::RWEXCL:
      rwstate.put_excl(to_wake);
      break;
    default:
      assert(0 == "invalid lock type");
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
    return (rwstate.count > 0);
  }

  ObjectContext()
    : ssc(NULL),
      destructor_callback(0),
      blocked(false), requeue_scrub_on_unblock(false) {}

  ~ObjectContext() {
    assert(rwstate.empty());
    if (destructor_callback)
      destructor_callback->complete(0);
  }

  void start_block() {
    assert(!blocked);
    blocked = true;
  }
  void stop_block() {
    assert(blocked);
    blocked = false;
  }
  bool is_blocked() const {
    return blocked;
  }

  /// in-progress copyfrom ops for this object
  bool blocked:1;
  bool requeue_scrub_on_unblock:1;    // true if we need to requeue scrub on unblock

};

inline ostream& operator<<(ostream& out, const ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline ostream& operator<<(ostream& out, const ObjectContext::RWState& rw)
{
  return out << "rwstate(" << rw.get_state_name()
	     << " n=" << rw.count
	     << " w=" << rw.waiters.size()
	     << ")";
}

inline ostream& operator<<(ostream& out, const ObjectContext& obc)
{
  return out << "obc(" << obc.obs << " " << obc.rwstate << ")";
}

class ObcLockManager {
  struct ObjectLockState {
    ObjectContextRef obc;
    ObjectContext::RWState::State type;
    ObjectLockState(
      ObjectContextRef obc,
      ObjectContext::RWState::State type)
      : obc(obc), type(type) {}
  };
  map<hobject_t, ObjectLockState> locks;
public:
  ObcLockManager() = default;
  ObcLockManager(ObcLockManager &&) = default;
  ObcLockManager(const ObcLockManager &) = delete;
  ObcLockManager &operator=(ObcLockManager &&) = default;
  bool empty() const {
    return locks.empty();
  }
  bool get_lock_type(
    ObjectContext::RWState::State type,
    const hobject_t &hoid,
    ObjectContextRef obc,
    OpRequestRef op) {
    assert(locks.find(hoid) == locks.end());
    if (obc->get_lock_type(op, type)) {
      locks.insert(make_pair(hoid, ObjectLockState(obc, type)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock, ignore starvation
  bool take_write_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    assert(locks.find(hoid) == locks.end());
    if (obc->rwstate.take_write_lock()) {
      locks.insert(
	make_pair(
	  hoid, ObjectLockState(obc, ObjectContext::RWState::RWWRITE)));
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
    assert(locks.find(hoid) == locks.end());
    if (obc->get_snaptrimmer_write(mark_if_unsuccessful)) {
      locks.insert(
	make_pair(
	  hoid, ObjectLockState(obc, ObjectContext::RWState::RWWRITE)));
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
    assert(locks.find(hoid) == locks.end());
    if (obc->get_write_greedy(op)) {
      locks.insert(
	make_pair(
	  hoid, ObjectLockState(obc, ObjectContext::RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }

  /// try get read lock
  bool try_get_read_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    assert(locks.find(hoid) == locks.end());
    if (obc->try_get_read_lock()) {
      locks.insert(
	make_pair(
	  hoid,
	  ObjectLockState(obc, ObjectContext::RWState::RWREAD)));
      return true;
    } else {
      return false;
    }
  }

  void put_locks(
    list<pair<hobject_t, list<OpRequestRef> > > *to_requeue,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    for (auto p: locks) {
      list<OpRequestRef> _to_requeue;
      p.second.obc->put_lock_type(
	p.second.type,
	&_to_requeue,
	requeue_recovery,
	requeue_snaptrimmer);
      if (to_requeue) {
	to_requeue->push_back(
	  make_pair(
	    p.second.obc->obs.oi.soid,
	    std::move(_to_requeue)));
      }
    }
    locks.clear();
  }
  ~ObcLockManager() {
    assert(locks.empty());
  }
};



#endif
