// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <array>
#include <set>
#include <vector>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

enum class OperationTypeCode {
  client_request = 0,
  peering_event = 1,
  compound_peering_request = 2,
  pg_advance_map = 3,
  pg_creation = 4,
  replicated_request = 5,
  last_op = 6
};

static constexpr const char* const OP_NAMES[] = {
  "client_request",
  "peering_event",
  "compound_peering_request",
  "pg_advance_map",
  "pg_creation",
  "replicated_request",
};

// prevent the addition of OperationTypeCode-s with no matching OP_NAMES entry:
static_assert(
  (sizeof(OP_NAMES)/sizeof(OP_NAMES[0])) ==
  static_cast<int>(OperationTypeCode::last_op));

class OperationRegistry;

using registry_hook_t = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

class Operation;
class Blocker;

/**
 * Provides an abstraction for registering and unregistering a blocker
 * for the duration of a future becoming available.
 */
template <typename... T>
class blocking_future {
  friend class Operation;
  friend class Blocker;
  Blocker *blocker;
  seastar::future<T...> fut;
  blocking_future(Blocker *b, seastar::future<T...> &&f)
    : blocker(b), fut(std::move(f)) {}

  template <typename... V, typename... U>
  friend blocking_future<V...> make_ready_blocking_future(U&&... args);
};

template <typename... V, typename... U>
blocking_future<V...> make_ready_blocking_future(U&&... args) {
  return blocking_future<V...>(
    nullptr,
    seastar::make_ready_future<V...>(std::forward<U>(args)...));
}

/**
 * Provides an interface for dumping diagnostic information about
 * why a particular op is not making progress.
 */
class Blocker {
protected:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

public:
  template <typename... T>
  blocking_future<T...> make_blocking_future(seastar::future<T...> &&f) {
    return blocking_future(this, std::move(f));
  }

  void dump(ceph::Formatter *f) const;

  virtual const char *get_type_name() const = 0;

  virtual ~Blocker() = default;
};

template <typename T>
class BlockerT : public Blocker {
public:
  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~BlockerT() = default;
};

/**
 * Common base for all crimson-osd operations.  Mainly provides
 * an interface for registering ops in flight and dumping
 * diagnostic information.
 */
class Operation : public boost::intrusive_ref_counter<
  Operation, boost::thread_unsafe_counter> {
  friend class OperationRegistry;
  registry_hook_t registry_hook;

  std::vector<Blocker*> blockers;
  uint64_t id = 0;
  void set_id(uint64_t in_id) {
    id = in_id;
  }
protected:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

public:
  uint64_t get_id() const {
    return id;
  }

  virtual OperationTypeCode get_type() const = 0;
  virtual const char *get_type_name() const = 0;
  virtual void print(std::ostream &) const = 0;

  void add_blocker(Blocker *b) {
    blockers.push_back(b);
  }

  void clear_blocker(Blocker *b) {
    auto iter = std::find(blockers.begin(), blockers.end(), b);
    if (iter != blockers.end()) {
      blockers.erase(iter);
    }
  }

  template <typename... T>
  seastar::future<T...> with_blocking_future(blocking_future<T...> &&f) {
    if (f.fut.available()) {
      return std::move(f.fut);
    }
    assert(f.blocker);
    add_blocker(f.blocker);
    return std::move(f.fut).then_wrapped([this, blocker=f.blocker](auto &&arg) {
      clear_blocker(blocker);
      return std::move(arg);
    });
  }

  void dump(ceph::Formatter *f);
  void dump_brief(ceph::Formatter *f);
  virtual ~Operation() = default;
};
using OperationRef = boost::intrusive_ptr<Operation>;

std::ostream &operator<<(std::ostream &, const Operation &op);

template <typename T>
class OperationT : public Operation {

protected:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

public:
  static constexpr const char *type_name = OP_NAMES[static_cast<int>(T::type)];
  using IRef = boost::intrusive_ptr<T>;

  OperationTypeCode get_type() const final {
    return T::type;
  }

  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~OperationT() = default;
};

/**
 * Maintains a set of lists of all active ops.
 */
class OperationRegistry {
  friend class Operation;
  using op_list_member_option = boost::intrusive::member_hook<
    Operation,
    registry_hook_t,
    &Operation::registry_hook
    >;
  using op_list = boost::intrusive::list<
    Operation,
    op_list_member_option,
    boost::intrusive::constant_time_size<false>>;

  std::array<
    op_list,
    static_cast<int>(OperationTypeCode::last_op)
  > registries;

  std::array<
    uint64_t,
    static_cast<int>(OperationTypeCode::last_op)
  > op_id_counters = {};

public:
  template <typename T, typename... Args>
  typename T::IRef create_operation(Args&&... args) {
    typename T::IRef op = new T(std::forward<Args>(args)...);
    registries[static_cast<int>(T::type)].push_back(*op);
    op->set_id(op_id_counters[static_cast<int>(T::type)]++);
    return op;
  }
};

/**
 * Ensures that at most one op may consider itself in the phase at a time.
 * Ops will see enter() unblock in the order in which they tried to enter
 * the phase.  entering (though not necessarily waiting for the future to
 * resolve) a new phase prior to exiting the previous one will ensure that
 * the op ordering is preserved.
 */
class OrderedPipelinePhase : public Blocker {
  const char * name;

protected:
  virtual void dump_detail(ceph::Formatter *f) const final;
  const char *get_type_name() const final {
    return name;
  }

public:
  seastar::shared_mutex mutex;

  /**
   * Used to encapsulate pipeline residency state.
   */
  class Handle {
    OrderedPipelinePhase *phase = nullptr;

  public:
    Handle() = default;

    Handle(const Handle&) = delete;
    Handle(Handle&&) = delete;
    Handle &operator=(const Handle&) = delete;
    Handle &operator=(Handle&&) = delete;

    /**
     * Returns a future which unblocks when the handle has entered the passed
     * OrderedPipelinePhase.  If already in a phase, enter will also release
     * that phase after placing itself in the queue for the next one to preserve
     * ordering.
     */
    blocking_future<> enter(OrderedPipelinePhase &phase);

    /**
     * Releases the current phase if there is one.  Called in ~Handle().
     */
    void exit();

    ~Handle();
  };

  OrderedPipelinePhase(const char *name) : name(name) {}
};

}
