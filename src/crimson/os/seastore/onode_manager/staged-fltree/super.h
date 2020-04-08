// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include "crimson/common/type_helpers.h"

#include "fwd.h"

namespace crimson::os::seastore::onode {

class Node;
class Super;
class RootNodeTracker {
 public:
  virtual ~RootNodeTracker() = default;
  virtual bool is_clean() const = 0;
  virtual Ref<Node> get_root(Transaction&) const = 0;
  static RootNodeTrackerURef create(bool read_isolated);
 protected:
  RootNodeTracker() = default;
  RootNodeTracker(const RootNodeTracker&) = delete;
  RootNodeTracker(RootNodeTracker&&) = delete;
  RootNodeTracker& operator=(const RootNodeTracker&) = delete;
  RootNodeTracker& operator=(RootNodeTracker&&) = delete;
  virtual void do_track_super(Transaction&, Super&) = 0;
  virtual void do_untrack_super(Transaction&, Super&) = 0;
  friend class Super;
};

class Super {
 public:
  using URef = std::unique_ptr<Super>;
  Super(const Super&) = delete;
  Super(Super&&) = delete;
  Super& operator=(const Super&) = delete;
  Super& operator=(Super&&) = delete;
  virtual ~Super() {
    assert(tracked_root_node == nullptr);
    tracker.do_untrack_super(t, *this);
  }

  virtual laddr_t get_root_laddr() const = 0;
  virtual void write_root_laddr(context_t, laddr_t) = 0;

  void do_track_root(Node& root) {
    assert(tracked_root_node == nullptr);
    tracked_root_node = &root;
  }
  void do_untrack_root(Node& root) {
    assert(tracked_root_node == &root);
    tracked_root_node = nullptr;
  }
  Node* get_p_root() const {
    assert(tracked_root_node != nullptr);
    return tracked_root_node;
  }

 protected:
  Super(Transaction& t, RootNodeTracker& tracker)
      : t{t}, tracker{tracker} {
    tracker.do_track_super(t, *this);
  }

 private:
  Transaction& t;
  RootNodeTracker& tracker;
  Node* tracked_root_node = nullptr;
};

class RootNodeTrackerIsolated final : public RootNodeTracker {
 public:
  ~RootNodeTrackerIsolated() override { assert(is_clean()); }
 protected:
  bool is_clean() const override {
    return tracked_supers.empty();
  }
  void do_track_super(Transaction& t, Super& super) override {
    assert(tracked_supers.find(&t) == tracked_supers.end());
    tracked_supers[&t] = &super;
  }
  void do_untrack_super(Transaction& t, Super& super) override {
    auto removed = tracked_supers.erase(&t);
    assert(removed);
  }
  ::Ref<Node> get_root(Transaction& t) const override;
  std::map<Transaction*, Super*> tracked_supers;
};

class RootNodeTrackerShared final : public RootNodeTracker {
 public:
  ~RootNodeTrackerShared() override { assert(is_clean()); }
 protected:
  bool is_clean() const override {
    return tracked_super == nullptr;
  }
  void do_track_super(Transaction&, Super& super) override {
    assert(is_clean());
    tracked_super = &super;
  }
  void do_untrack_super(Transaction&, Super& super) override {
    assert(tracked_super == &super);
    tracked_super = nullptr;
  }
  ::Ref<Node> get_root(Transaction&) const override;
  Super* tracked_super = nullptr;
};

inline RootNodeTrackerURef RootNodeTracker::create(bool read_isolated) {
  if (read_isolated) {
    return RootNodeTrackerURef(new RootNodeTrackerIsolated());
  } else {
    return RootNodeTrackerURef(new RootNodeTrackerShared());
  }
}

}
