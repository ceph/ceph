// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef EXTENT_CACHE_H
#define EXTENT_CACHE_H

#include <map>
#include <list>
#include <vector>
#include <utility>
#include <optional>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "include/buffer.h"
#include "common/hobject.h"

/**
   ExtentCache

   The main purpose of this cache is to ensure that we can pipeline
   overlapping partial overwrites.

   To that end we need to ensure that an extent pinned for an operation is
   live until that operation completes.  However, a particular extent
   might be pinned by multiple operations (several pipelined writes
   on the same object).

   1) When we complete an operation, we only look at extents owned only
      by that operation.
   2) Per-extent overhead is fixed size.
   2) Per-operation metadata is fixed size.

   This is simple enough to realize with two main structures:
   - extent: contains a pointer to the pin owning it and intrusive list
             pointers to other extents owned by the same pin
   - pin_state: contains the list head for extents owned by it

   This works as long as we only need to remember one "owner" for
   each extent.  To make this work, we'll need to leverage some
   invariants guaranteed by higher layers:

   1) Writes on a particular object must be ordered
   2) A particular object will have outstanding reads or writes, but not
      both (note that you can have a read while a write is committed, but
      not applied).

   Our strategy therefore will be to have whichever in-progress op will
   finish "last" be the owner of a particular extent.  For now, we won't
   cache reads, so 2) simply means that we can assume that reads and
   recovery operations imply no unstable extents on the object in
   question.

   Write: WaitRead -> WaitCommit -> Complete

   Invariant 1) above actually indicates that we can't have writes
   bypassing the WaitRead state while there are writes waiting on
   Reads.  Thus, the set of operations pinning a particular extent
   must always complete in order or arrival.

   This suggests that a particular extent may be in only the following
   states:


   0) Empty (not in the map at all)
   1) Write Pending N
      - Some write with reqid <= N is currently fetching the data for
        this extent
      - The extent must persist until Write reqid N completes
      - All ops pinning this extent are writes in the WaitRead state of
        the Write pipeline (there must be an in progress write, so no
	reads can be in progress).
   2) Write Pinned N:
      - This extent has data corresponding to some reqid M <= N
      - The extent must persist until Write reqid N commits
      - All ops pinning this extent are writes in some Write
        state (all are possible).  Reads are not possible
	in this state (or the others) due to 2).

   All of the above suggests that there are 3 things users can
   ask of the cache corresponding to the 3 Write pipelines
   states.
 */

/// If someone wants these types, but not ExtentCache, move to another file
struct bl_split_merge {
  ceph::buffer::list split(
    uint64_t offset,
    uint64_t length,
    ceph::buffer::list &bl) const {
    ceph::buffer::list out;
    out.substr_of(bl, offset, length);
    return out;
  }
  bool can_merge(const ceph::buffer::list &left, const ceph::buffer::list &right) const {
    return true;
  }
  ceph::buffer::list merge(ceph::buffer::list &&left, ceph::buffer::list &&right) const {
    ceph::buffer::list bl{std::move(left)};
    bl.claim_append(right);
    return bl;
  }
  uint64_t length(const ceph::buffer::list &b) const { return b.length(); }
};
using extent_set = interval_set<uint64_t>;
using extent_map = interval_map<uint64_t, ceph::buffer::list, bl_split_merge>;

class ExtentCache {
  struct object_extent_set;
  struct pin_state;
private:

  struct extent {
    object_extent_set *parent_extent_set = nullptr;
    pin_state *parent_pin_state = nullptr;
    boost::intrusive::set_member_hook<> extent_set_member;
    boost::intrusive::list_member_hook<> pin_list_member;

    uint64_t offset;
    uint64_t length;
    std::optional<ceph::buffer::list> bl;

    uint64_t get_length() const {
      return length;
    }

    bool is_pending() const {
      return bl == std::nullopt;
    }

    bool pinned_by_write() const {
      ceph_assert(parent_pin_state);
      return parent_pin_state->is_write();
    }

    uint64_t pin_tid() const {
      ceph_assert(parent_pin_state);
      return parent_pin_state->tid;
    }

    extent(uint64_t offset, ceph::buffer::list _bl)
      : offset(offset), length(_bl.length()), bl(_bl) {}

    extent(uint64_t offset, uint64_t length)
      : offset(offset), length(length) {}

    bool operator<(const extent &rhs) const {
      return offset < rhs.offset;
    }
  private:
    // can briefly violate the two link invariant, used in unlink() and move()
    void _link_pin_state(pin_state &pin_state);
    void _unlink_pin_state();
  public:
    void unlink();
    void link(object_extent_set &parent_extent_set, pin_state &pin_state);
    void move(pin_state &to);
  };

  struct object_extent_set : boost::intrusive::set_base_hook<> {
    hobject_t oid;
    explicit object_extent_set(const hobject_t &oid) : oid(oid) {}

    using set_member_options = boost::intrusive::member_hook<
      extent,
      boost::intrusive::set_member_hook<>,
      &extent::extent_set_member>;
    using set = boost::intrusive::set<extent, set_member_options>;
    set extent_set;

    bool operator<(const object_extent_set &rhs) const {
      return oid < rhs.oid;
    }

    struct uint_cmp {
      bool operator()(uint64_t lhs, const extent &rhs) const {
	return lhs < rhs.offset;
      }
      bool operator()(const extent &lhs, uint64_t rhs) const {
	return lhs.offset < rhs;
      }
    };
    std::pair<set::iterator, set::iterator> get_containing_range(
      uint64_t offset, uint64_t length);

    void erase(uint64_t offset, uint64_t length);

    struct update_action {
      enum type {
	NONE,
	UPDATE_PIN
      };
      type action = NONE;
      std::optional<ceph::buffer::list> bl;
    };
    template <typename F>
    void traverse_update(
      pin_state &pin,
      uint64_t offset,
      uint64_t length,
      F &&f) {
      auto range = get_containing_range(offset, length);

      if (range.first == range.second || range.first->offset > offset) {
	uint64_t extlen = range.first == range.second ?
	  length : range.first->offset - offset;

	update_action action;
	f(offset, extlen, nullptr, &action);
	ceph_assert(!action.bl || action.bl->length() == extlen);
	if (action.action == update_action::UPDATE_PIN) {
	  extent *ext = action.bl ?
	    new extent(offset, *action.bl) :
	    new extent(offset, extlen);
	  ext->link(*this, pin);
	} else {
	  ceph_assert(!action.bl);
	}
      }

      for (auto p = range.first; p != range.second;) {
	extent *ext = &*p;
	++p;

	uint64_t extoff = std::max(ext->offset, offset);
	uint64_t extlen = std::min(
	  ext->length - (extoff - ext->offset),
	  offset + length - extoff);

	update_action action;
	f(extoff, extlen, ext, &action);
	ceph_assert(!action.bl || action.bl->length() == extlen);
	extent *final_extent = nullptr;
	if (action.action == update_action::NONE) {
	  final_extent = ext;
	} else {
	  pin_state *ps = ext->parent_pin_state;
	  ext->unlink();
	  if ((ext->offset < offset) &&
	      (ext->offset + ext->get_length() > offset)) {
	    extent *head = nullptr;
	    if (ext->bl) {
	      ceph::buffer::list bl;
	      bl.substr_of(
		*(ext->bl),
		0,
		offset - ext->offset);
	      head = new extent(ext->offset, bl);
	    } else {
	      head = new extent(
		ext->offset, offset - ext->offset);
	    }
	    head->link(*this, *ps);
	  }
	  if ((ext->offset + ext->length > offset + length) &&
	      (offset + length > ext->offset)) {
	    uint64_t nlen =
	      (ext->offset + ext->get_length()) - (offset + length);
	    extent *tail = nullptr;
	    if (ext->bl) {
	      ceph::buffer::list bl;
	      bl.substr_of(
		*(ext->bl),
		ext->get_length() - nlen,
		nlen);
	      tail = new extent(offset + length, bl);
	    } else {
	      tail = new extent(offset + length, nlen);
	    }
	    tail->link(*this, *ps);
	  }
	  if (action.action == update_action::UPDATE_PIN) {
	    if (ext->bl) {
	      ceph::buffer::list bl;
	      bl.substr_of(
		*(ext->bl),
		extoff - ext->offset,
		extlen);
	      final_extent = new ExtentCache::extent(
		extoff,
		bl);
	    } else {
	      final_extent = new ExtentCache::extent(
		extoff, extlen);
	    }
	    final_extent->link(*this, pin);
	  }
	  delete ext;
	}

	if (action.bl) {
	  ceph_assert(final_extent);
	  ceph_assert(final_extent->length == action.bl->length());
	  final_extent->bl = *(action.bl);
	}

	uint64_t next_off = p == range.second ?
	  offset + length : p->offset;
	if (extoff + extlen < next_off) {
	  uint64_t tailoff = extoff + extlen;
	  uint64_t taillen = next_off - tailoff;

	  update_action action;
	  f(tailoff, taillen, nullptr, &action);
	  ceph_assert(!action.bl || action.bl->length() == taillen);
	  if (action.action == update_action::UPDATE_PIN) {
	    extent *ext = action.bl ?
	      new extent(tailoff, *action.bl) :
	      new extent(tailoff, taillen);
	    ext->link(*this, pin);
	  } else {
	    ceph_assert(!action.bl);
	  }
	}
      }
    }
  };
  struct Cmp {
    bool operator()(const hobject_t &oid, const object_extent_set &rhs) const {
      return oid < rhs.oid;
    }
    bool operator()(const object_extent_set &lhs, const hobject_t &oid) const {
      return lhs.oid < oid;
    }
  };

  object_extent_set &get_or_create(const hobject_t &oid);
  object_extent_set *get_if_exists(const hobject_t &oid);

  void remove_and_destroy_if_empty(object_extent_set &set);
  using cache_set = boost::intrusive::set<object_extent_set>;
  cache_set per_object_caches;

  uint64_t next_write_tid = 1;
  uint64_t next_read_tid = 1;
  struct pin_state {
    uint64_t tid = 0;
    enum pin_type_t {
      NONE,
      WRITE,
    };
    pin_type_t pin_type = NONE;
    bool is_write() const { return pin_type == WRITE; }

    pin_state(const pin_state &other) = delete;
    pin_state &operator=(const pin_state &other) = delete;
    pin_state(pin_state &&other) = delete;
    pin_state() = default;

    using list_member_options = boost::intrusive::member_hook<
      extent,
      boost::intrusive::list_member_hook<>,
      &extent::pin_list_member>;
    using list = boost::intrusive::list<extent, list_member_options>;
    list pin_list;
    ~pin_state() {
      ceph_assert(pin_list.empty());
      ceph_assert(tid == 0);
      ceph_assert(pin_type == NONE);
    }
    void _open(uint64_t in_tid, pin_type_t in_type) {
      ceph_assert(pin_type == NONE);
      ceph_assert(in_tid > 0);
      tid = in_tid;
      pin_type = in_type;
    }
  };

  void release_pin(pin_state &p) {
    for (auto iter = p.pin_list.begin(); iter != p.pin_list.end(); ) {
      std::unique_ptr<extent> extent(&*iter); // we now own this
      iter++; // unlink will invalidate
      ceph_assert(extent->parent_extent_set);
      auto &eset = *(extent->parent_extent_set);
      extent->unlink();
      remove_and_destroy_if_empty(eset);
    }
    p.tid = 0;
    p.pin_type = pin_state::NONE;
  }

public:
  class write_pin : private pin_state {
    friend class ExtentCache;
  private:
    void open(uint64_t in_tid) {
      _open(in_tid, pin_state::WRITE);
    }
  public:
    write_pin() : pin_state() {}
  };

  void open_write_pin(write_pin &pin) {
    pin.open(next_write_tid++);
  }

  /**
   * Reserves extents required for rmw, and learn
   * which need to be read
   *
   * Pins all extents in to_write.  Returns subset of to_read not
   * currently present in the cache.  Caller must obtain those
   * extents before calling get_remaining_extents_for_rmw.
   *
   * Transition table:
   * - Empty -> Write Pending pin.reqid
   * - Write Pending N -> Write Pending pin.reqid
   * - Write Pinned N -> Write Pinned pin.reqid
   *
   * @param oid [in] object undergoing rmw
   * @param pin [in,out] pin to use (obtained from create_write_pin)
   * @param to_write [in] extents which will be written
   * @param to_read [in] extents to read prior to write (must be subset
   *                     of to_write)
   * @return subset of to_read which isn't already present or pending
   */
  extent_set reserve_extents_for_rmw(
    const hobject_t &oid,
    write_pin &pin,
    const extent_set &to_write,
    const extent_set &to_read);

  /**
   * Gets extents required for rmw not returned from
   * reserve_extents_for_rmw
   *
   * Requested extents (to_get) must be the set to_read \ the set
   * returned from reserve_extents_for_rmw.  No transition table,
   * all extents at this point must be present and already pinned
   * for this pin by reserve_extents_for_rmw.
   *
   * @param oid [in] object
   * @param pin [in,out] pin associated with this IO
   * @param to_get [in] extents to get (see above for restrictions)
   * @return map of buffers from to_get
   */
  extent_map get_remaining_extents_for_rmw(
    const hobject_t &oid,
    write_pin &pin,
    const extent_set &to_get);

  /**
   * Updates the cache to reflect the rmw write
   *
   * All presented extents must already have been specified in
   * reserve_extents_for_rmw under to_write.
   *
   * Transition table:
   * - Empty -> invalid, must call reserve_extents_for_rmw first
   * - Write Pending N -> Write Pinned N, update buffer
   *     (assert N >= pin.reqid)
   * - Write Pinned N -> Update buffer (assert N >= pin.reqid)
   *
   * @param oid [in] object
   * @param pin [in,out] pin associated with this IO
   * @param extents [in] map of buffers to update
   * @return void
   */
  void present_rmw_update(
    const hobject_t &oid,
    write_pin &pin,
    const extent_map &extents);

  /**
   * Release all buffers pinned by pin
   */
  void release_write_pin(
    write_pin &pin) {
    release_pin(pin);
  }

  std::ostream &print(std::ostream &out) const;
};

std::ostream &operator <<(std::ostream &lhs, const ExtentCache &cache);

#endif
