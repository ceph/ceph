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
#include <boost/optional.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "include/buffer.h"
#include "common/hobject.h"

/**
   ExtentCache

   The main purpose of this cache is to make sure that extents with
   committed but unapplied writes can be read -- both for normal reads
   and for rmw write cycles.

   To that end we need to ensure that an extent pinned for an operation is
   live until that operation completes.  However, a particular extent
   might be pinned by multiple operations (several pipelined writes
   on the same object, or a read on an extent pinned by a committed, but
   unapplied write).  We'd like a few properties:

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
   invariants guarranteed by higher layers:

   1) Writes on a particular object must be ordered
   2) A particular object will have outstanding reads or writes, but not
      both (note that you can have a read while a write is committed, but
      not applied).

   Our strategy therefore will be to have whichever in-progress op will
   finish "last" be the owner of a particular extent.

   From invariant 2), an extent (which must be part of an object) can be
   participating in one of two pipelines at any particular point:

   Write: [WaitRead -> ] WaitCommit -> WaitApply -> Complete
   Read: WaitRead -> Complete

   [WaitRead -> ] for Write indicates that we may need to start by
   reading the full extents for any partial modifications.
   Invariant 1) above actually indicates that we can't have writes
   bypassing the WaitRead state while there are writes waiting on
   Reads.  Thus, the set of operations pinning a particular extent
   must always complete in order or arrival (What about reads
   overlapping an extent pinned by an unapplied write?  Either could
   complete first!  See below, the read can simply grab the buffer
   in that case, no need pin unless the extent isn't present or is
   pinned by reads).

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
      - The extent must persist until Write reqid N completes
      - All ops pinning this extent are writes in some Write
        state (all are actually possible).  Reads *are* possible
        if the extent is in this state, but if a read comes in,
        it must be the case that all writes pinning the extent
        are in the WaitApply state of the write pipeline.
   3) Read Pending N:
      - Some read with reqid <= N is currently fetching the data for
        this extent.
      - All ops pinning this extent are reads in the WaitRead state
        of the Read pipeline.
      - There can be no unapplied writes in this case.  When a read
        arrives, any writes pinning a particular extent must be
	committed, but not applied.  In that case, we simply take
	the data and run, no need to pin it (more on this below).
	The only way we transition to Pending is if the extent is
	fully applied.  Furthermore, no writes can arrive until
	all reads complete by Invariant 2.
    4) Read Pinned N:
      - Some read with reqid <= N is currently fetching data for
        other extents, and this extent was in state Pending when
	it started.
      - All ops pinning this extent are reads in the WaitRead state
        of the Read pipeline.
      - There can be no unapplied writes in this case (as above).
      - The extent must have transitioned here from Pending.


   States 3 and 4 require a bit of explanation.  Writes and reads have
   somewhat different requirements.  Writes actually cannot "read"
   the extent they want to use for an rmw out of cache until they
   enter WaitCommit, because until that point they cannot be sure
   that there aren't other writes ahead of them in the queue which
   will modify that extent.  Thus, writes need to "reserve" all
   extents they will eventually need up front, but can't actually
   read them until everything is ready to go.  Reads don't have
   that requirement because there can be no writes in progress while
   a read is in progress (again, except for committed, but un-applied
   writes which are ok because transitioning from committed to
   applied does not change the logical contents of the object).

   There is one other kind of read which is important: backfill
   recovery.  When the recovery operation is iniated, writes are
   blocked, so we can be sure that all writes are committed.
   Also, we don't care about ordering of backfill reads w.r.t.
   client reads, so no need to worry about carefully setting
   extents to the WaitRead state.  Thus, all we have to do it just
   check the cache for pinned reads and return them without extending
   the pin or anything.

   All of the above suggests that there are 6 things users can
   ask of the cache corresponding to the 2 read pipeline states,
   3 write pipeline states, and backfill reads. See below for
   the public methods and comments.
 */

/// If someone wants these types, but not ExtentCache, move to another file
struct bl_split_merge {
  bufferlist split(
    uint64_t offset,
    uint64_t length,
    bufferlist &bl) const {
    bufferlist out;
    out.substr_of(bl, offset, length);
    return out;
  }
  bool can_merge(const bufferlist &left, const bufferlist &right) const {
    return true;
  }
  bufferlist merge(bufferlist &&left, bufferlist &&right) const {
    bufferlist bl;
    bl.claim(left);
    bl.claim_append(right);
    return bl;
  }
  uint64_t length(const bufferlist &b) const { return b.length(); }
};
using extent_set = interval_set<uint64_t>;
using extent_map = interval_map<uint64_t, bufferlist, bl_split_merge>;

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
    boost::optional<bufferlist> bl;

    uint64_t get_length() const {
      return length;
    }

    bool is_pending() const {
      return bl == boost::none;
    }

    bool pinned_by_read() const {
      assert(parent_pin_state);
      return parent_pin_state->is_read();
    }

    bool pinned_by_write() const {
      assert(parent_pin_state);
      return parent_pin_state->is_write();
    }

    uint64_t pin_tid() const {
      assert(parent_pin_state);
      return parent_pin_state->tid;
    }

    extent(uint64_t offset, bufferlist _bl)
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
    object_extent_set(const hobject_t &oid) : oid(oid) {}

    using set_member_options = boost::intrusive::member_hook<
      extent,
      boost::intrusive::set_member_hook<>,
      &extent::extent_set_member>;
    using set = boost::intrusive::set<extent, set_member_options>;
    set extent_set;

    bool operator<(const object_extent_set &rhs) const {
      return cmp_bitwise(oid, rhs.oid) < 0;
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
      boost::optional<bufferlist> bl;
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
	assert(!action.bl || action.bl->length() == extlen);
	if (action.action == update_action::UPDATE_PIN) {
	  extent *ext = action.bl ?
	    new extent(offset, *action.bl) :
	    new extent(offset, extlen);
	  ext->link(*this, pin);
	} else {
	  assert(!action.bl);
	}
      }

      for (auto p = range.first; p != range.second;) {
	extent *ext = &*p;
	++p;

	uint64_t extoff = MAX(ext->offset, offset);
	uint64_t extlen = MIN(
	  ext->length - (extoff - ext->offset),
	  offset + length - extoff);

	update_action action;
	f(extoff, extlen, ext, &action);
	assert(!action.bl || action.bl->length() == extlen);
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
	      bufferlist bl;
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
	      bufferlist bl;
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
	      bufferlist bl;
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
	  assert(final_extent);
	  assert(final_extent->length == action.bl->length());
	  final_extent->bl = *(action.bl);
	}

	uint64_t next_off = p == range.second ?
	  offset + length : p->offset;
	if (extoff + extlen < next_off) {
	  uint64_t tailoff = extoff + extlen;
	  uint64_t taillen = next_off - tailoff;

	  update_action action;
	  f(tailoff, taillen, nullptr, &action);
	  assert(!action.bl || action.bl->length() == taillen);
	  if (action.action == update_action::UPDATE_PIN) {
	    extent *ext = action.bl ?
	      new extent(tailoff, *action.bl) :
	      new extent(tailoff, taillen);
	    ext->link(*this, pin);
	  } else {
	    assert(!action.bl);
	  }
	}
      }
    }
  };
  struct Cmp {
    bool operator()(const hobject_t &oid, const object_extent_set &rhs) const {
      return cmp_bitwise(oid, rhs.oid) < 0;
    }
    bool operator()(const object_extent_set &lhs, const hobject_t &oid) const {
      return cmp_bitwise(lhs.oid, oid) < 0;
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
      READ,
    };
    pin_type_t pin_type = NONE;
    bool is_read() const { return pin_type == READ; }
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
      assert(pin_list.empty());
      assert(tid == 0);
      assert(pin_type == NONE);
    }
    void _open(uint64_t in_tid, pin_type_t in_type) {
      assert(pin_type == NONE);
      assert(in_tid > 0);
      tid = in_tid;
      pin_type = in_type;
    }
  };

  void release_pin(pin_state &p) {
    for (auto iter = p.pin_list.begin(); iter != p.pin_list.end(); ) {
      unique_ptr<extent> extent(&*iter); // we now own this
      iter++; // unlink will invalidate
      assert(extent->parent_extent_set);
      auto &eset = *(extent->parent_extent_set);
      extent->unlink();
      remove_and_destroy_if_empty(eset);
    }
    p.tid = 0;
    p.pin_type = pin_state::NONE;
  }

public:
  class read_pin : private pin_state {
    friend class ExtentCache;
    void open(uint64_t in_tid) {
      _open(in_tid, pin_state::READ);
    }
  public:
    read_pin() : pin_state() {}
  };
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

  void open_read_pin(read_pin &pin) {
    pin.open(next_read_tid++);
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
   * - Read Pending N -> invalid, violates Invariant 1
   * - Read Pinned N -> invalid, violates Invariant 1
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
   * - Read Pending N -> invalid, violates Invariant 1
   * - Read Pinned N -> invalid, violates Invariant 1
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

  /**
   * Get read extents available, pin the rest, determine
   * which extents this operation is responsible for
   * fetching
   *
   * The set of requested extents can be partitioned into:
   * - Extents present: just return them
   * - Extents pending: will be present by the time all
   *                    preceding reads are done
   * - Extents not present: must be fetched by this IO
   * The return value reflects this trichotomy.
   *
   *  Transition table:
   *  - Empty -> Read Pending pin.reqid (goes into fetch, need)
   *  - Write Pending N -> invalid, violates Invariant 1
   *  - Write Pinned N -> no change, no new pin, goes into got
   *  - Read Pending N -> Read Pending pin.reqid (goes into need)
   *  - Read Pinned N -> no change, no new pin, goes into got  *
   *
   * @param oid [in] object
   * @param pin [in,out] pin associated with this IO
   * @param to_get [in] requested extents
   * @return { got: extents already in cache
   *           pending: extents with pending reads
   *           must_get: extents which this operation must read
   *         }
   */
  struct get_or_pin_results {
    extent_map got;
    extent_set pending;
    extent_set must_get;
  };
  get_or_pin_results get_or_pin_extents(
    const hobject_t &oid,
    read_pin &pin,
    const extent_set &to_get);

  /**
   * Present extents from must_get, get extents
   * from pending.
   *
   * Transition table:
   * - Empty -> invalid, must be pinned or would have been in got
   * - Write Pending N -> invalid, violates Invariant 1
   * - Write Pinned N -> invalid, would have been in got in 5)
   * - Read Pending pin.reqid -> Empty it's our pin, just drop it
   * - Read Pending N -> Read Pinned N (assert N > pin.reqid)
   * - Read Pinned N -> invalid, only one fetcher
   *
   * @param oid [in] object
   * @param pin [in,out] pin associated with this IO
   * @param got [in] extents requested by get_or_pin_results
   * @param need [in] exents still required
   * @return map of extents in need
   */
  extent_map present_get_extents_read(
    const hobject_t &oid,
    read_pin &pin,
    const extent_map &got,
    const extent_set &need);

  /**
   * Get all extents spanning the specified extent_set -- don't pin them
   *
   * No transition table, doesn't pin anything
   *
   * @param oid [in] object
   * @param need [in] extents to read
   * @return map of overlapping extents
   */
  extent_map get_all_pinned_spanning_extents(
    const hobject_t &oid,
    const extent_set &need);

  /**
   * Release all buffers pinned by pin
   */
  void release_read_pin(
    read_pin &pin) {
    release_pin(pin);
  }

  ostream &print(
    ostream &out) const;
};

ostream &operator<<(ostream &lhs, const ExtentCache &cache);

#endif
