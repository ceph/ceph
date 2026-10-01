// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <vector>

#include "common/dout.h"
#include "rgw_common.h"

#include "xxhash.h"

#include "driver/posix/unordered_dense.h"
#include "identity_db.h"

/* An allocation-free hash for rgw_user.
 *
 * The obvious to_str() allocates for anything past the small-string
 * bound, and an account member's id is a 36-character UUID, so every
 * lookup would allocate.  Hashing the three members in place reads
 * the same bytes and allocates nothing.
 *
 * XXH3 rather than the wyhash bundled inside unordered_dense,
 * because rgw already consumes XXH3 -- `rgw_xxh_digest.h` wraps it
 * for checksums and `rgw_d3n_cacherequest.h` uses the one-shot form
 * for exactly this, hashing a name into a key.  Chaining through
 * the seed combines the three without concatenating them. */
namespace ankerl::unordered_dense {
template <>
struct hash<rgw_user> {
  using is_avalanching = void;
  auto operator()(const rgw_user& u) const noexcept -> uint64_t {
    uint64_t h = XXH3_64bits(u.id.data(), u.id.size());
    h = XXH3_64bits_withSeed(u.tenant.data(), u.tenant.size(), h);
    return XXH3_64bits_withSeed(u.ns.data(), u.ns.size(), h);
  }
};
} // namespace ankerl::unordered_dense

namespace rgw { namespace sal { namespace nsfs {

class PersonalityTable;

/* A pinned slot.
 *
 * Holds a personality id and keeps its slot from being reused for as
 * long as it lives.  An operation takes one at its first I/O and
 * holds it until it is destroyed, which is what stops the cursor
 * wrapping onto a slot a live request still depends on -- including
 * between two reads of the same operation, when nothing is in flight
 * at all and an in-flight count would see the slot as free. */
class PersonalityRef {
  PersonalityTable* table{nullptr};
  uint32_t slot{0};
  uint16_t pid{0};

  friend class PersonalityTable;
  PersonalityRef(PersonalityTable* t, uint32_t s, uint16_t p)
    : table(t), slot(s), pid(p) {}

public:
  PersonalityRef() = default;
  ~PersonalityRef() { reset(); }

  PersonalityRef(const PersonalityRef&) = delete;
  PersonalityRef& operator=(const PersonalityRef&) = delete;

  PersonalityRef(PersonalityRef&& o) noexcept
    : table(o.table), slot(o.slot), pid(o.pid) { o.table = nullptr; }

  PersonalityRef& operator=(PersonalityRef&& o) noexcept {
    if (this != &o) {
      reset();
      table = o.table; slot = o.slot; pid = o.pid;
      o.table = nullptr;
    }
    return *this;
  }

  void reset();

  bool valid() const { return table != nullptr; }
  /* The value for sqe->personality.  Only meaningful while valid. */
  uint16_t id() const { return pid; }
};

/* One ring's personality table.
 *
 * Per ring, because a personality belongs to the ring it was
 * registered on and its id means nothing anywhere else.  There is no
 * locking: a ring has one submitting task, so its table does too.
 *
 * Registration is injected rather than called directly, so the parts
 * worth testing -- eviction order, pinning, the all-pinned case --
 * can be tested with no ring, no capabilities and no root.
 */
class PersonalityTable {
public:
  /* How a credential becomes a personality.
   *
   * The real one registers on this thread's io_uring ring, which
   * copies the calling task's credentials, so it must change them
   * for the length of that one call.  A test one need not. */
  struct Registrar {
    virtual ~Registrar() = default;
    /* >= 0 is a personality id;  < 0 is -errno. */
    virtual int register_personality(const DoutPrefixProvider* dpp,
				     const Credentials& cred) = 0;
    virtual void unregister_personality(const DoutPrefixProvider* dpp,
					uint16_t id) = 0;
  };

  struct Stats {
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t registrations{0};
    uint64_t evictions{0};
    uint64_t exhausted{0};		/* every slot pinned */
    uint64_t failed{0};			/* the registrar said no */
  };

  /* `capacity` is clamped to the id space:  sqe->personality is
   * __u16, so a table larger than 65535 could not be addressed. */
  PersonalityTable(size_t capacity, Registrar* reg);
  ~PersonalityTable();

  PersonalityTable(const PersonalityTable&) = delete;
  PersonalityTable& operator=(const PersonalityTable&) = delete;

  /* A pinned reference if this identity is already registered here.
   *
   *   0        `out` is pinned and usable
   *   -ENOENT  not present;  the caller resolves credentials and
   *            calls insert()
   *
   * Separate from insert() so that a hit costs no credential
   * resolution -- which on the S3 path is a database read. */
  int find(const rgw_user& key, PersonalityRef* out);

  /* Register `cred` and install it, evicting if the table is full.
   *
   *   0        `out` is pinned and usable
   *   -EBUSY   every slot is pinned;  the caller must fail the
   *            request rather than serve it unimpersonated
   *   < 0      whatever the registrar returned
   */
  int insert(const DoutPrefixProvider* dpp, const rgw_user& key,
	     const Credentials& cred, PersonalityRef* out);

  size_t capacity() const { return slots.size(); }
  size_t size() const { return by_key.size(); }
  const Stats& get_stats() const { return stats; }

private:
  friend class PersonalityRef;

  struct Slot {
    rgw_user key;
    uint16_t id{0};
    uint32_t pins{0};
    bool occupied{false};
  };

  void unpin(uint32_t slot);
  /* The next slot the cursor can take, skipping pinned ones.
   * Returns capacity() when every slot is pinned. */
  uint32_t claim_slot(const DoutPrefixProvider* dpp);

  std::vector<Slot> slots;
  ankerl::unordered_dense::map<rgw_user, uint32_t> by_key;
  uint32_t cursor{0};
  Registrar* registrar;
  Stats stats;
};

} } } // namespace rgw::sal::nsfs
