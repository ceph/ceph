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

#include "common/errno.h"

#include "personality.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* sqe->personality is __u16, so an id above this could not be named
 * on a submission entry even if the kernel handed one out. */
static constexpr size_t MAX_PERSONALITIES = 65535;

void PersonalityRef::reset()
{
  if (table) {
    table->unpin(slot);
    table = nullptr;
  }
}

const Credentials& PersonalityRef::credentials() const
{
  ceph_assert(table != nullptr);
  return table->slots[slot].cred;
}

uint64_t PersonalityRef::generation() const
{
  ceph_assert(table != nullptr);
  return table->slots[slot].gen;
}

PersonalityTable::PersonalityTable(size_t capacity, Registrar* reg,
				   const GenerationSource* gen)
  : registrar(reg), gen_source(gen)
{
  if (capacity > MAX_PERSONALITIES) {
    capacity = MAX_PERSONALITIES;
  }
  if (capacity == 0) {
    capacity = 1;
  }
  slots.resize(capacity);
  by_key.reserve(capacity);
}

PersonalityTable::~PersonalityTable()
{
  /* Unregister what we registered.  The ring is going away in the
   * usual case and would drop these anyway, but a table that
   * outlives its contents in a test should not leak them. */
  for (const auto& s : slots) {
    if (s.occupied && registrar) {
      registrar->unregister_personality(nullptr, s.id);
    }
  }
}

void PersonalityTable::unpin(uint32_t slot)
{
  ceph_assert(std::this_thread::get_id() == owner);
  ceph_assert(slot < slots.size());
  ceph_assert(slots[slot].pins > 0);
  --slots[slot].pins;
  if ((slots[slot].pins == 0) && slots[slot].detached) {
    release(slot);
  }
}

void PersonalityTable::release(uint32_t slot)
{
  if (registrar) {
    registrar->unregister_personality(nullptr, slots[slot].id);
  }
  slots[slot].occupied = false;
  slots[slot].detached = false;
  slots[slot].cred = Credentials{};
}

void PersonalityTable::invalidate(uint32_t slot)
{
  if (auto it = by_key.find(slots[slot].key);
      (it != by_key.end()) && (it->second == slot)) {
    by_key.erase(it);
  }
  if (slots[slot].pins > 0) {
    /* A live submission still names it.  Out of the index now, so no
     * one else is given it;  given back when that submission ends. */
    slots[slot].detached = true;
    return;
  }
  release(slot);
}

uint64_t PersonalityTable::current_generation(const rgw_owner& key) const
{
  return gen_source ? gen_source->generation(key) : 0;
}

int PersonalityTable::find(const rgw_owner& key, PersonalityRef* out,
			   const DoutPrefixProvider* dpp)
{
  auto it = by_key.find(key);
  if (it == by_key.end()) {
    ++stats.misses;
    if (dpp) {
      ldpp_dout(dpp, 20) << "nsfs: personality miss for " << key << dendl;
    }
    return -ENOENT;
  }
  const uint32_t slot = it->second;

  /* Stale?  The record this was minted from has been rewritten, so
   * the credentials in the slot are no longer what this identity
   * is. */
  if (slots[slot].gen != current_generation(key)) {
    ++stats.stale;
    if (dpp) {
      ldpp_dout(dpp, 20) << "nsfs: personality for " << key
	<< " is stale (slot gen " << slots[slot].gen << ", now "
	<< current_generation(key) << ");  out of the index"
	<< (slots[slot].pins ? ", given back at its last unpin" : "")
	<< dendl;
    }
    invalidate(slot);
    ++stats.misses;
    return -ENOENT;
  }

  ++slots[slot].pins;
  ++stats.hits;
  if (dpp) {
    ldpp_dout(dpp, 20) << "nsfs: personality hit for " << key << " id "
      << slots[slot].id << " uid " << slots[slot].cred.uid << " with "
      << slots[slot].cred.groups.size() << " groups" << dendl;
  }
  *out = PersonalityRef(this, slot, slots[slot].id);
  return 0;
}

uint32_t PersonalityTable::claim_slot(const DoutPrefixProvider* dpp)
{
  const uint32_t n = slots.size();

  /* FIFO:  the cursor advances and takes what it lands on, skipping
   * anything a live operation still holds.  Age rather than use is
   * the right order only because the table is meant to exceed the
   * working set;  if it does not, evictions here become the
   * registration churn that says the table is too small. */
  for (uint32_t tried = 0; tried < n; ++tried) {
    const uint32_t slot = cursor;
    cursor = (cursor + 1) % n;

    if (slots[slot].pins > 0) {
      continue;
    }
    if (slots[slot].occupied) {
      if (registrar) {
	registrar->unregister_personality(dpp, slots[slot].id);
      }
      by_key.erase(slots[slot].key);
      slots[slot].occupied = false;
      ++stats.evictions;
    }
    return slot;
  }
  return n;				/* every slot pinned */
}

int PersonalityTable::insert(const DoutPrefixProvider* dpp,
			     const rgw_owner& key, const Credentials& cred,
			     PersonalityRef* out)
{
  return acquire(dpp, key, cred, current_generation(key), out);
}

int PersonalityTable::place(const DoutPrefixProvider* dpp,
			    const rgw_owner& key, const Credentials& cred,
			    uint64_t gen, bool indexed, PersonalityRef* out)
{
  const uint32_t slot = claim_slot(dpp);
  if (slot == slots.size()) {
    ++stats.exhausted;
    ldpp_dout(dpp, 0) << "ERROR: every personality slot is held by a live"
      " operation (" << slots.size() << " of " << slots.size() << ");"
      " cannot serve " << key << dendl;
    return -EBUSY;
  }

  const int id = registrar
      ? registrar->register_personality(dpp, cred)
      : -ENOTSUP;
  if (id < 0) {
    ++stats.failed;
    ldpp_dout(dpp, 0) << "ERROR: could not register a personality for "
      << key << " (uid " << cred.uid << " gid " << cred.gid << "): "
      << cpp_strerror(-id) << dendl;
    return id;
  }
  if (static_cast<size_t>(id) > MAX_PERSONALITIES) {
    /* Unnameable on an SQE.  Give it straight back rather than
     * store something that can never be used. */
    ++stats.failed;
    ldpp_dout(dpp, 0) << "ERROR: personality id " << id << " exceeds the"
      " range of sqe->personality" << dendl;
    registrar->unregister_personality(dpp, static_cast<uint16_t>(id));
    return -EOVERFLOW;
  }

  slots[slot].key = key;
  slots[slot].cred = cred;
  slots[slot].gen = gen;
  slots[slot].id = static_cast<uint16_t>(id);
  slots[slot].occupied = true;
  slots[slot].detached = !indexed;
  slots[slot].pins = 1;
  if (indexed) {
    by_key[key] = slot;
  }
  ++stats.registrations;

  *out = PersonalityRef(this, slot, slots[slot].id);
  return 0;
}

int PersonalityTable::acquire(const DoutPrefixProvider* dpp,
			      const rgw_owner& key, const Credentials& cred,
			      uint64_t gen, PersonalityRef* out)
{
  const uint64_t current = current_generation(key);
  if (auto it = by_key.find(key); it != by_key.end()) {
    const uint32_t slot = it->second;
    if (slots[slot].gen != current) {
      ++stats.stale;
      invalidate(slot);
    } else if (gen == current) {
      ++slots[slot].pins;
      ++stats.hits;
      *out = PersonalityRef(this, slot, slots[slot].id);
      return 0;
    }
  }
  /* Indexed only when the credentials are current and nothing current
   * is indexed:  an older operation's credentials are its own. */
  return place(dpp, key, cred, gen,
	       (gen == current) && !by_key.contains(key), out);
}

} } } // namespace rgw::sal::nsfs
