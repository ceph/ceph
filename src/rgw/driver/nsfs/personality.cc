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
  ceph_assert(slot < slots.size());
  ceph_assert(slots[slot].pins > 0);
  --slots[slot].pins;
}

bool PersonalityTable::invalidate(uint32_t slot)
{
  if (slots[slot].pins > 0) {
    /* A live operation is still submitting under it.  Leave it;  the
     * next caller after that operation finishes will find it stale
     * again and drop it then.  Registering a second personality for
     * the same identity would be the alternative, and it would make
     * the table's size a function of how often an identity is
     * rewritten. */
    return false;
  }
  if (registrar) {
    registrar->unregister_personality(nullptr, slots[slot].id);
  }
  by_key.erase(slots[slot].key);
  slots[slot].occupied = false;
  slots[slot].cred = Credentials{};
  return true;
}

int PersonalityTable::find(const rgw_user& key, PersonalityRef* out,
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
  if (gen_source && (slots[slot].gen != gen_source->generation(key))) {
    ++stats.stale;
    const bool dropped = invalidate(slot);
    if (dpp) {
      ldpp_dout(dpp, 20) << "nsfs: personality for " << key
	<< " is stale (slot gen " << slots[slot].gen << ", now "
	<< gen_source->generation(key) << ");  "
	<< (dropped ? "dropped" : "PINNED, serving the old one")
	<< dendl;
    }
    if (dropped) {
      ++stats.misses;
      return -ENOENT;
    }
    /* pinned, so it could not be dropped -- fall through and serve
     * the old one rather than failing a request mid-flight */
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
			     const rgw_user& key, const Credentials& cred,
			     PersonalityRef* out)
{
  /* A concurrent insert of the same key cannot happen -- one task
   * owns this table -- but a caller may insert twice if it did not
   * check find() first.  Answer from what is there rather than
   * registering a second time. */
  if (auto it = by_key.find(key); it != by_key.end()) {
    const uint32_t slot = it->second;
    const bool stale = gen_source &&
	(slots[slot].gen != gen_source->generation(key));
    if (! stale || ! invalidate(slot)) {
      ++slots[slot].pins;
      ++stats.hits;
      *out = PersonalityRef(this, slot, slots[slot].id);
      return 0;
    }
    ++stats.stale;
    /* dropped;  fall through and register the new credentials */
  }

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
  slots[slot].gen = gen_source ? gen_source->generation(key) : 0;
  slots[slot].id = static_cast<uint16_t>(id);
  slots[slot].occupied = true;
  slots[slot].pins = 1;
  by_key[key] = slot;
  ++stats.registrations;

  *out = PersonalityRef(this, slot, slots[slot].id);
  return 0;
}

} } } // namespace rgw::sal::nsfs
