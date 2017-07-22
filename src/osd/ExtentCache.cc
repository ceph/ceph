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

#include "ExtentCache.h"

void ExtentCache::extent::_link_pin_state(pin_state &pin_state)
{
  assert(parent_extent_set);
  assert(!parent_pin_state);
  parent_pin_state = &pin_state;
  pin_state.pin_list.push_back(*this);
}

void ExtentCache::extent::_unlink_pin_state()
{
  assert(parent_extent_set);
  assert(parent_pin_state);
  auto liter = pin_state::list::s_iterator_to(*this);
  parent_pin_state->pin_list.erase(liter);
  parent_pin_state = nullptr;
}

void ExtentCache::extent::unlink()
{
  assert(parent_extent_set);
  assert(parent_pin_state);

  _unlink_pin_state();

  // remove from extent set
  {
    auto siter = object_extent_set::set::s_iterator_to(*this);
    auto &set = object_extent_set::set::container_from_iterator(siter);
    assert(&set == &(parent_extent_set->extent_set));
    set.erase(siter);
  }

  parent_extent_set = nullptr;
  assert(!parent_pin_state);
}

void ExtentCache::extent::link(
  object_extent_set &extent_set,
  pin_state &pin_state)
{
  assert(!parent_extent_set);
  parent_extent_set = &extent_set;
  extent_set.extent_set.insert(*this);

  _link_pin_state(pin_state);
}

void ExtentCache::extent::move(
  pin_state &to)
{
  _unlink_pin_state();
  _link_pin_state(to);
}

void ExtentCache::remove_and_destroy_if_empty(object_extent_set &eset)
{
  if (eset.extent_set.empty()) {
    auto siter = cache_set::s_iterator_to(eset);
    auto &set = cache_set::container_from_iterator(siter);
    assert(&set == &per_object_caches);

    // per_object_caches owns eset
    per_object_caches.erase(eset);
    delete &eset;
  }
}

ExtentCache::object_extent_set &ExtentCache::get_or_create(
  const hobject_t &oid)
{
  cache_set::insert_commit_data data;
  auto p = per_object_caches.insert_check(oid, Cmp(), data);
  if (p.second) {
    auto *eset = new object_extent_set(oid);
    per_object_caches.insert_commit(*eset, data);
    return *eset;
  } else {
    return *(p.first);
  }
}

ExtentCache::object_extent_set *ExtentCache::get_if_exists(
  const hobject_t &oid)
{
  cache_set::insert_commit_data data;
  auto p = per_object_caches.insert_check(oid, Cmp(), data);
  if (p.second) {
    return nullptr;
  } else {
    return &*(p.first);
  }
}

std::pair<
  ExtentCache::object_extent_set::set::iterator,
  ExtentCache::object_extent_set::set::iterator
  > ExtentCache::object_extent_set::get_containing_range(
    uint64_t off, uint64_t len)
{
  // fst is first iterator with end after off (may be end)
  auto fst = extent_set.upper_bound(off, uint_cmp());
  if (fst != extent_set.begin())
    --fst;
  if (fst != extent_set.end() && off >= (fst->offset + fst->get_length()))
    ++fst;

  // lst is first iterator with start >= off + len (may be end)
  auto lst = extent_set.lower_bound(off + len, uint_cmp());
  return std::make_pair(fst, lst);
}

extent_set ExtentCache::reserve_extents_for_rmw(
  const hobject_t &oid,
  write_pin &pin,
  const extent_set &to_write,
  const extent_set &to_read)
{
  if (to_write.empty() && to_read.empty()) {
    return extent_set();
  }
  extent_set must_read;
  auto &eset = get_or_create(oid);
  extent_set missing;
  for (auto &&res: to_write) {
    eset.traverse_update(
      pin,
      res.first,
      res.second,
      [&](uint64_t off, uint64_t len,
	  extent *ext, object_extent_set::update_action *action) {
	action->action = object_extent_set::update_action::UPDATE_PIN;
	if (!ext) {
	  missing.insert(off, len);
	}
      });
  }
  must_read.intersection_of(
    to_read,
    missing);
  return must_read;
}

extent_map ExtentCache::get_remaining_extents_for_rmw(
  const hobject_t &oid,
  write_pin &pin,
  const extent_set &to_get)
{
  if (to_get.empty()) {
    return extent_map();
  }
  extent_map ret;
  auto &eset = get_or_create(oid);
  for (auto &&res: to_get) {
    bufferlist bl;
    uint64_t cur = res.first;
    eset.traverse_update(
      pin,
      res.first,
      res.second,
      [&](uint64_t off, uint64_t len,
	  extent *ext, object_extent_set::update_action *action) {
	assert(off == cur);
	cur = off + len;
	action->action = object_extent_set::update_action::NONE;
	assert(ext && ext->bl && ext->pinned_by_write());
	bl.substr_of(
	  *(ext->bl),
	  off - ext->offset,
	  len);
	ret.insert(off, len, bl);
      });
  }
  return ret;
}

void ExtentCache::present_rmw_update(
  const hobject_t &oid,
  write_pin &pin,
  const extent_map &extents)
{
  if (extents.empty()) {
    return;
  }
  auto &eset = get_or_create(oid);
  for (auto &&res: extents) {
    eset.traverse_update(
      pin,
      res.get_off(),
      res.get_len(),
      [&](uint64_t off, uint64_t len,
	  extent *ext, object_extent_set::update_action *action) {
	action->action = object_extent_set::update_action::NONE;
	assert(ext && ext->pinned_by_write());
	action->bl = bufferlist();
	action->bl->substr_of(
	  res.get_val(),
	  off - res.get_off(),
	  len);
      });
  }
}

ostream &ExtentCache::print(ostream &out) const
{
  out << "ExtentCache(" << std::endl;
  for (auto esiter = per_object_caches.begin();
       esiter != per_object_caches.end();
       ++esiter) {
    out << "  Extents(" << esiter->oid << ")[" << std::endl;
    for (auto exiter = esiter->extent_set.begin();
	 exiter != esiter->extent_set.end();
	 ++exiter) {
      out << "    Extent(" << exiter->offset
	  << "~" << exiter->get_length()
	  << ":" << exiter->pin_tid()
	  << ")" << std::endl;
    }
  }
  return out << ")" << std::endl;
}

ostream &operator<<(ostream &lhs, const ExtentCache &cache)
{
  return cache.print(lhs);
}
