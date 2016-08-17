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


#include <gtest/gtest.h>
#include "osd/ExtentCache.h"
#include <iostream>

extent_map imap_from_vector(vector<pair<uint64_t, uint64_t> > &&in)
{
  extent_map out;
  for (auto &&tup: in) {
    bufferlist bl;
    bl.append_zero(tup.second);
    out.insert(tup.first, bl.length(), bl);
  }
  return out;
}

extent_map imap_from_iset(const extent_set &set)
{
  extent_map out;
  for (auto &&iter: set) {
    bufferlist bl;
    bl.append_zero(iter.second);
    out.insert(iter.first, iter.second, bl);
  }
  return out;
}

extent_set iset_from_vector(vector<pair<uint64_t, uint64_t> > &&in)
{
  extent_set out;
  for (auto &&tup: in) {
    out.insert(tup.first, tup.second);
  }
  return out;
}

TEST(extentcache, single_read)
{
  hobject_t oid;

  ExtentCache c;
  ExtentCache::read_pin pin;
  c.open_read_pin(pin);

  auto to_read = iset_from_vector(
    {{0, 5}, {10, 5}, {20, 5}});
  auto results = c.get_or_pin_extents(
    oid, pin, to_read);
  ASSERT_EQ(results.got, extent_map());
  ASSERT_EQ(results.pending, extent_set());
  ASSERT_EQ(results.must_get, to_read);

  auto got = imap_from_iset(to_read);
  auto got2 = c.present_get_extents_read(
    oid,
    pin,
    got,
    to_read);
  ASSERT_EQ(got, got2);

  c.release_read_pin(pin);
}

TEST(extentcache, read_read_overlap)
{
  hobject_t oid;

  ExtentCache c;
  ExtentCache::read_pin pin, pin2;
  c.open_read_pin(pin);
  c.open_read_pin(pin2);

  auto to_read = iset_from_vector(
    {{0, 5}, {10, 5}, {20, 5}});
  auto results = c.get_or_pin_extents(
    oid, pin, to_read);
  ASSERT_EQ(results.got, extent_map());
  ASSERT_EQ(results.pending, extent_set());
  ASSERT_EQ(results.must_get, to_read);

  c.print(std::cerr);

  auto to_read2 = iset_from_vector(
    {{0, 7}, {8, 1}, {11, 3}, {19, 5}});
  auto results2 = c.get_or_pin_extents(
    oid, pin2, to_read2);
  ASSERT_EQ(results2.got, extent_map());
  ASSERT_EQ(
    results2.pending,
    iset_from_vector({{0, 5}, {11, 3}, {20, 4}}));
  ASSERT_EQ(
    results2.must_get,
    iset_from_vector({{5, 2}, {8, 1}, {19, 1}}));

  c.print(std::cerr);

  auto got = imap_from_iset(results.must_get);
  auto complete = c.present_get_extents_read(
    oid,
    pin,
    got,
    to_read);
  ASSERT_EQ(got, complete);

  c.print(std::cerr);

  auto got2 = imap_from_iset(results2.must_get);
  auto complete2 = c.present_get_extents_read(
    oid,
    pin2,
    got2,
    to_read2);
  ASSERT_EQ(imap_from_iset(to_read2), complete2);

  c.release_read_pin(pin);
  c.release_read_pin(pin2);
}

TEST(extentcache, simple_write)
{
  hobject_t oid;

  ExtentCache c;
  ExtentCache::write_pin pin;
  c.open_write_pin(pin);

  auto to_read = iset_from_vector(
    {{0, 2}, {8, 2}, {20, 2}});
  auto to_write = iset_from_vector(
    {{0, 10}, {20, 4}});
  auto must_read = c.reserve_extents_for_rmw(
    oid, pin, to_write, to_read);
  ASSERT_EQ(
    must_read,
    to_read);

  c.print(std::cerr);

  auto got = imap_from_iset(must_read);
  auto pending_read = to_read;
  pending_read.subtract(must_read);

  auto pending = c.get_remaining_extents_for_rmw(
    oid,
    pin,
    pending_read);
  ASSERT_TRUE(pending.empty());

  auto write_map = imap_from_iset(to_write);
  c.present_rmw_update(
    oid,
    pin,
    write_map);

  c.release_write_pin(pin);
}

TEST(extentcache, write_read_overlap)
{
  hobject_t oid;

  ExtentCache c;
  ExtentCache::write_pin pin;
  c.open_write_pin(pin);

  auto to_read = iset_from_vector(
    {{0, 2}, {8, 2}, {20, 2}});
  auto to_write = iset_from_vector(
    {{0, 10}, {20, 4}});
  auto must_read = c.reserve_extents_for_rmw(
    oid, pin, to_write, to_read);
  ASSERT_EQ(
    must_read,
    to_read);

  c.print(std::cerr);

  auto got = imap_from_iset(must_read);
  auto pending_read = to_read;
  pending_read.subtract(must_read);

  auto pending = c.get_remaining_extents_for_rmw(
    oid,
    pin,
    pending_read);
  ASSERT_TRUE(pending.empty());

  auto write_map = imap_from_iset(to_write);
  c.present_rmw_update(
    oid,
    pin,
    write_map);

  // concurrent read
  ExtentCache::read_pin pin2;
  c.open_read_pin(pin2);

  auto to_read2 = iset_from_vector({{5, 10}});
  auto results = c.get_or_pin_extents(
    oid,
    pin2,
    to_read2);
  ASSERT_EQ(
    results.got,
    imap_from_vector({{5, 5}}));
  ASSERT_EQ(
    results.pending,
    extent_set());
  ASSERT_EQ(
    results.must_get,
    iset_from_vector({{10, 5}}));


  auto fulfilled_pending = c.present_get_extents_read(
      oid,
      pin2,
      imap_from_iset(results.must_get),
      results.pending);
  ASSERT_TRUE(fulfilled_pending.empty());

  c.print(std::cerr);

  {
    auto span = iset_from_vector({{1, 24}});
    auto pinned = c.get_all_pinned_spanning_extents(
      oid,
      span);
    ASSERT_EQ(
      pinned,
      imap_from_vector({{1, 14}, {20, 4}}));
  }

  c.release_read_pin(pin2);

  c.print(std::cerr);

  {
    auto span = iset_from_vector({{1, 24}});
    auto pinned = c.get_all_pinned_spanning_extents(
      oid,
      span);
    ASSERT_EQ(
      pinned,
      imap_from_vector({{1, 9}, {20, 4}}));
  }

  c.release_write_pin(pin);

  c.print(std::cerr);

  {
    auto span = iset_from_vector({{1, 24}});
    auto pinned = c.get_all_pinned_spanning_extents(
      oid,
      span);
    ASSERT_EQ(
      pinned,
      extent_map());
  }
}

TEST(extentcache, write_write_overlap)
{
  hobject_t oid;

  ExtentCache c;
  ExtentCache::write_pin pin;
  c.open_write_pin(pin);

  // start write 1
  auto to_read = iset_from_vector(
    {{0, 2}, {8, 2}, {20, 2}});
  auto to_write = iset_from_vector(
    {{0, 10}, {20, 4}});
  auto must_read = c.reserve_extents_for_rmw(
    oid, pin, to_write, to_read);
  ASSERT_EQ(
    must_read,
    to_read);

  c.print(std::cerr);

  // start write 2
  ExtentCache::write_pin pin2;
  c.open_write_pin(pin2);
  auto to_read2 = iset_from_vector(
    {{2, 4}, {10, 4}, {18, 4}});
  auto to_write2 = iset_from_vector(
    {{2, 12}, {18, 12}});
  auto must_read2 = c.reserve_extents_for_rmw(
    oid, pin2, to_write2, to_read2);
  ASSERT_EQ(
    must_read2,
    iset_from_vector({{10, 4}, {18, 2}}));

  c.print(std::cerr);

  // complete read for write 1 and start commit
  auto got = imap_from_iset(must_read);
  auto pending_read = to_read;
  pending_read.subtract(must_read);
  auto pending = c.get_remaining_extents_for_rmw(
    oid,
    pin,
    pending_read);
  ASSERT_TRUE(pending.empty());

  auto write_map = imap_from_iset(to_write);
  c.present_rmw_update(
    oid,
    pin,
    write_map);

  c.print(std::cerr);

  // complete read for write 2 and start commit
  auto pending_read2 = to_read2;
  pending_read2.subtract(must_read2);
  auto pending2 = c.get_remaining_extents_for_rmw(
    oid,
    pin2,
    pending_read2);
  ASSERT_EQ(
    pending2,
    imap_from_iset(pending_read2));

  auto write_map2 = imap_from_iset(to_write2);
  c.present_rmw_update(
    oid,
    pin2,
    write_map2);

  c.print(std::cerr);

  {
    auto span = iset_from_vector({{1, 27}});
    auto pinned = c.get_all_pinned_spanning_extents(
      oid,
      span);
    ASSERT_EQ(
      pinned,
      imap_from_vector({{1, 13}, {18, 10}}));
  }

  c.release_write_pin(pin);

  c.print(std::cerr);

  {
    auto span = iset_from_vector({{1, 27}});
    auto pinned = c.get_all_pinned_spanning_extents(
      oid,
      span);
    ASSERT_EQ(
      pinned,
      imap_from_vector({{2, 12}, {18, 10}}));
  }

  c.release_write_pin(pin2);
}
