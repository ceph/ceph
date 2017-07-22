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

  c.release_write_pin(pin);

  c.print(std::cerr);

  c.release_write_pin(pin2);
}

TEST(extentcache, write_write_overlap2)
{
  hobject_t oid;

  ExtentCache c;
  ExtentCache::write_pin pin;
  c.open_write_pin(pin);

  // start write 1
  auto to_read = extent_set();
  auto to_write = iset_from_vector(
    {{659456, 4096}});
  auto must_read = c.reserve_extents_for_rmw(
    oid, pin, to_write, to_read);
  ASSERT_EQ(
    must_read,
    to_read);

  c.print(std::cerr);

  // start write 2
  ExtentCache::write_pin pin2;
  c.open_write_pin(pin2);
  auto to_read2 = extent_set();
  auto to_write2 = iset_from_vector(
    {{663552, 4096}});
  auto must_read2 = c.reserve_extents_for_rmw(
    oid, pin2, to_write2, to_read2);
  ASSERT_EQ(
    must_read2,
    to_read2);


  // start write 3
  ExtentCache::write_pin pin3;
  c.open_write_pin(pin3);
  auto to_read3 = iset_from_vector({{659456, 8192}});
  auto to_write3 = iset_from_vector({{659456, 8192}});
  auto must_read3 = c.reserve_extents_for_rmw(
    oid, pin3, to_write3, to_read3);
  ASSERT_EQ(
    must_read3,
    extent_set());

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

  // complete read for write 2 and start commit
  auto pending_read3 = to_read3;
  pending_read3.subtract(must_read3);
  auto pending3 = c.get_remaining_extents_for_rmw(
    oid,
    pin3,
    pending_read3);
  ASSERT_EQ(
    pending3,
    imap_from_iset(pending_read3));

  auto write_map3 = imap_from_iset(to_write3);
  c.present_rmw_update(
    oid,
    pin3,
    write_map3);


  c.print(std::cerr);

  c.release_write_pin(pin);

  c.print(std::cerr);

  c.release_write_pin(pin2);

  c.print(std::cerr);

  c.release_write_pin(pin3);
}
