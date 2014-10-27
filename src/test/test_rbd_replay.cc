// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/escape.h"
#include "gtest/gtest.h"
#include <stdint.h>
#include <boost/foreach.hpp>
#include <cstdarg>
#include "rbd_replay/Deser.hpp"
#include "rbd_replay/ImageNameMap.hpp"
#include "rbd_replay/ios.hpp"
#include "rbd_replay/rbd_loc.hpp"
#include "rbd_replay/Ser.hpp"


using namespace rbd_replay;

std::ostream& operator<<(std::ostream& o, const rbd_loc& name) {
  return o << "('" << name.pool << "', '" << name.image << "', '" << name.snap << "')";
}

static void add_mapping(ImageNameMap *map, std::string mapping_string) {
  ImageNameMap::Mapping mapping;
  if (!map->parse_mapping(mapping_string, &mapping)) {
    ASSERT_TRUE(false) << "Failed to parse mapping string '" << mapping_string << "'";
  }
  map->add_mapping(mapping);
}

TEST(RBDReplay, Ser) {
  std::ostringstream oss;
  rbd_replay::Ser ser(oss);
  ser.write_uint32_t(0x01020304u);
  ser.write_string("hello");
  ser.write_bool(true);
  ser.write_bool(false);
  std::string s(oss.str());
  const char* data = s.data();
  size_t size = s.size();
  ASSERT_EQ(15U, size);
  const char expected_data[] = {1, 2, 3, 4, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o', 1, 0};
  for (size_t i = 0; i < size; i++) {
    EXPECT_EQ(expected_data[i], data[i]);
  }
}

TEST(RBDReplay, Deser) {
  const char data[] = {1, 2, 3, 4, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o', 1, 0};
  const std::string s(data, sizeof(data));
  std::istringstream iss(s);
  rbd_replay::Deser deser(iss);
  EXPECT_FALSE(deser.eof());
  EXPECT_EQ(0x01020304u, deser.read_uint32_t());
  EXPECT_FALSE(deser.eof());
  EXPECT_EQ("hello", deser.read_string());
  EXPECT_FALSE(deser.eof());
  EXPECT_TRUE(deser.read_bool());
  EXPECT_FALSE(deser.eof());
  EXPECT_FALSE(deser.read_bool());
  EXPECT_FALSE(deser.eof());
  deser.read_uint8_t();
  EXPECT_TRUE(deser.eof());
}

TEST(RBDReplay, ImageNameMap) {
  ImageNameMap m;
  add_mapping(&m, "x@y=y@x");
  add_mapping(&m, "a\\=b@c=h@i");
  add_mapping(&m, "a@b\\=c=j@k");
  add_mapping(&m, "a\\@b@c=d@e");
  add_mapping(&m, "a@b\\@c=f@g");
  add_mapping(&m, "image@snap_1=image_1");
  ImageNameMap::Mapping mapping;
  EXPECT_FALSE(m.parse_mapping("bad=@@@", &mapping));
  EXPECT_FALSE(m.parse_mapping("bad==stuff", &mapping));
  EXPECT_EQ(rbd_loc("", "y", "x"), m.map(rbd_loc("", "x", "y")));
  EXPECT_EQ(rbd_loc("", "h", "i"), m.map(rbd_loc("", "a=b", "c")));
  EXPECT_EQ(rbd_loc("", "j", "k"), m.map(rbd_loc("", "a", "b=c")));
  EXPECT_EQ(rbd_loc("", "d", "e"), m.map(rbd_loc("", "a@b", "c")));
  EXPECT_EQ(rbd_loc("", "f", "g"), m.map(rbd_loc("", "a", "b@c")));
  EXPECT_EQ(rbd_loc("", "image_1", ""), m.map(rbd_loc("", "image", "snap_1")));
}

TEST(RBDReplay, rbd_loc_str) {
  EXPECT_EQ("", rbd_loc("", "", "").str());
  EXPECT_EQ("a/", rbd_loc("a", "", "").str());
  EXPECT_EQ("b", rbd_loc("", "b", "").str());
  EXPECT_EQ("a/b", rbd_loc("a", "b", "").str());
  EXPECT_EQ("@c", rbd_loc("", "", "c").str());
  EXPECT_EQ("a/@c", rbd_loc("a", "", "c").str());
  EXPECT_EQ("b@c", rbd_loc("", "b", "c").str());
  EXPECT_EQ("a/b@c", rbd_loc("a", "b", "c").str());
  EXPECT_EQ("a\\@x/b\\@y@c\\@z", rbd_loc("a@x", "b@y", "c@z").str());
  EXPECT_EQ("a\\/x/b\\/y@c\\/z", rbd_loc("a/x", "b/y", "c/z").str());
  EXPECT_EQ("a\\\\x/b\\\\y@c\\\\z", rbd_loc("a\\x", "b\\y", "c\\z").str());
}

TEST(RBDReplay, rbd_loc_parse) {
  rbd_loc m("x", "y", "z");

  EXPECT_TRUE(m.parse(""));
  EXPECT_EQ("", m.pool);
  EXPECT_EQ("", m.image);
  EXPECT_EQ("", m.snap);

  EXPECT_TRUE(m.parse("a/"));
  EXPECT_EQ("a", m.pool);
  EXPECT_EQ("", m.image);
  EXPECT_EQ("", m.snap);

  EXPECT_TRUE(m.parse("b"));
  EXPECT_EQ("", m.pool);
  EXPECT_EQ("b", m.image);
  EXPECT_EQ("", m.snap);

  EXPECT_TRUE(m.parse("a/b"));
  EXPECT_EQ("a", m.pool);
  EXPECT_EQ("b", m.image);
  EXPECT_EQ("", m.snap);

  EXPECT_TRUE(m.parse("@c"));
  EXPECT_EQ("", m.pool);
  EXPECT_EQ("", m.image);
  EXPECT_EQ("c", m.snap);

  EXPECT_TRUE(m.parse("a/@c"));
  EXPECT_EQ("a", m.pool);
  EXPECT_EQ("", m.image);
  EXPECT_EQ("c", m.snap);

  EXPECT_TRUE(m.parse("b@c"));
  EXPECT_EQ("", m.pool);
  EXPECT_EQ("b", m.image);
  EXPECT_EQ("c", m.snap);

  EXPECT_TRUE(m.parse("a/b@c"));
  EXPECT_EQ("a", m.pool);
  EXPECT_EQ("b", m.image);
  EXPECT_EQ("c", m.snap);

  EXPECT_TRUE(m.parse("a\\@x/b\\@y@c\\@z"));
  EXPECT_EQ("a@x", m.pool);
  EXPECT_EQ("b@y", m.image);
  EXPECT_EQ("c@z", m.snap);

  EXPECT_TRUE(m.parse("a\\/x/b\\/y@c\\/z"));
  EXPECT_EQ("a/x", m.pool);
  EXPECT_EQ("b/y", m.image);
  EXPECT_EQ("c/z", m.snap);

  EXPECT_TRUE(m.parse("a\\\\x/b\\\\y@c\\\\z"));
  EXPECT_EQ("a\\x", m.pool);
  EXPECT_EQ("b\\y", m.image);
  EXPECT_EQ("c\\z", m.snap);

  EXPECT_FALSE(m.parse("a@b@c"));
  EXPECT_FALSE(m.parse("a/b/c"));
  EXPECT_FALSE(m.parse("a@b/c"));
}

static IO::ptr mkio(action_id_t ionum, int num_expected, ...) {
  IO::ptr io(new StartThreadIO(ionum, ionum, 0));

  va_list ap;
  va_start(ap, num_expected);
  for (int i = 0; i < num_expected ; i++) {
    IO::ptr* dep = va_arg(ap, IO::ptr*);
    if (!dep) {
      break;
    }
    io->dependencies().insert(*dep);
  }
  va_end(ap);

  return io;
}

TEST(RBDReplay, batch_unreachable_from) {
  io_set_t deps;
  io_set_t base;
  io_set_t unreachable;
  IO::ptr io1(mkio(1, 0));
  IO::ptr io2(mkio(2, 1, &io1));
  IO::ptr io3(mkio(3, 1, &io2));
  IO::ptr io4(mkio(4, 1, &io1));
  IO::ptr io5(mkio(5, 2, &io2, &io4));
  IO::ptr io6(mkio(6, 2, &io3, &io5));
  IO::ptr io7(mkio(7, 1, &io4));
  IO::ptr io8(mkio(8, 2, &io5, &io7));
  IO::ptr io9(mkio(9, 2, &io6, &io8));
  // 1 (deps) <-- 2 (deps) <-- 3 (deps)
  // ^            ^            ^
  // |            |            |
  // 4 <--------- 5 (base) <-- 6 (deps)
  // ^            ^            ^
  // |            |            |
  // 7 <--------- 8 <--------- 9
  deps.insert(io1);
  deps.insert(io2);
  deps.insert(io3);
  deps.insert(io6);
  base.insert(io5);
  // Anything in 'deps' which is not reachable from 'base' is added to 'unreachable'
  batch_unreachable_from(deps, base, &unreachable);
  EXPECT_EQ(0U, unreachable.count(io1));
  EXPECT_EQ(0U, unreachable.count(io2));
  EXPECT_EQ(1U, unreachable.count(io3));
  EXPECT_EQ(0U, unreachable.count(io4));
  EXPECT_EQ(0U, unreachable.count(io5));
  EXPECT_EQ(1U, unreachable.count(io6));
  EXPECT_EQ(0U, unreachable.count(io7));
  EXPECT_EQ(0U, unreachable.count(io8));
  EXPECT_EQ(0U, unreachable.count(io9));
}
