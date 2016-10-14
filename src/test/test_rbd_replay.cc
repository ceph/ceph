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
#include "rbd_replay/ImageNameMap.hpp"
#include "rbd_replay/ios.hpp"
#include "rbd_replay/rbd_loc.hpp"


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

