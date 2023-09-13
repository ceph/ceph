// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "common/SloppyCRCMap.h"
#include "common/Formatter.h"
#include <gtest/gtest.h>

using namespace std;

void dump(const SloppyCRCMap& scm)
{
  auto f = Formatter::create_unique("json-pretty");
  f->open_object_section("map");
  scm.dump(f.get());
  f->close_section();
  f->flush(cout);
}

TEST(SloppyCRCMap, basic) {
  SloppyCRCMap scm(4);

  bufferlist a, b;
  a.append("The quick brown fox jumped over a fence whose color I forget.");
  b.append("asdf");

  scm.write(0, a.length(), a);
  if (0)
    dump(scm);
  ASSERT_EQ(0, scm.read(0, a.length(), a, &cout));

  scm.write(12, b.length(), b);
  if (0)
    dump(scm);

  ASSERT_EQ(0, scm.read(12, b.length(), b, &cout));
  ASSERT_EQ(1, scm.read(0, a.length(), a, &cout));
}

TEST(SloppyCRCMap, truncate) {
  SloppyCRCMap scm(4);

  bufferlist a, b;
  a.append("asdf");
  b.append("qwer");

  scm.write(0, a.length(), a);
  scm.write(4, a.length(), a);
  ASSERT_EQ(0, scm.read(4, 4, a, &cout));
  ASSERT_EQ(1, scm.read(4, 4, b, &cout));
  scm.truncate(4);
  ASSERT_EQ(0, scm.read(4, 4, b, &cout));
}

TEST(SloppyCRCMap, zero) {
  SloppyCRCMap scm(4);

  bufferlist a, b;
  a.append("asdf");
  b.append("qwer");

  scm.write(0, a.length(), a);
  scm.write(4, a.length(), a);
  ASSERT_EQ(0, scm.read(4, 4, a, &cout));
  ASSERT_EQ(1, scm.read(4, 4, b, &cout));
  scm.zero(4, 4);
  ASSERT_EQ(1, scm.read(4, 4, a, &cout));
  ASSERT_EQ(1, scm.read(4, 4, b, &cout));

  bufferptr bp(4);
  bp.zero();
  bufferlist c;
  c.append(bp);
  ASSERT_EQ(0, scm.read(0, 4, a, &cout));
  ASSERT_EQ(0, scm.read(4, 4, c, &cout));
  scm.zero(0, 15);
  ASSERT_EQ(1, scm.read(0, 4, a, &cout));
  ASSERT_EQ(0, scm.read(0, 4, c, &cout));
}

TEST(SloppyCRCMap, clone_range) {
  SloppyCRCMap src(4);
  SloppyCRCMap dst(4);

  bufferlist a, b;
  a.append("asdfghjkl");
  b.append("qwertyui");

  src.write(0, a.length(), a);
  src.write(8, a.length(), a);
  src.write(16, a.length(), a);

  dst.write(0, b.length(), b);
  dst.clone_range(0, 8, 0, src);
  ASSERT_EQ(2, dst.read(0, 8, b, &cout));
  ASSERT_EQ(0, dst.read(8, 8, b, &cout));

  dst.write(16, b.length(), b);
  ASSERT_EQ(2, dst.read(16, 8, a, &cout));
  dst.clone_range(16, 8, 16, src);
  ASSERT_EQ(0, dst.read(16, 8, a, &cout));

  dst.write(16, b.length(), b);
  ASSERT_EQ(1, dst.read(16, 4, a, &cout));
  dst.clone_range(16, 8, 2, src);
  ASSERT_EQ(0, dst.read(16, 4, a, &cout));

  dst.write(0, b.length(), b);
  dst.write(8, b.length(), b);
  ASSERT_EQ(2, dst.read(0, 8, a, &cout));
  ASSERT_EQ(2, dst.read(8, 8, a, &cout));
  dst.clone_range(2, 8, 0, src);
  ASSERT_EQ(0, dst.read(0, 8, a, &cout));
  ASSERT_EQ(0, dst.read(8, 4, a, &cout));
}
