// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * Author: Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/convenience.h" // include first: tests that header is standalone

#include <string>
#include <boost/optional.hpp>
#include <gtest/gtest.h>

// A just god would not allow the C++ standard to make taking the
// address of member functions in the standard library undefined behavior.
static std::string::size_type l(const std::string& s) {
  return s.size();
}

TEST(Convenience, MaybeDo)
{
  boost::optional<std::string> s("qwerty");
  boost::optional<std::string> t;
  auto r = ceph::maybe_do(s, l);
  EXPECT_TRUE(r);
  EXPECT_EQ(*r, s->size());

  EXPECT_FALSE(ceph::maybe_do(t, l));
}

TEST(Convenience, MaybeDoOr)
{
  const boost::optional<std::string> s("qwerty");
  const boost::optional<std::string> t;
  auto r = ceph::maybe_do_or(s, l, 0);
  EXPECT_EQ(r, s->size());

  EXPECT_EQ(ceph::maybe_do_or(t, l, 0), 0);
}

TEST(Convenience, StdMaybeDo)
{
  std::optional<std::string> s("qwerty");
  std::optional<std::string> t;
  auto r = ceph::maybe_do(s, l);
  EXPECT_TRUE(r);
  EXPECT_EQ(*r, s->size());

  EXPECT_FALSE(ceph::maybe_do(t, l));
}

TEST(Convenience, StdMaybeDoOr)
{
  const std::optional<std::string> s("qwerty");
  const std::optional<std::string> t;
  auto r = ceph::maybe_do_or(s, l, 0);
  EXPECT_EQ(r, s->size());

  EXPECT_EQ(ceph::maybe_do_or(t, l, 0), 0);
}
