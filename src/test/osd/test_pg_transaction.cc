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
#include "osd/PGTransaction.h"

TEST(pgtransaction, simple)
{
  hobject_t h;
  PGTransaction t;
  ASSERT_TRUE(t.empty());
  t.nop(h);
  ASSERT_FALSE(t.empty());
  unsigned num = 0;
  t.safe_create_traverse(
    [&](const pair<const hobject_t, PGTransaction::ObjectOperation> &p) {
      ASSERT_EQ(p.first, h);
      using T = PGTransaction::ObjectOperation::Init;
      ASSERT_TRUE(boost::get<T::None>(&p.second.init_type));
      ++num;
    });
  ASSERT_EQ(num, 1u);
}

TEST(pgtransaction, clone_safe_create_traverse)
{
  hobject_t h, h2;
  h2.snap = 1;
  PGTransaction t;
  ASSERT_TRUE(t.empty());
  t.nop(h2);
  ASSERT_FALSE(t.empty());
  t.clone(h, h2);
  unsigned num = 0;
  t.safe_create_traverse(
    [&](const pair<const hobject_t, PGTransaction::ObjectOperation> &p) {
      using T = PGTransaction::ObjectOperation::Init;
      if (num == 0) {
	ASSERT_EQ(p.first, h);
	ASSERT_TRUE(boost::get<T::Clone>(&p.second.init_type));
	ASSERT_EQ(
	  boost::get<T::Clone>(&p.second.init_type)->source,
	  h2);
      } else if (num == 1) {
	ASSERT_EQ(p.first, h2);
	ASSERT_TRUE(boost::get<T::None>(&p.second.init_type));
      } else {
	ASSERT_LT(num, 2u);
      }
      ++num;
    });
}

TEST(pgtransaction, clone_safe_create_traverse2)
{
  hobject_t h, h2, h3;
  h.snap = 10;
  h2.snap = 5;
  h3.snap = 3;
  PGTransaction t;
  ASSERT_TRUE(t.empty());
  t.nop(h3);
  ASSERT_FALSE(t.empty());
  t.clone(h, h2);
  t.remove(h2);
  t.clone(h2, h3);
  unsigned num = 0;
  t.safe_create_traverse(
    [&](const pair<const hobject_t, PGTransaction::ObjectOperation> &p) {
      using T = PGTransaction::ObjectOperation::Init;
      if (num == 0) {
	ASSERT_EQ(p.first, h);
	ASSERT_TRUE(boost::get<T::Clone>(&p.second.init_type));
	ASSERT_EQ(
	  boost::get<T::Clone>(&p.second.init_type)->source,
	  h2);
      } else if (num == 1) {
	ASSERT_EQ(p.first, h2);
	ASSERT_TRUE(boost::get<T::Clone>(&p.second.init_type));
	ASSERT_EQ(
	  boost::get<T::Clone>(&p.second.init_type)->source,
	  h3);
      } else if (num == 2) {
	ASSERT_EQ(p.first, h3);
	ASSERT_TRUE(boost::get<T::None>(&p.second.init_type));
      } else {
	ASSERT_LT(num, 3u);
      }
      ++num;
    });
}

TEST(pgtransaction, clone_safe_create_traverse3)
{
  hobject_t h, h2, h3;
  h.snap = 10;
  h2.snap = 5;
  h3.snap = 3;
  PGTransaction t;
  t.remove(h);
  t.remove(h2);
  t.clone(h2, h3);
  unsigned num = 0;
  t.safe_create_traverse(
    [&](const pair<const hobject_t, PGTransaction::ObjectOperation> &p) {
      using T = PGTransaction::ObjectOperation::Init;
      if (p.first == h) {
	ASSERT_TRUE(p.second.is_delete());
      } else if (p.first == h2) {
	ASSERT_TRUE(boost::get<T::Clone>(&p.second.init_type));
	ASSERT_EQ(
	  boost::get<T::Clone>(&p.second.init_type)->source,
	  h3);
      }
      ASSERT_LT(num, 2u);
      ++num;
    });
}
