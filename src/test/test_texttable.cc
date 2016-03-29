// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/TextTable.h"
#include <iostream>
#include "gtest/gtest.h"

TEST(TextTable, Alignment) {
  TextTable t;

  // test alignment
  // 3 5-character columns
  t.define_column("HEAD1", TextTable::LEFT, TextTable::LEFT);
  t.define_column("HEAD2", TextTable::LEFT, TextTable::CENTER);
  t.define_column("HEAD3", TextTable::LEFT, TextTable::RIGHT);

  t << "1" << 2 << 3 << TextTable::endrow;
  std::ostringstream oss;
  oss << t;
  ASSERT_STREQ("HEAD1 HEAD2 HEAD3 \n1       2       3 \n", oss.str().c_str());
}

TEST(TextTable, WidenAndClearShrink) {
  TextTable t;

  t.define_column("1", TextTable::LEFT, TextTable::LEFT);

  // default column size is 1, widen to 5
  t << "wider";

  // validate wide output
  std::ostringstream oss;
  oss << t;
  ASSERT_STREQ("1     \nwider \n", oss.str().c_str());
  oss.str("");

  // reset, validate single-char width output
  t.clear();
  t << "s";
  oss << t;
  ASSERT_STREQ("1 \ns \n", oss.str().c_str());
}

TEST(TextTable, Indent) {
  TextTable t;

  t.define_column("1", TextTable::LEFT, TextTable::LEFT);
  t.set_indent(10);
  t << "s";
  std::ostringstream oss;
  oss << t;
  ASSERT_STREQ("          1 \n          s \n", oss.str().c_str());
}


TEST(TextTable, TooManyItems) {
  TextTable t;

  t.define_column("1", TextTable::LEFT, TextTable::LEFT);
  t.define_column("2", TextTable::LEFT, TextTable::LEFT);
  t.define_column("3", TextTable::LEFT, TextTable::LEFT);

  // expect assertion failure on this, which throws FailedAssertion
  ASSERT_DEATH((t << "1" << "2" << "3" << "4" << TextTable::endrow), "");
}
