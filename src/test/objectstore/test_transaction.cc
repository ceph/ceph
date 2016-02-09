// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "os/ObjectStore.h"
#include <gtest/gtest.h>

TEST(Transaction, MoveConstruct)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  // move-construct in b
  auto b = std::move(a);
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, MoveAssign)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  b = std::move(a); // move-assign to b
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, CopyConstruct)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = a; // copy-construct in b
  ASSERT_FALSE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, CopyAssign)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  b = a; // copy-assign to b
  ASSERT_FALSE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, Swap)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  std::swap(a, b); // swap a and b
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}
