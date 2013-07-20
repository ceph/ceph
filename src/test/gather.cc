// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Greg Farnum <gregory.farnum@dreamhost.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#include "include/Context.h"
#include "test/unit.h"

class C_Checker : public Context {
public:
  bool *finish_called;
  int *result;
  C_Checker(bool* _finish_called, int *r) :
    finish_called(_finish_called), result(r) {}
  void finish(int r) { *finish_called = true; *result = r; }
};

TEST(ContextGather, Constructor) {
  C_GatherBuilder gather(g_ceph_context);
  EXPECT_FALSE(gather.has_subs());
  EXPECT_TRUE(gather.get() == NULL);
}

TEST(ContextGather, OneSub) {
  C_GatherBuilder gather(g_ceph_context);
  Context *sub = gather.new_sub();
  EXPECT_EQ(1, gather.num_subs_created());
  EXPECT_EQ(1, gather.num_subs_remaining());

  bool finish_called = false;
  int result = 0;
  C_Checker *checker = new C_Checker(&finish_called, &result);
  gather.set_finisher(checker);
  gather.activate();
  sub->complete(0);
  EXPECT_TRUE(finish_called);
  EXPECT_EQ(0, result);
}

TEST(ContextGather, ManySubs) {
  bool finish_called = false;
  int result = 0;
  C_GatherBuilder gather(g_ceph_context, new C_Checker(&finish_called, &result));
  int sub_count = 8;
  Context* subs[sub_count];
  //create subs and test
  for (int i = 0; i < sub_count; ++i) {
    subs[i] = gather.new_sub();
    EXPECT_EQ(i+1, gather.num_subs_created());
    EXPECT_EQ(i+1, gather.num_subs_remaining());
  }
  EXPECT_TRUE(gather.has_subs());
  gather.activate();

  //finish all except one sub
  for (int j = 0; j < sub_count - 1; ++j) {
    subs[j]->complete(0);
    EXPECT_FALSE(finish_called);
  }

  //finish last one and check asserts
  subs[sub_count-1]->complete(0);
  EXPECT_TRUE(finish_called);
}

TEST(ContextGather, AlternatingSubCreateFinish) {
  C_GatherBuilder gather(g_ceph_context);
  int sub_count = 8;
  bool finish_called = false;
  int result = 0;
  C_Checker *checker = new C_Checker(&finish_called, &result);
  gather.set_finisher(checker);
  Context* subs[sub_count];

  //create half the subs
  for (int i = 0; i < sub_count / 2; ++i) {
    subs[i] = gather.new_sub();
    EXPECT_EQ(i + 1, gather.num_subs_created());
    EXPECT_EQ(i + 1, gather.num_subs_remaining());
  }

  //alternate finishing first half of subs and creating last half of subs
  for (int j = 0; j < sub_count / 2; ++j) {
    subs[j]->complete(0);
    subs[sub_count / 2 + j] = gather.new_sub();
  }
  gather.activate();

  //finish last half of subs
  for (int k = sub_count / 2; k < sub_count; ++k) {
    subs[k]->complete(0);
  }

  EXPECT_TRUE(finish_called);
}
