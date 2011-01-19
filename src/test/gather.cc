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
#include "config.h"
#include "include/Context.h"
#include "gtest/gtest.h"

class C_Checker : public Context {
public:
  bool *finish_called;
  int *result;
  C_Checker(bool* _finish_called, int *r) :
    finish_called(_finish_called), result(r) {}
  void finish(int r) { *finish_called = true; *result = r; }
};

TEST(ContextGather, Constructor) {
  C_Gather *gather = new C_Gather();
  EXPECT_TRUE(gather->empty());
  EXPECT_EQ(0, gather->get_num());
  EXPECT_EQ(0, gather->get_num_remaining());
  delete gather;
}

TEST(ContextGather, OneSub) {
  C_Gather *gather = new C_Gather();
  Context *sub = gather->new_sub();
  EXPECT_EQ(1, gather->get_num());
  EXPECT_EQ(1, gather->get_num_remaining());
  EXPECT_FALSE(gather->empty());

  bool finish_called = false;
  int result = 0;
  C_Checker *checker = new C_Checker(&finish_called, &result);
  gather->set_finisher(checker);
  sub->finish(0);
  delete sub;
  EXPECT_TRUE(finish_called);
  EXPECT_EQ(0, result);
}

TEST(ContextGather, ManySubs) {
  C_Gather *gather = new C_Gather();
  int sub_count = 8;
  bool finish_called = false;
  int result = 0;
  C_Checker *checker = new C_Checker(&finish_called, &result);
  gather->set_finisher(checker);
  Context* subs[sub_count];
  //create subs and test
  for (int i = 0; i < sub_count; ++i) {
    subs[i] = gather->new_sub();
    EXPECT_EQ(i+1, gather->get_num());
    EXPECT_EQ(i+1, gather->get_num_remaining());
  }

  //finish all except one sub
  for (int j = 0; j < sub_count - 1; ++j) {
    subs[j]->finish(0);
    delete subs[j];
    EXPECT_EQ(sub_count - j - 1, gather->get_num_remaining());
    EXPECT_EQ(sub_count, gather->get_num());
    EXPECT_FALSE(finish_called);
    EXPECT_FALSE(gather->empty());
  }

  //finish last one and check asserts
  subs[sub_count-1]->finish(0);
  delete subs[sub_count-1];
  EXPECT_TRUE(finish_called);
}

TEST(ContextGather, AlternatingSubCreateFinish) {
  C_Gather *gather = new C_Gather();
  int sub_count = 8;
  bool finish_called = false;
  int result = 0;
  C_Checker *checker = new C_Checker(&finish_called, &result);
  gather->set_finisher(checker);
  Context* subs[sub_count];

  //create half the subs
  for (int i = 0; i < sub_count / 2; ++i) {
    subs[i] = gather->new_sub();
    EXPECT_EQ(i + 1, gather->get_num());
    EXPECT_EQ(i + 1, gather->get_num_remaining());
  }

  //alternate finishing first half of subs and creating last half of subs
  for (int j = 0; j < sub_count / 2; ++j) {
    subs[j]->finish(0);
    delete subs[j];
    EXPECT_EQ(sub_count / 2 - 1, gather->get_num_remaining());
    
    subs[sub_count / 2 + j] = gather->new_sub();
    EXPECT_EQ(sub_count / 2 + 1 + j, gather->get_num());
    EXPECT_EQ(sub_count / 2, gather->get_num_remaining());
    EXPECT_FALSE(gather->empty());
  }

  //finish last half of subs
  for (int k = sub_count / 2; k < sub_count; ++k) {
    EXPECT_EQ(sub_count - k, gather->get_num_remaining());
    EXPECT_EQ(sub_count, gather->get_num());
    EXPECT_FALSE(gather->empty());
    subs[k]->finish(0);
    delete subs[k];
  }

  EXPECT_TRUE(finish_called);
}
