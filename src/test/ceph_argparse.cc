// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_argparse.h"

#include "gtest/gtest.h"
#include <vector>

/* Holds a std::vector with C-strings.
 * Will free() them properly in the destructor.
 *
 * Note: the ceph_argparse functions modify the vector, removing elements as
 * they find them.  So we keep a parallel vector, orig, to make sure that we
 * never forget to delete a string.
 */
class VectorContainer
{
public:
  VectorContainer(const char** arr_) {
    for (const char **a = arr_; *a; ++a) {
      const char *str = (const char*)strdup(*a);
      arr.push_back(str);
      orig.push_back(str);
    }
  }
  ~VectorContainer() {
    for (std::vector<const char*>::iterator i = orig.begin();
	   i != orig.end(); ++i)
    {
	 free((void*)*i);
    }
  }
  void refresh() {
    arr.assign(orig.begin(), orig.end());
  }
  std::vector < const char* > arr;

private:
  std::vector < const char* > orig;
};

TEST(CephArgParse, SimpleArgParse) {
  const char *BAR5[] = { "./myprog", "--bar", "5", NULL };
  const char *FOO[] = { "./myprog", "--foo", "--baz", NULL };
  const char *NONE[] = { "./myprog", NULL };

  bool found_foo = false;
  std::string found_bar;
  VectorContainer bar5(BAR5);
  for (std::vector<const char*>::iterator i = bar5.arr.begin();
       i != bar5.arr.end(); )
  {
      if (ceph_argparse_flag(bar5.arr, i, "--foo", (char*)NULL)) {
	found_foo = true;
      }
      else if (ceph_argparse_witharg(bar5.arr, i, &found_bar, "--bar", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_foo, false);
  ASSERT_EQ(found_bar, "5");

  found_foo = false;
  found_bar = "";
  VectorContainer foo(FOO);
  for (std::vector<const char*>::iterator i = foo.arr.begin();
       i != foo.arr.end(); )
  {
      if (ceph_argparse_flag(foo.arr, i, "--foo", (char*)NULL)) {
	found_foo = true;
      }
      else if (ceph_argparse_witharg(foo.arr, i, &found_bar, "--bar", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_foo, true);
  ASSERT_EQ(found_bar, "");

  found_foo = false;
  found_bar = "";
  VectorContainer none(NONE);
  for (std::vector<const char*>::iterator i = none.arr.begin();
       i != none.arr.end(); )
  {
      if (ceph_argparse_flag(none.arr, i, "--foo", (char*)NULL)) {
	found_foo = true;
      }
      else if (ceph_argparse_witharg(none.arr, i, &found_bar, "--bar", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_foo, false);
  ASSERT_EQ(found_bar, "");
}


TEST(CephArgParse, WithDashesAndUnderscores) {
  const char *BAZSTUFF1[] = { "./myprog", "--goo", "--baz-stuff", "50", "--end", NULL };
  const char *BAZSTUFF2[] = { "./myprog", "--goo2", "--baz_stuff", "50", NULL };
  const char *BAZSTUFF3[] = { "./myprog", "--goo2", "--baz-stuff=50", "50", NULL };
  const char *BAZSTUFF4[] = { "./myprog", "--goo2", "--baz_stuff=50", "50", NULL };
  const char *NONE1[] = { "./myprog", NULL };
  const char *NONE2[] = { "./myprog", "--goo2", "--baz_stuff2", "50", NULL };
  const char *NONE3[] = { "./myprog", "--goo2", "__baz_stuff", "50", NULL };

  // as flag
  std::string found_baz;
  VectorContainer bazstuff1(BAZSTUFF1);
  for (std::vector<const char*>::iterator i = bazstuff1.arr.begin();
       i != bazstuff1.arr.end(); )
  {
      if (ceph_argparse_flag(bazstuff1.arr, i, "--baz-stuff", (char*)NULL)) {
	found_baz = "true";
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "true");

  // as flag
  found_baz = "";
  VectorContainer bazstuff2(BAZSTUFF2);
  for (std::vector<const char*>::iterator i = bazstuff2.arr.begin();
       i != bazstuff2.arr.end(); )
  {
      if (ceph_argparse_flag(bazstuff2.arr, i, "--baz-stuff", (char*)NULL)) {
	found_baz = "true";
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "true");

  // with argument
  found_baz = "";
  bazstuff1.refresh();
  for (std::vector<const char*>::iterator i = bazstuff1.arr.begin();
       i != bazstuff1.arr.end(); )
  {
      if (ceph_argparse_witharg(bazstuff1.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "50");

  // with argument
  found_baz = "";
  bazstuff2.refresh();
  for (std::vector<const char*>::iterator i = bazstuff2.arr.begin();
       i != bazstuff2.arr.end(); )
  {
      if (ceph_argparse_witharg(bazstuff2.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "50");

  // with argument
  found_baz = "";
  VectorContainer bazstuff3(BAZSTUFF3);
  for (std::vector<const char*>::iterator i = bazstuff3.arr.begin();
       i != bazstuff3.arr.end(); )
  {
      if (ceph_argparse_witharg(bazstuff3.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "50");

  // with argument
  found_baz = "";
  VectorContainer bazstuff4(BAZSTUFF4);
  for (std::vector<const char*>::iterator i = bazstuff4.arr.begin();
       i != bazstuff4.arr.end(); )
  {
      if (ceph_argparse_witharg(bazstuff4.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "50");

  // not found
  found_baz = "";
  VectorContainer none1(NONE1);
  for (std::vector<const char*>::iterator i = none1.arr.begin();
       i != none1.arr.end(); )
  {
      if (ceph_argparse_flag(none1.arr, i, "--baz-stuff", (char*)NULL)) {
	found_baz = "true";
      }
      else if (ceph_argparse_witharg(none1.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "");

  // not found
  found_baz = "";
  VectorContainer none2(NONE2);
  for (std::vector<const char*>::iterator i = none2.arr.begin();
       i != none2.arr.end(); )
  {
      if (ceph_argparse_flag(none2.arr, i, "--baz-stuff", (char*)NULL)) {
	found_baz = "true";
      }
      else if (ceph_argparse_witharg(none2.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "");

  // not found
  found_baz = "";
  VectorContainer none3(NONE3);
  for (std::vector<const char*>::iterator i = none3.arr.begin();
       i != none3.arr.end(); )
  {
      if (ceph_argparse_flag(none3.arr, i, "--baz-stuff", (char*)NULL)) {
	found_baz = "true";
      }
      else if (ceph_argparse_witharg(none3.arr, i, &found_baz, "--baz-stuff", (char*)NULL)) {
      }
      else
	++i;
  }
  ASSERT_EQ(found_baz, "");
}
