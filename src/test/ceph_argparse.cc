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
  explicit VectorContainer(const char** arr_) {
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
  bool baz_found = false;
  std::string found_baz = "";
  VectorContainer foo(FOO);
  ostringstream err;
  for (std::vector<const char*>::iterator i = foo.arr.begin();
       i != foo.arr.end(); )
  {
      if (ceph_argparse_flag(foo.arr, i, "--foo", (char*)NULL)) {
	found_foo = true;
      }
      else if (ceph_argparse_witharg(foo.arr, i, &found_bar, "--bar", (char*)NULL)) {
      }
      else if (ceph_argparse_witharg(foo.arr, i, &found_baz, err, "--baz", (char*)NULL)) {
	ASSERT_NE(string(""), err.str());
	baz_found = true;
      }
      else
	++i;
  }
  ASSERT_EQ(found_foo, true);
  ASSERT_EQ(found_bar, "");
  ASSERT_EQ(baz_found, true);
  ASSERT_EQ(found_baz, "");

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

TEST(CephArgParse, DoubleDash) {
  const char *ARGS[] = { "./myprog", "--foo", "5", "--", "--bar", "6", NULL };

  int foo = -1, bar = -1;
  VectorContainer args(ARGS);
  for (std::vector<const char*>::iterator i = args.arr.begin();
       i != args.arr.end(); )
  {
    std::string myarg;
    if (ceph_argparse_double_dash(args.arr, i)) {
      break;
    }
    else if (ceph_argparse_witharg(args.arr, i, &myarg, "--foo", (char*)NULL)) {
      foo = atoi(myarg.c_str());
    }
    else if (ceph_argparse_witharg(args.arr, i, &myarg, "--bar", (char*)NULL)) {
      bar = atoi(myarg.c_str());
    }
    else
      ++i;
  }
  ASSERT_EQ(foo, 5);
  ASSERT_EQ(bar, -1);
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

TEST(CephArgParse, WithFloat) {
  const char *BAZSTUFF1[] = { "./myprog", "--foo", "50.5", "--bar", "52", NULL };

  VectorContainer bazstuff1(BAZSTUFF1);
  ostringstream err;
  float foo;
  int bar = -1;
  for (std::vector<const char*>::iterator i = bazstuff1.arr.begin();
       i != bazstuff1.arr.end(); )
  {
    if (ceph_argparse_double_dash(bazstuff1.arr, i)) {
      break;
    } else if (ceph_argparse_witharg(bazstuff1.arr, i, &foo, err, "--foo", (char*)NULL)) {
      ASSERT_EQ(string(""), err.str());
    } else if (ceph_argparse_witharg(bazstuff1.arr, i, &bar, err, "--bar", (char*)NULL)) {
      ASSERT_EQ(string(""), err.str());
    }
    else {
      ++i;
    }
  }
  ASSERT_EQ(foo, 50.5);
  ASSERT_EQ(bar, 52);
}

TEST(CephArgParse, WithInt) {
  const char *BAZSTUFF1[] = { "./myprog", "--foo", "50", "--bar", "52", NULL };
  const char *BAZSTUFF2[] = { "./myprog", "--foo", "--bar", "52", NULL };
  const char *BAZSTUFF3[] = { "./myprog", "--foo", "40", "--", "--bar", "42", NULL };

  // normal test
  VectorContainer bazstuff1(BAZSTUFF1);
  ostringstream err;
  int foo = -1, bar = -1;
  for (std::vector<const char*>::iterator i = bazstuff1.arr.begin();
       i != bazstuff1.arr.end(); )
  {
    if (ceph_argparse_double_dash(bazstuff1.arr, i)) {
      break;
    } else if (ceph_argparse_witharg(bazstuff1.arr, i, &foo, err, "--foo", (char*)NULL)) {
      ASSERT_EQ(string(""), err.str());
    } else if (ceph_argparse_witharg(bazstuff1.arr, i, &bar, err, "--bar", (char*)NULL)) {
      ASSERT_EQ(string(""), err.str());
    }
    else {
      ++i;
    }
  }
  ASSERT_EQ(foo, 50);
  ASSERT_EQ(bar, 52);

  // parse error test
  VectorContainer bazstuff2(BAZSTUFF2);
  ostringstream err2;
  for (std::vector<const char*>::iterator i = bazstuff2.arr.begin();
       i != bazstuff2.arr.end(); )
  {
    if (ceph_argparse_double_dash(bazstuff2.arr, i)) {
      break;
    } else if (ceph_argparse_witharg(bazstuff2.arr, i, &foo, err2, "--foo", (char*)NULL)) {
      ASSERT_NE(string(""), err2.str());
    }
    else {
      ++i;
    }
  }

  // double dash test
  VectorContainer bazstuff3(BAZSTUFF3);
  foo = -1, bar = -1;
  for (std::vector<const char*>::iterator i = bazstuff3.arr.begin();
       i != bazstuff3.arr.end(); )
  {
    if (ceph_argparse_double_dash(bazstuff3.arr, i)) {
      break;
    } else if (ceph_argparse_witharg(bazstuff3.arr, i, &foo, err, "--foo", (char*)NULL)) {
      ASSERT_EQ(string(""), err.str());
    } else if (ceph_argparse_witharg(bazstuff3.arr, i, &bar, err, "--bar", (char*)NULL)) {
      ASSERT_EQ(string(""), err.str());
    }
    else {
      ++i;
    }
  }
  ASSERT_EQ(foo, 40);
  ASSERT_EQ(bar, -1);
}

TEST(CephArgParse, env_to_vec) {
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    unsetenv("WHATEVER");
    env_to_vec(args);
    EXPECT_EQ(0u, args.size());
    env_to_vec(args, "WHATEVER");
    EXPECT_EQ(0u, args.size());
    args.push_back("a");
    setenv("CEPH_ARGS", "b c", 0);
    env_to_vec(args);
    EXPECT_EQ(3u, args.size());
    EXPECT_EQ(string("b"), args[1]);
    EXPECT_EQ(string("c"), args[2]);
    setenv("WHATEVER", "d e", 0);
    env_to_vec(args, "WHATEVER");
    EXPECT_EQ(5u, args.size());
    EXPECT_EQ(string("d"), args[3]);
    EXPECT_EQ(string("e"), args[4]);
  }
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    args.push_back("a");
    args.push_back("--");
    args.push_back("c");
    setenv("CEPH_ARGS", "b -- d", 0);
    env_to_vec(args);
    EXPECT_EQ(5u, args.size());
    EXPECT_EQ(string("a"), args[0]);
    EXPECT_EQ(string("b"), args[1]);
    EXPECT_EQ(string("--"), args[2]);
    EXPECT_EQ(string("c"), args[3]);
    EXPECT_EQ(string("d"), args[4]);
  }
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    args.push_back("a");
    args.push_back("--");
    setenv("CEPH_ARGS", "b -- c", 0);
    env_to_vec(args);
    EXPECT_EQ(4u, args.size());
    EXPECT_EQ(string("a"), args[0]);
    EXPECT_EQ(string("b"), args[1]);
    EXPECT_EQ(string("--"), args[2]);
    EXPECT_EQ(string("c"), args[3]);
  }
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    args.push_back("--");
    args.push_back("c");
    setenv("CEPH_ARGS", "b -- d", 0);
    env_to_vec(args);
    EXPECT_EQ(4u, args.size());
    EXPECT_EQ(string("b"), args[0]);
    EXPECT_EQ(string("--"), args[1]);
    EXPECT_EQ(string("c"), args[2]);
    EXPECT_EQ(string("d"), args[3]);
  }
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    args.push_back("b");
    setenv("CEPH_ARGS", "c -- d", 0);
    env_to_vec(args);
    EXPECT_EQ(4u, args.size());
    EXPECT_EQ(string("b"), args[0]);
    EXPECT_EQ(string("c"), args[1]);
    EXPECT_EQ(string("--"), args[2]);
    EXPECT_EQ(string("d"), args[3]);
  }
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    args.push_back("a");
    args.push_back("--");
    args.push_back("c");
    setenv("CEPH_ARGS", "-- d", 0);
    env_to_vec(args);
    EXPECT_EQ(4u, args.size());
    EXPECT_EQ(string("a"), args[0]);
    EXPECT_EQ(string("--"), args[1]);
    EXPECT_EQ(string("c"), args[2]);
    EXPECT_EQ(string("d"), args[3]);
  }
  {
    std::vector<const char*> args;
    unsetenv("CEPH_ARGS");
    args.push_back("a");
    args.push_back("--");
    args.push_back("c");
    setenv("CEPH_ARGS", "d", 0);
    env_to_vec(args);
    EXPECT_EQ(4u, args.size());
    EXPECT_EQ(string("a"), args[0]);
    EXPECT_EQ(string("d"), args[1]);
    EXPECT_EQ(string("--"), args[2]);
    EXPECT_EQ(string("c"), args[3]);
  }
}
/*
 * Local Variables:
 * compile-command: "cd .. ; make unittest_ceph_argparse && ./unittest_ceph_argparse"
 * End:
 */
