// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "common/filepath.h"

#include <gtest/gtest.h>

#include <string_view>

using namespace std::literals::string_view_literals;

TEST(filepath, constructor) {
  filepath fp;
  ASSERT_EQ(fp.get_path(), ""sv);
  ASSERT_EQ(fp.get_ino(), 0);
  ASSERT_TRUE(fp.empty());
  ASSERT_FALSE(fp.absolute());
  ASSERT_TRUE(fp.pure_relative());
  ASSERT_FALSE(fp.ino_relative());

  filepath fp2("/a/b/c");
  ASSERT_EQ(fp2.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp2.get_ino(), 1);
  ASSERT_FALSE(fp2.empty());
  ASSERT_TRUE(fp2.absolute());
  ASSERT_FALSE(fp2.pure_relative());
  ASSERT_TRUE(fp2.ino_relative());

  filepath fp3("a/b/c");
  ASSERT_EQ(fp3.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp3.get_ino(), 0);
  ASSERT_FALSE(fp3.empty());
  ASSERT_FALSE(fp3.absolute());
  ASSERT_TRUE(fp3.pure_relative());
  ASSERT_FALSE(fp3.ino_relative());

  filepath fp4("a/b/c", 123);
  ASSERT_EQ(fp4.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp4.get_ino(), 123);
  ASSERT_FALSE(fp4.empty());
  ASSERT_FALSE(fp4.absolute());
  ASSERT_FALSE(fp4.pure_relative());
  ASSERT_TRUE(fp4.ino_relative());
}

TEST(filepath, set_path) {
  filepath fp;
  fp.set_path("/a/b/c");
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp.get_ino(), 1);

  fp.set_path("a/b/c");
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp.get_ino(), 0);

  fp.set_path("a/b/c", 123);
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp.get_ino(), 123);

  fp.set_path("//a/b/c", 123);
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp.get_ino(), 1);

  fp.set_path("///a/b/c", 123);
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp.get_ino(), 1);

  fp.set_path("///////////////////a/b/c", 123);
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(fp.get_ino(), 1);
}

TEST(filepath, push_pop_dentry) {
  filepath fp;
  fp.push_dentry("a");
  ASSERT_EQ(fp.get_path(), "a"sv);
  fp.push_dentry("b");
  ASSERT_EQ(fp.get_path(), "a/b"sv);
  fp.push_dentry("c");
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);

  fp.pop_dentry();
  ASSERT_EQ(fp.get_path(), "a/b"sv);
  fp.pop_dentry();
  ASSERT_EQ(fp.get_path(), "a"sv);
  fp.pop_dentry();
  ASSERT_EQ(fp.get_path(), ""sv);
}

TEST(filepath, push_front_dentry) {
  filepath fp;
  fp.push_front_dentry("c");
  ASSERT_EQ(fp.get_path(), "c"sv);
  fp.push_front_dentry("b");
  ASSERT_EQ(fp.get_path(), "b/c"sv);
  fp.push_front_dentry("a");
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
}

TEST(filepath, append) {
  filepath fp1("a/b");
  filepath fp2("c/d");
  fp1.append(fp2);
  ASSERT_EQ(fp1.get_path(), "a/b/c/d"sv);
}

TEST(filepath, accessors) {
  filepath fp("/a/b/c");
  ASSERT_EQ(fp.get_ino(), 1);
  ASSERT_EQ(fp.get_path(), "a/b/c"sv);
  ASSERT_EQ(strcmp(fp.c_str(), "a/b/c"), 0);
  ASSERT_EQ(fp.length(), 5);
  ASSERT_EQ(fp.depth(), 3);
  ASSERT_EQ(fp[0], "a");
  ASSERT_EQ(fp[1], "b");
  ASSERT_EQ(fp[2], "c");
  ASSERT_EQ(fp.last_dentry(), "c");
}

TEST(filepath, iterators) {
  filepath fp("a/b/c");
  auto it = fp.begin();
  ASSERT_EQ(*it, "a");
  ++it;
  ASSERT_EQ(*it, "b");
  ++it;
  ASSERT_EQ(*it, "c");
  ++it;
  ASSERT_EQ(it, fp.end());
}

TEST(filepath, prefixpath) {
  filepath fp("a/b/c/d");
  filepath prefix = fp.prefixpath(2);
  ASSERT_EQ(prefix.get_path(), "a/b"sv);
}

TEST(filepath, postfixpath) {
  filepath fp("a/b/c/d");
  filepath postfix = fp.postfixpath(2);
  ASSERT_EQ(postfix.get_path(), "c/d"sv);
}

TEST(filepath, is_last_dot_or_dotdot) {
  filepath fp1("a/b/.");
  ASSERT_TRUE(fp1.is_last_dot_or_dotdot());

  filepath fp2("a/b/..");
  ASSERT_TRUE(fp2.is_last_dot_or_dotdot());

  filepath fp3("a/b/c");
  ASSERT_FALSE(fp3.is_last_dot_or_dotdot());
}

TEST(filepath, is_last_snap) {
  filepath fp1("//a/b");
  ASSERT_FALSE(fp1.is_last_snap());

  filepath fp2(32);
  fp2.push_dentry("");
  ASSERT_TRUE(fp2.is_last_snap());
  fp2.push_dentry("a");
  ASSERT_TRUE(fp2.is_last_snap());
  ASSERT_EQ(fp2.get_path(), "/a"sv);

  fp2.clear();
  ASSERT_FALSE(fp2.is_last_snap());
}

TEST(filepath, encode_decode) {
  {
    filepath fp1("a/b/c", 123);
    ceph::buffer::list bl;
    fp1.encode(bl);

    filepath fp2;
    auto iter = bl.cbegin();
    fp2.decode(iter);

    ASSERT_EQ(fp1.get_path(), fp2.get_path());
    ASSERT_EQ(fp1.get_ino(), fp2.get_ino());
  }

  // Test snappath encode/decode
  {
    filepath fp_snap;
    fp_snap.push_dentry("");
    fp_snap.append(filepath("a/b"));
    ASSERT_EQ(fp_snap.get_ino(), 0);
    ASSERT_TRUE(fp_snap.is_last_snap());

    ceph::buffer::list bl_snap;
    fp_snap.encode(bl_snap);

    filepath fp_snap_decoded;
    auto iter_snap = bl_snap.cbegin();
    fp_snap_decoded.decode(iter_snap);

    ASSERT_EQ(fp_snap.get_path(), fp_snap_decoded.get_path());
    ASSERT_EQ(fp_snap.get_ino(), fp_snap_decoded.get_ino());
    ASSERT_TRUE(fp_snap_decoded.is_last_snap());
  }
}

TEST(filepath, terminating_slash) {
  // A terminating slash is semantically significant and treated as "/.".
  {
    filepath fp("a/b/");
    ASSERT_EQ(fp.get_path(), "a/b/."sv);
  }

  // A single slash is just the root path.
  {
    filepath fp("/");
    ASSERT_EQ(fp.get_path(), ""sv);
    ASSERT_TRUE(fp.absolute());
    ASSERT_FALSE(fp.is_last_snap());
    ASSERT_EQ(fp.get_ino(), 1);
  }

  // Multiple leading slashes is still a root path.
  {
    filepath fp("//");
    ASSERT_EQ(fp.get_path(), ""sv);
    ASSERT_TRUE(fp.absolute());
    ASSERT_FALSE(fp.is_last_snap());
    ASSERT_EQ(fp.get_ino(), 1);
  }

  // Duplicate slashes in the middle are chomped, trailing slash is still significant.
  {
    filepath fp("a//b/");
    ASSERT_EQ(fp.get_path(), "a/b/."sv);
    ASSERT_FALSE(fp.absolute());
    ASSERT_FALSE(fp.is_last_snap());
  }
}

TEST(filepath, snappath_components) {
  filepath fp;
  fp.push_dentry("");
  fp.append(filepath("a/b"));
  ASSERT_TRUE(fp.is_last_snap());
  ASSERT_EQ(fp.depth(), 3);

  // For a snappath, the first component is an empty dentry.
  ASSERT_EQ(fp[0], ""sv);
  ASSERT_EQ(fp[1], "a"sv);
  ASSERT_EQ(fp[2], "b"sv);

  auto it = fp.begin();
  ASSERT_EQ(*it, ""sv);
  ++it;
  ASSERT_EQ(*it, "a"sv);
  ++it;
  ASSERT_EQ(*it, "b"sv);
  ++it;
  ASSERT_EQ(it, fp.end());
}

TEST(filepath, snappath_manual_construction_root) {
  // A snappath can be built by pushing an empty dentry first.
  filepath fp(1);
  fp.push_dentry("");
  fp.push_dentry("a");
  fp.push_dentry("b");

  // get_path() will trigger _rebuild_path(), which detects the empty
  // initial dentry and sets the snappath flag. The path string is
  // reconstructed with leading slashes.
  ASSERT_EQ(fp.get_path(), "/a/b"sv);
  ASSERT_TRUE(fp.is_last_snap());
  ASSERT_EQ(fp.get_ino(), 1);
  ASSERT_EQ(fp.depth(), 3);
}

TEST(filepath, snappath_manual_construction_relative) {
  // A snappath can be built by pushing an empty dentry first.
  filepath fp(0);
  fp.push_dentry("");
  fp.push_dentry("a");
  fp.push_dentry("b");

  // get_path() will trigger _rebuild_path(), which detects the empty
  // initial dentry and sets the snappath flag. The path string is
  // reconstructed with leading slashes.
  ASSERT_EQ(fp.get_path(), "/a/b"sv);
  ASSERT_TRUE(fp.is_last_snap());
  ASSERT_EQ(fp.get_ino(), 0);
  ASSERT_EQ(fp.depth(), 3);
}
