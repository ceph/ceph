// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Open Edge LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mds/ChangeNotifyFormat.h"

using namespace cephfs_notify;

namespace {

// relative_path() with the "outside the root" case made printable, so a
// failure shows the path instead of an optional's raw bytes.
std::string rel(std::string_view root, std::string_view path)
{
  auto r = relative_path(root, path);
  return r ? *r : std::string("<outside>");
}

} // namespace

// The record strings asserted below are the wire contract: one JSON object
// per message, paths relative to the watch root, inotify mask values. A
// consumer (OpenCloud's posixfs watcher is the reference one) dispatches on
// exactly these fields, so the assertions are byte-for-byte.

TEST(ChangeNotifyFormat, json_escape)
{
  std::string out;
  json_escape(out, "plain/path.txt");
  EXPECT_EQ(out, "plain/path.txt");

  out.clear();
  json_escape(out, "a\"b\\c");
  EXPECT_EQ(out, "a\\\"b\\\\c");

  std::string in = "n\nr\rt\t";
  in += '\x01';
  in += "end";
  out.clear();
  json_escape(out, in);
  EXPECT_EQ(out, "n\\nr\\rt\\t\\u0001end");

  // UTF-8 bytes pass through unchanged (paths are emitted as they are)
  out.clear();
  json_escape(out, "\xc3\xa9/file");
  EXPECT_EQ(out, "\xc3\xa9/file");
}

TEST(ChangeNotifyFormat, event_record)
{
  // the masks and paths of the six consumed operations, as the consumer
  // sees them
  EXPECT_EQ(event_record(NOTIFY_CREATE, "projects/alpha/report.txt"),
            "{\"mask\": 16, \"path\": \"projects/alpha/report.txt\"}");
  EXPECT_EQ(event_record(NOTIFY_CREATE | NOTIFY_ONLYDIR, "projects/beta"),
            "{\"mask\": 65552, \"path\": \"projects/beta\"}");
  EXPECT_EQ(event_record(NOTIFY_CLOSE_WRITE, "projects/alpha/report.txt"),
            "{\"mask\": 4, \"path\": \"projects/alpha/report.txt\"}");
  EXPECT_EQ(event_record(NOTIFY_DELETE, "projects/alpha/report.txt"),
            "{\"mask\": 32, \"path\": \"projects/alpha/report.txt\"}");
  EXPECT_EQ(event_record(NOTIFY_DELETE | NOTIFY_ONLYDIR, "projects/beta"),
            "{\"mask\": 65568, \"path\": \"projects/beta\"}");

  // an escaping path stays one JSON string
  EXPECT_EQ(event_record(NOTIFY_CREATE, "a\"b"),
            "{\"mask\": 16, \"path\": \"a\\\"b\"}");
}

TEST(ChangeNotifyFormat, move_records)
{
  // both ends inside the root: one message carrying both halves
  EXPECT_EQ(
      move_record(NOTIFY_MOVED_FROM, "projects/alpha/report.txt",
                  NOTIFY_MOVED_TO, "projects/alpha/final.txt"),
      "{\"mask\": 0, \"path\": \"\", \"src_mask\": 512, "
      "\"src_path\": \"projects/alpha/report.txt\", \"dest_mask\": 1024, "
      "\"dest_path\": \"projects/alpha/final.txt\"}");

  // a directory move carries ONLYDIR on both halves
  EXPECT_EQ(move_record(NOTIFY_MOVED_FROM | NOTIFY_ONLYDIR, "projects/beta",
                        NOTIFY_MOVED_TO | NOTIFY_ONLYDIR, "projects/gamma"),
            "{\"mask\": 0, \"path\": \"\", \"src_mask\": 66048, "
            "\"src_path\": \"projects/beta\", \"dest_mask\": 66560, "
            "\"dest_path\": \"projects/gamma\"}");

  // moved in from outside the watch root: the destination half only
  EXPECT_EQ(move_in_record(NOTIFY_MOVED_TO, "projects/alpha/final.txt"),
            "{\"mask\": 0, \"path\": \"\", \"dest_mask\": 1024, "
            "\"dest_path\": \"projects/alpha/final.txt\"}");

  // moved out of the watch root: DELETE on the in-root source, with ONLYDIR
  // preserved for a directory
  EXPECT_EQ(move_out_record(NOTIFY_MOVED_FROM, "projects/alpha/report.txt"),
            "{\"mask\": 32, \"path\": \"projects/alpha/report.txt\"}");
  EXPECT_EQ(move_out_record(NOTIFY_MOVED_FROM | NOTIFY_ONLYDIR, "projects/beta"),
            "{\"mask\": 65568, \"path\": \"projects/beta\"}");
}

TEST(ChangeNotifyFormat, relative_path_fs_root)
{
  // "/" means the filesystem root: every absolute path is inside it
  EXPECT_EQ(rel("/", "/"), "");
  EXPECT_EQ(rel("/", "/projects/alpha"), "projects/alpha");
  EXPECT_EQ(rel("/", "/a"), "a");
}

TEST(ChangeNotifyFormat, relative_path_watch_root)
{
  EXPECT_EQ(rel("/watch", "/watch"), "");
  EXPECT_EQ(rel("/watch", "/watch/a/b.txt"), "a/b.txt");
  EXPECT_EQ(rel("/watch/sub", "/watch/sub/a"), "a");

  // outside the root: the root's parent, a sibling, a name that merely
  // starts with the root's name
  EXPECT_EQ(rel("/watch", "/"), "<outside>");
  EXPECT_EQ(rel("/watch", "/other/a"), "<outside>");
  EXPECT_EQ(rel("/watch", "/watch2/a"), "<outside>");
}

TEST(ChangeNotifyFormat, boundary_move_decisions)
{
  // the move-in / move-out decision the producer applies to a rename's two
  // ends: relativize both, then pick the record shape
  auto src = relative_path("/watch", "/watch/projects/report.txt");
  auto dest_out = relative_path("/watch", "/elsewhere/final.txt");
  ASSERT_TRUE(src.has_value());
  ASSERT_FALSE(dest_out.has_value());
  // source inside, destination outside -> the in-root half is a DELETE
  EXPECT_EQ(move_out_record(NOTIFY_MOVED_FROM, *src),
            "{\"mask\": 32, \"path\": \"projects/report.txt\"}");

  auto src_out = relative_path("/watch", "/elsewhere/report.txt");
  auto dest = relative_path("/watch", "/watch/projects/final.txt");
  ASSERT_FALSE(src_out.has_value());
  ASSERT_TRUE(dest.has_value());
  // source outside, destination inside -> destination-only MOVED_TO
  EXPECT_EQ(move_in_record(NOTIFY_MOVED_TO, *dest),
            "{\"mask\": 0, \"path\": \"\", \"dest_mask\": 1024, "
            "\"dest_path\": \"projects/final.txt\"}");

  // both outside: neither end is reported
  EXPECT_FALSE(relative_path("/watch", "/a/x").has_value());
  EXPECT_FALSE(relative_path("/watch", "/b/y").has_value());
}

TEST(ChangeNotifyFormat, classify_ops)
{
  struct Case {
    const char *op;
    bool is_dir;
    OpKind kind;
    uint32_t mask;
  } cases[] = {
    {"mknod", false, OpKind::create, 16},
    {"openc", false, OpKind::create, 16},
    {"symlink", false, OpKind::create, 16},
    {"link_local", false, OpKind::create, 16},
    {"link_remote", false, OpKind::create, 16},
    {"mkdir", true, OpKind::create_dir, 65552},
    {"unlink_local", false, OpKind::remove, 32},
    {"unlink_local", true, OpKind::remove_dir, 65568},
    {"unlink_remote", false, OpKind::remove, 32},
    // rename carries both ends and is assembled separately
    {"rename", false, OpKind::none, 0},
    {"rename", true, OpKind::none, 0},
    // metadata operations are out of scope for the event stream
    {"setattr", false, OpKind::none, 0},
    {"setxattr", false, OpKind::none, 0},
    {"removexattr", false, OpKind::none, 0},
    {"setlayout", true, OpKind::none, 0},
  };
  for (const auto &c : cases) {
    EXPECT_EQ(classify_op(c.op, c.is_dir), c.kind) << c.op;
    EXPECT_EQ(op_mask(classify_op(c.op, c.is_dir)), c.mask) << c.op;
  }
}
