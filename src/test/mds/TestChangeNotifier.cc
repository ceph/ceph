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

#include <deque>
#include <string>
#include <string_view>
#include <vector>

#include "mds/ChangeNotifyFormat.h"
#include "mds/ChangeNotifyQueue.h"

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
// per message, paths relative to the watch root, mask values in the
// consumer's CEPH_MDS_NOTIFY_* numbering (not the Linux inotify values). A
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

namespace {

std::string esc(std::string_view s)
{
  std::string out;
  json_escape(out, s);
  return out;
}

const std::string FFFD = "\xef\xbf\xbd";

} // namespace

// Names are bytes, records are UTF-8: every byte at which a name stops
// being well-formed UTF-8 is replaced by U+FFFD.
TEST(ChangeNotifyFormat, utf8)
{
  // well-formed 2, 3 and 4 byte sequences, up to U+10FFFF, pass through
  EXPECT_EQ(esc("\xc3\xa9t\xc3\xa9"), "\xc3\xa9t\xc3\xa9");
  EXPECT_EQ(esc("\xe6\x97\xa5\xe6\x9c\xac"), "\xe6\x97\xa5\xe6\x9c\xac");
  EXPECT_EQ(esc("\xf0\x9f\x93\x81"), "\xf0\x9f\x93\x81");
  EXPECT_EQ(esc("\xf4\x8f\xbf\xbf"), "\xf4\x8f\xbf\xbf");
  EXPECT_TRUE(is_valid_utf8("\xe6\x97\xa5/\xf0\x9f\x93\x81"));

  // truncated sequences, at the end and before ASCII
  EXPECT_EQ(esc("a\xe6\x97"), "a" + FFFD + FFFD);
  EXPECT_EQ(esc("\xe6\x97x"), FFFD + FFFD + "x");
  // lone continuation byte, and 0xff
  EXPECT_EQ(esc("a\x80" "b"), "a" + FFFD + "b");
  EXPECT_EQ(esc("bad\xff name"), "bad" + FFFD + " name");
  // overlong encodings of '/'
  EXPECT_EQ(esc("\xc0\xaf"), FFFD + FFFD);
  EXPECT_EQ(esc("\xe0\x80\xaf"), FFFD + FFFD + FFFD);
  // UTF-16 surrogate U+D800
  EXPECT_EQ(esc("\xed\xa0\x80"), FFFD + FFFD + FFFD);
  // above U+10FFFF
  EXPECT_EQ(esc("\xf4\x90\x80\x80"), FFFD + FFFD + FFFD + FFFD);

  for (const char *s : {"a\xe6\x97", "a\x80" "b", "\xff", "\xc0\xaf",
                        "\xed\xa0\x80", "\xf4\x90\x80\x80"})
    EXPECT_FALSE(is_valid_utf8(s)) << s;

  // escaping still applies around a replaced byte
  EXPECT_EQ(event_record(NOTIFY_CREATE, "q\"\xff\n"),
            "{\"mask\": 16, \"path\": \"q\\\"" + FFFD + "\\n\"}");
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
    // a truncate changes the content: open with O_TRUNC always, setattr
    // when its mask carries the size (tested below)
    {"open_truncate", false, OpKind::content, 4},
    {"setxattr", false, OpKind::none, 0},
    {"removexattr", false, OpKind::none, 0},
    {"setlayout", true, OpKind::none, 0},
  };
  for (const auto &c : cases) {
    EXPECT_EQ(classify_op(c.op, c.is_dir), c.kind) << c.op;
    EXPECT_EQ(op_mask(classify_op(c.op, c.is_dir)), c.mask) << c.op;
  }
}

TEST(ChangeNotifyFormat, classify_truncate)
{
  // setattr with CEPH_SETATTR_SIZE in the mask: a truncate, reported as
  // CLOSE_WRITE (the consumer's "content changed, rescan")
  EXPECT_EQ(classify_op("setattr", false, true), OpKind::content);
  EXPECT_EQ(op_mask(classify_op("setattr", false, true)), NOTIFY_CLOSE_WRITE);
  // chmod/chown/utimes: setattr without the size bit, not reported
  EXPECT_EQ(classify_op("setattr", false, false), OpKind::none);
  // the size flag means nothing for the other operations
  EXPECT_EQ(classify_op("setxattr", false, true), OpKind::none);
  EXPECT_EQ(classify_op("openc", false, true), OpKind::create);
  EXPECT_EQ(classify_op("open_truncate", false, false), OpKind::content);
}

TEST(ChangeNotifyFormat, classify_renames)
{
  // user path -> user path: an ordinary move
  EXPECT_EQ(classify_rename(false, false), RenameKind::move);
  // stray -> user path: reintegration of an unlinked primary into a
  // remaining hard link; the name already existed, nothing to report
  EXPECT_EQ(classify_rename(true, false), RenameKind::none);
  // stray -> stray: migration of strays off a stopping rank
  EXPECT_EQ(classify_rename(true, true), RenameKind::none);
  // user path -> internal tree: reported as the source leaving
  EXPECT_EQ(classify_rename(false, true), RenameKind::source_gone);
}

// The stream position is appended after the existing members of every
// record shape; nothing before it moves.
TEST(ChangeNotifyFormat, stream_position)
{
  std::string rec = event_record(NOTIFY_CREATE, "projects/alpha/report.txt");
  append_position(rec, 0, 7, 1);
  EXPECT_EQ(rec,
            "{\"mask\": 16, \"path\": \"projects/alpha/report.txt\", "
            "\"rank\": 0, \"epoch\": 7, \"seq\": 1}");

  rec = move_record(NOTIFY_MOVED_FROM, "projects/alpha/report.txt",
                    NOTIFY_MOVED_TO, "projects/alpha/final.txt");
  append_position(rec, 1, 42, 18446744073709551615ULL);
  EXPECT_EQ(rec,
            "{\"mask\": 0, \"path\": \"\", \"src_mask\": 512, "
            "\"src_path\": \"projects/alpha/report.txt\", \"dest_mask\": 1024, "
            "\"dest_path\": \"projects/alpha/final.txt\", "
            "\"rank\": 1, \"epoch\": 42, \"seq\": 18446744073709551615}");

  rec = move_in_record(NOTIFY_MOVED_TO | NOTIFY_ONLYDIR, "projects/gamma");
  append_position(rec, 2, 3, 9);
  EXPECT_EQ(rec,
            "{\"mask\": 0, \"path\": \"\", \"dest_mask\": 66560, "
            "\"dest_path\": \"projects/gamma\", "
            "\"rank\": 2, \"epoch\": 3, \"seq\": 9}");

  rec = move_out_record(NOTIFY_MOVED_FROM | NOTIFY_ONLYDIR, "projects/beta");
  append_position(rec, 0, 3, 10);
  EXPECT_EQ(rec,
            "{\"mask\": 65568, \"path\": \"projects/beta\", "
            "\"rank\": 0, \"epoch\": 3, \"seq\": 10}");

  // a path that ends in a brace is inside the string: only the record's
  // own closing brace is replaced
  rec = event_record(NOTIFY_CREATE, "dir}");
  append_position(rec, 0, 1, 2);
  EXPECT_EQ(rec, "{\"mask\": 16, \"path\": \"dir}\", "
                 "\"rank\": 0, \"epoch\": 1, \"seq\": 2}");
}

namespace {

std::vector<uint64_t> drain_seqs(PositionedQueue &q)
{
  std::deque<QueuedEvent> out;
  q.take_all(out);
  std::vector<uint64_t> seqs;
  for (const auto &ev : out)
    seqs.push_back(ev.seq);
  return seqs;
}

} // namespace

// A dropped event still consumes its sequence number, so the loss shows up
// as a gap in what is delivered.
TEST(ChangeNotifyQueue, drops_leave_gaps)
{
  PositionedQueue q(2);
  q.set_epoch(5);
  EXPECT_TRUE(q.push("a"));
  EXPECT_TRUE(q.push("b"));
  EXPECT_FALSE(q.push("c"));   // full: dropped, seq 3 consumed
  EXPECT_FALSE(q.push("d"));   // full: dropped, seq 4 consumed
  EXPECT_EQ(q.dropped(), 2u);
  EXPECT_EQ(q.accepted(), 2u);
  EXPECT_EQ(drain_seqs(q), (std::vector<uint64_t>{1, 2}));
  EXPECT_EQ(q.size(), 0u);

  EXPECT_TRUE(q.push("e"));
  std::deque<QueuedEvent> out;
  q.take_all(out);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].seq, 5u);
  EXPECT_EQ(out[0].epoch, 5u);
  EXPECT_EQ(out[0].record, "e");
  EXPECT_EQ(q.dropped(), 2u);
  EXPECT_EQ(q.accepted(), 3u);
  EXPECT_EQ(q.next_seq(), 6u);
}

TEST(ChangeNotifyQueue, epoch_restarts_sequence)
{
  PositionedQueue q(8);
  EXPECT_EQ(q.epoch(), 0u);
  EXPECT_EQ(q.next_seq(), 1u);
  q.set_epoch(10);
  q.push("a");
  q.push("b");
  EXPECT_EQ(q.next_seq(), 3u);

  // the same epoch again (every map update repeats it) changes nothing
  q.set_epoch(10);
  EXPECT_EQ(q.next_seq(), 3u);

  // a new incarnation: numbering starts over, queued events keep the
  // position they were given
  q.set_epoch(11);
  EXPECT_EQ(q.next_seq(), 1u);
  q.push("c");
  std::deque<QueuedEvent> out;
  q.take_all(out);
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out[0].epoch, 10u);
  EXPECT_EQ(out[0].seq, 1u);
  EXPECT_EQ(out[1].epoch, 10u);
  EXPECT_EQ(out[1].seq, 2u);
  EXPECT_EQ(out[2].epoch, 11u);
  EXPECT_EQ(out[2].seq, 1u);
}

TEST(ChangeNotifyQueue, zero_capacity_is_one)
{
  PositionedQueue q(0);
  EXPECT_EQ(q.capacity(), 1u);
  EXPECT_TRUE(q.push("a"));
  EXPECT_FALSE(q.push("b"));
  q.set_capacity(3);
  EXPECT_TRUE(q.push("c"));
  EXPECT_EQ(drain_seqs(q), (std::vector<uint64_t>{1, 3}));
}
