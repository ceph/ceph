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

#ifndef CEPH_MDS_CHANGE_NOTIFY_FORMAT_H
#define CEPH_MDS_CHANGE_NOTIFY_FORMAT_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

/**
 * Wire format for MDS change-notification events.
 *
 * One JSON object per message; paths are relative to the configured watch
 * root and the mask values are the Linux inotify IN_* bits. That is the
 * contract a consumer dispatches on (OpenCloud's posixfs watcher consumes
 * the same numbering), so the pieces that define it - record assembly,
 * watch-root path handling and operation classification - live here as
 * pure functions, apart from the MDS-side producer they serve:
 * src/test/mds/TestChangeNotifier.cc asserts the format without an MDS.
 */
namespace cephfs_notify {

// Event mask bits (Linux inotify IN_* values; consumers use the same set).
enum NotifyMask : uint32_t {
  NOTIFY_ACCESS        = 0x00000001,
  NOTIFY_ATTRIB        = 0x00000002,
  NOTIFY_CLOSE_WRITE   = 0x00000004,
  NOTIFY_CLOSE_NOWRITE = 0x00000008,
  NOTIFY_CREATE        = 0x00000010,
  NOTIFY_DELETE        = 0x00000020,
  NOTIFY_DELETE_SELF   = 0x00000040,
  NOTIFY_MODIFY        = 0x00000080,
  NOTIFY_MOVE_SELF     = 0x00000100,
  NOTIFY_MOVED_FROM    = 0x00000200,
  NOTIFY_MOVED_TO      = 0x00000400,
  NOTIFY_OPEN          = 0x00000800,
  NOTIFY_CLOSE         = 0x00001000,
  NOTIFY_MOVE          = 0x00002000,
  NOTIFY_ONESHOT       = 0x00004000,
  NOTIFY_IGNORED       = 0x00008000,
  NOTIFY_ONLYDIR       = 0x00010000,
};

/// Append `s` to `out` escaped as the body of a JSON string.
inline void json_escape(std::string &out, std::string_view s)
{
  for (char c : s) {
    switch (c) {
    case '"':
      out += "\\\"";
      break;
    case '\\':
      out += "\\\\";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      if (static_cast<unsigned char>(c) < 0x20) {
        static const char hex[] = "0123456789abcdef";
        auto u = static_cast<unsigned char>(c);
        out += "\\u00";
        out += hex[u >> 4];
        out += hex[u & 0xf];
      } else {
        out += c;
      }
    }
  }
}

/// Record for a single event: {"mask": <mask>, "path": "<rel_path>"}
inline std::string event_record(uint32_t mask, std::string_view rel_path)
{
  std::string rec = "{\"mask\": " + std::to_string(mask) + ", \"path\": \"";
  json_escape(rec, rel_path);
  rec += "\"}";
  return rec;
}

/// Record for a move with both ends inside the watch root: the source half
/// (MOVED_FROM) and the destination half (MOVED_TO) in one message.
inline std::string move_record(uint32_t src_mask, std::string_view src_rel,
                               uint32_t dest_mask, std::string_view dest_rel)
{
  std::string rec = "{\"mask\": 0, \"path\": \"\", \"src_mask\": " +
                    std::to_string(src_mask) + ", \"src_path\": \"";
  json_escape(rec, src_rel);
  rec += "\", \"dest_mask\": " + std::to_string(dest_mask) +
         ", \"dest_path\": \"";
  json_escape(rec, dest_rel);
  rec += "\"}";
  return rec;
}

/// Record for a move whose source is outside the watch root (moved in): the
/// destination half only. The consumer assimilates the destination.
inline std::string move_in_record(uint32_t dest_mask, std::string_view dest_rel)
{
  std::string rec = "{\"mask\": 0, \"path\": \"\", \"dest_mask\": " +
                    std::to_string(dest_mask) + ", \"dest_path\": \"";
  json_escape(rec, dest_rel);
  rec += "\"}";
  return rec;
}

/// Record for a move whose destination is outside the watch root (moved
/// out): a DELETE on the in-root source, because the consumer has no
/// MOVED_FROM-only action; `src_mask` supplies ONLYDIR for directories.
inline std::string move_out_record(uint32_t src_mask, std::string_view src_rel)
{
  return event_record(NOTIFY_DELETE | (src_mask & NOTIFY_ONLYDIR), src_rel);
}

/**
 * `path` relative to the watch `root`, or nullopt when it is outside.
 *
 * root "/" means the filesystem root, so every absolute path is inside.
 * `path` and `root` are MDS-internal absolute paths; the caller passes a
 * `root` without a trailing slash ("/" excepted).
 */
inline std::optional<std::string> relative_path(std::string_view root,
                                                std::string_view path)
{
  if (root == "/") {
    if (!path.empty() && path.front() == '/')
      path.remove_prefix(1);
    return std::string(path);
  }
  if (path == root)
    return std::string();
  if (path.size() > root.size() &&
      path.compare(0, root.size(), root) == 0 &&
      path[root.size()] == '/')
    return std::string(path.substr(root.size() + 1));
  return std::nullopt;
}

/// What a journaled namespace operation means for the event stream.
enum class OpKind {
  none,        ///< nothing the consumer acts on
  create,      ///< CREATE
  create_dir,  ///< CREATE|ONLYDIR
  remove,      ///< DELETE
  remove_dir,  ///< DELETE|ONLYDIR
};

/**
 * Classify a journaled operation (the EUpdate::type string).
 *
 * `is_dir` is the kind of the affected inode and is only consulted for
 * unlink_local: creates that make a directory have their own op ("mkdir"),
 * and hardlinks to directories do not exist, so link/unlink_remote are
 * never directories.
 */
inline OpKind classify_op(std::string_view op, bool is_dir)
{
  if (op == "mkdir")
    return OpKind::create_dir;
  if (op == "mknod" || op == "openc" || op == "symlink" ||
      op == "link_local" || op == "link_remote")
    return OpKind::create;
  if (op == "unlink_local")
    return is_dir ? OpKind::remove_dir : OpKind::remove;
  if (op == "unlink_remote")
    return OpKind::remove;
  // renames carry both ends and are assembled separately; the metadata ops
  // (setattr, setxattr, ...) are out of scope for the event stream.
  return OpKind::none;
}

/// The mask emitted for `kind`; 0 for OpKind::none.
inline uint32_t op_mask(OpKind kind)
{
  switch (kind) {
  case OpKind::create:
    return NOTIFY_CREATE;
  case OpKind::create_dir:
    return NOTIFY_CREATE | NOTIFY_ONLYDIR;
  case OpKind::remove:
    return NOTIFY_DELETE;
  case OpKind::remove_dir:
    return NOTIFY_DELETE | NOTIFY_ONLYDIR;
  case OpKind::none:
    break;
  }
  return 0;
}

} // namespace cephfs_notify

#endif // CEPH_MDS_CHANGE_NOTIFY_FORMAT_H
