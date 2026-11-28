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

#include "common/utf8.h"

/**
 * Wire format for MDS change-notification events.
 *
 * One JSON object per message. Paths are relative to the watch root. Mask
 * values follow the numbering of the reference consumer (OpenCloud's
 * posixfs watcher, CEPH_MDS_NOTIFY_*): the names come from inotify, the
 * values do not, and 0x10000 marks a directory. Every record ends with its
 * stream position (append_position()). These are pure functions so that
 * src/test/mds/TestChangeNotifier.cc can assert the format without an MDS.
 */
namespace cephfs_notify {

// The consumer's CEPH_MDS_NOTIFY_* values (not the Linux IN_* values).
enum NotifyMask : uint32_t {
  NOTIFY_CLOSE_WRITE = 0x00000004,
  NOTIFY_CREATE      = 0x00000010,
  NOTIFY_DELETE      = 0x00000020,
  NOTIFY_MOVED_FROM  = 0x00000200,
  NOTIFY_MOVED_TO    = 0x00000400,
  NOTIFY_ONLYDIR     = 0x00010000, ///< the target is a directory
};

/// True when `s` is well-formed UTF-8.
inline bool is_valid_utf8(std::string_view s)
{
  return check_utf8(s.data(), static_cast<int>(s.size())) == 0;
}

/// Append `s` to `out` escaped as the body of a JSON string. Names are
/// arbitrary bytes but JSON is UTF-8: each byte at which `s` stops being
/// well-formed UTF-8 (check_utf8()) is replaced by U+FFFD.
inline void json_escape(std::string &out, std::string_view s)
{
  while (!s.empty()) {
    // 0 when well-formed, else 1 + the offset of the first ill-formed byte
    const int bad = check_utf8(s.data(), static_cast<int>(s.size()));
    const size_t good = bad ? static_cast<size_t>(bad - 1) : s.size();
    for (char c : s.substr(0, good)) {
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
    if (!bad)
      break;
    out += "\xef\xbf\xbd";  // U+FFFD
    s.remove_prefix(good + 1);
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
 * Append the stream position to a complete record: `"rank": <rank>,
 * "epoch": <epoch>, "seq": <seq>` become its last members. `epoch` is the
 * rank's incarnation (it grows every time a daemon takes the rank over) and
 * `seq` numbers the rank's events within the epoch, starting at 1.
 */
inline void append_position(std::string &rec, int rank, uint64_t epoch,
                            uint64_t seq)
{
  if (!rec.empty() && rec.back() == '}')
    rec.pop_back();
  rec += ", \"rank\": " + std::to_string(rank) +
         ", \"epoch\": " + std::to_string(epoch) +
         ", \"seq\": " + std::to_string(seq) + "}";
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
  content,     ///< CLOSE_WRITE: the file content changed (a truncate)
};

/**
 * Classify a journaled operation (the EUpdate::type string).
 *
 * `is_dir` only matters for unlink_local (mkdir has its own op, and there
 * are no hard links to directories). `size_change` only matters for
 * setattr: a size change is a truncate, reported like a write
 * (CLOSE_WRITE); other setattrs are metadata and not reported.
 */
inline OpKind classify_op(std::string_view op, bool is_dir,
                          bool size_change = false)
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
  if (op == "open_truncate" || (op == "setattr" && size_change))
    return OpKind::content;
  // renames carry both ends and are assembled separately; the metadata ops
  // (other setattrs, setxattr, ...) are out of scope for the event stream.
  return OpKind::none;
}

/// What a journaled rename means for the event stream.
enum class RenameKind {
  move,         ///< both ends user-visible: one move record
  source_gone,  ///< the source left the user namespace: reported as a move
                ///< out (DELETE of the source)
  none,         ///< MDS housekeeping, nothing user-visible changed
};

/**
 * Classify a rename by whether each end's parent directory is MDS-internal
 * (~mdsN: the MDS directory and its stray directories). A rename from the
 * internal tree is housekeeping (stray migration, or reintegration into a
 * hard link that already names the inode) and is not reported.
 */
inline RenameKind classify_rename(bool src_internal, bool dest_internal)
{
  if (src_internal)
    return RenameKind::none;
  if (dest_internal)
    return RenameKind::source_gone;
  return RenameKind::move;
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
  case OpKind::content:
    return NOTIFY_CLOSE_WRITE;
  case OpKind::none:
    break;
  }
  return 0;
}

} // namespace cephfs_notify

#endif // CEPH_MDS_CHANGE_NOTIFY_FORMAT_H
