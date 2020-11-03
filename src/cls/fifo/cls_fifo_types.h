// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"

#include "common/ceph_time.h"

class JSONObj;

namespace rados::cls::fifo {
struct objv {
  std::string instance;
  std::uint64_t ver{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(instance, bl);
    encode(ver, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(instance, bl);
    decode(ver, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);

  bool operator ==(const objv& rhs) const {
    return (instance == rhs.instance &&
	    ver == rhs.ver);
  }
  bool operator !=(const objv& rhs) const {
    return (instance != rhs.instance ||
	    ver != rhs.ver);
  }
  bool same_or_later(const objv& rhs) const {
    return (instance == rhs.instance ||
	    ver >= rhs.ver);
  }

  bool empty() const {
    return instance.empty();
  }

  std::string to_str() const {
    return fmt::format("{}{{{}}}", instance, ver);
  }
};
WRITE_CLASS_ENCODER(objv)
inline ostream& operator <<(std::ostream& os, const objv& objv)
{
  return os << objv.to_str();
}

struct data_params {
  std::uint64_t max_part_size{0};
  std::uint64_t max_entry_size{0};
  std::uint64_t full_size_threshold{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_part_size, bl);
    encode(max_entry_size, bl);
    encode(full_size_threshold, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_part_size, bl);
    decode(max_entry_size, bl);
    decode(full_size_threshold, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);

  bool operator ==(const data_params& rhs) const {
    return (max_part_size == rhs.max_part_size &&
	    max_entry_size == rhs.max_entry_size &&
	    full_size_threshold == rhs.full_size_threshold);
  }
};
WRITE_CLASS_ENCODER(data_params)
inline std::ostream& operator <<(std::ostream& m, const data_params& d) {
  return m << "max_part_size: " << d.max_part_size << ", "
	   << "max_entry_size: " << d.max_entry_size << ", "
	   << "full_size_threshold: " << d.full_size_threshold;
}

struct journal_entry {
  enum class Op {
    unknown  = 0,
    create   = 1,
    set_head = 2,
    remove   = 3,
  } op{Op::unknown};

  std::int64_t part_num{0};
  std::string part_tag;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode((int)op, bl);
    encode(part_num, bl);
    encode(part_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    int i;
    decode(i, bl);
    op = static_cast<Op>(i);
    decode(part_num, bl);
    decode(part_tag, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;

  bool operator ==(const journal_entry& e) {
    return (op == e.op &&
	    part_num == e.part_num &&
	    part_tag == e.part_tag);
  }
};
WRITE_CLASS_ENCODER(journal_entry)
inline std::ostream& operator <<(std::ostream& m, const journal_entry::Op& o) {
  switch (o) {
  case journal_entry::Op::unknown:
    return m << "Op::unknown";
  case journal_entry::Op::create:
    return m << "Op::create";
  case journal_entry::Op::set_head:
    return m << "Op::set_head";
  case journal_entry::Op::remove:
    return m << "Op::remove";
  }
  return m << "Bad value: " << static_cast<int>(o);
}
inline std::ostream& operator <<(std::ostream& m, const journal_entry& j) {
  return m << "op: " << j.op << ", "
	   << "part_num: " << j.part_num <<  ", "
	   << "part_tag: " << j.part_tag;
}

// This is actually a useful builder, since otherwise we end up with
// four uint64_ts in a row and only care about a subset at a time.
class update {
  std::optional<std::uint64_t> tail_part_num_;
  std::optional<std::uint64_t> head_part_num_;
  std::optional<std::uint64_t> min_push_part_num_;
  std::optional<std::uint64_t> max_push_part_num_;
  std::vector<fifo::journal_entry> journal_entries_add_;
  std::vector<fifo::journal_entry> journal_entries_rm_;

public:

  update&& tail_part_num(std::optional<std::uint64_t> num) noexcept {
    tail_part_num_ = num;
    return std::move(*this);
  }
  auto tail_part_num() const noexcept {
    return tail_part_num_;
  }

  update&& head_part_num(std::optional<std::uint64_t> num) noexcept {
    head_part_num_ = num;
    return std::move(*this);
  }
  auto head_part_num() const noexcept {
    return head_part_num_;
  }

  update&& min_push_part_num(std::optional<std::uint64_t> num)
    noexcept {
    min_push_part_num_ = num;
    return std::move(*this);
  }
  auto min_push_part_num() const noexcept {
    return min_push_part_num_;
  }

  update&& max_push_part_num(std::optional<std::uint64_t> num) noexcept {
    max_push_part_num_ = num;
    return std::move(*this);
  }
  auto max_push_part_num() const noexcept {
    return max_push_part_num_;
  }

  update&& journal_entry_add(fifo::journal_entry entry) {
    journal_entries_add_.push_back(std::move(entry));
    return std::move(*this);
  }
  update&& journal_entries_add(
    std::optional<std::vector<fifo::journal_entry>>&& entries) {
    if (entries) {
      journal_entries_add_ = std::move(*entries);
    } else {
      journal_entries_add_.clear();
    }
    return std::move(*this);
  }
  const auto& journal_entries_add() const & noexcept {
    return journal_entries_add_;
  }
  auto&& journal_entries_add() && noexcept {
    return std::move(journal_entries_add_);
  }

  update&& journal_entry_rm(fifo::journal_entry entry) {
    journal_entries_rm_.push_back(std::move(entry));
    return std::move(*this);
  }
  update&& journal_entries_rm(
    std::optional<std::vector<fifo::journal_entry>>&& entries) {
    if (entries) {
      journal_entries_rm_ = std::move(*entries);
    } else {
      journal_entries_rm_.clear();
    }
    return std::move(*this);
  }
  const auto& journal_entries_rm() const & noexcept {
    return journal_entries_rm_;
  }
  auto&& journal_entries_rm() && noexcept {
    return std::move(journal_entries_rm_);
  }
  friend std::ostream& operator <<(std::ostream& m, const update& u);
};
inline std::ostream& operator <<(std::ostream& m, const update& u) {
  bool prev = false;
  if (u.tail_part_num_) {
    m << "tail_part_num: " << *u.tail_part_num_;
    prev = true;
  }
  if (u.head_part_num_) {
    if (prev)
      m << ", ";
    m << "head_part_num: " << *u.head_part_num_;
    prev = true;
  }
  if (u.min_push_part_num_) {
    if (prev)
      m << ", ";
    m << "min_push_part_num: " << *u.min_push_part_num_;
    prev = true;
  }
  if (u.max_push_part_num_) {
    if (prev)
      m << ", ";
    m << "max_push_part_num: " << *u.max_push_part_num_;
    prev = true;
  }
  if (!u.journal_entries_add_.empty()) {
    if (prev)
      m << ", ";
    m << "journal_entries_add: {" << u.journal_entries_add_ << "}";
    prev = true;
  }
  if (!u.journal_entries_rm_.empty()) {
    if (prev)
      m << ", ";
    m << "journal_entries_rm: {" << u.journal_entries_rm_ << "}";
    prev = true;
  }
  if (!prev)
    m << "(none)";
  return m;
}

struct info {
  std::string id;
  objv version;
  std::string oid_prefix;
  data_params params;

  std::int64_t tail_part_num{0};
  std::int64_t head_part_num{-1};
  std::int64_t min_push_part_num{0};
  std::int64_t max_push_part_num{-1};

  std::string head_tag;
  std::map<int64_t, string> tags;

  std::multimap<int64_t, journal_entry> journal;

  bool need_new_head() const {
    return (head_part_num < min_push_part_num);
  }

  bool need_new_part() const {
    return (max_push_part_num < min_push_part_num);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(version, bl);
    encode(oid_prefix, bl);
    encode(params, bl);
    encode(tail_part_num, bl);
    encode(head_part_num, bl);
    encode(min_push_part_num, bl);
    encode(max_push_part_num, bl);
    encode(tags, bl);
    encode(head_tag, bl);
    encode(journal, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(version, bl);
    decode(oid_prefix, bl);
    decode(params, bl);
    decode(tail_part_num, bl);
    decode(head_part_num, bl);
    decode(min_push_part_num, bl);
    decode(max_push_part_num, bl);
    decode(tags, bl);
    decode(head_tag, bl);
    decode(journal, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);

  std::string part_oid(std::int64_t part_num) const {
    return fmt::format("{}.{}", oid_prefix, part_num);
  }

  journal_entry next_journal_entry(std::string tag) const {
    journal_entry entry;
    entry.op = journal_entry::Op::create;
    entry.part_num = max_push_part_num + 1;
    entry.part_tag = std::move(tag);
    return entry;
  }

  std::optional<std::string>
  apply_update(const update& update) {
    if (update.tail_part_num()) {
      tail_part_num = *update.tail_part_num();
    }

    if (update.min_push_part_num()) {
      min_push_part_num = *update.min_push_part_num();
    }

    if (update.max_push_part_num()) {
      max_push_part_num = *update.max_push_part_num();
    }

    for (const auto& entry : update.journal_entries_add()) {
      auto iter = journal.find(entry.part_num);
      if (iter != journal.end() &&
	  iter->second.op == entry.op) {
	/* don't allow multiple concurrent (same) operations on the same part,
	   racing clients should use objv to avoid races anyway */
	return fmt::format("multiple concurrent operations on same part are not "
			   "allowed, part num={}", entry.part_num);
      }

      if (entry.op == journal_entry::Op::create) {
	tags[entry.part_num] = entry.part_tag;
      }

      journal.emplace(entry.part_num, entry);
    }

    for (const auto& entry : update.journal_entries_rm()) {
      journal.erase(entry.part_num);
    }

    if (update.head_part_num()) {
      tags.erase(head_part_num);
      head_part_num = *update.head_part_num();
      auto iter = tags.find(head_part_num);
      if (iter != tags.end()) {
	head_tag = iter->second;
      } else {
	head_tag.erase();
      }
    }

    return std::nullopt;
  }
};
WRITE_CLASS_ENCODER(info)
inline std::ostream& operator <<(std::ostream& m, const info& i) {
  return m << "id: " << i.id << ", "
	   << "version: " << i.version << ", "
	   << "oid_prefix: " << i.oid_prefix << ", "
	   << "params: {" << i.params << "}, "
	   << "tail_part_num: " << i.tail_part_num << ", "
	   << "head_part_num: " << i.head_part_num << ", "
	   << "min_push_part_num: " << i.min_push_part_num << ", "
	   << "max_push_part_num: " << i.max_push_part_num << ", "
	   << "head_tag: " << i.head_tag << ", "
	   << "tags: {" << i.tags << "}, "
	   << "journal: {" << i.journal;
}

struct part_list_entry {
  ceph::buffer::list data;
  std::uint64_t ofs = 0;
  ceph::real_time mtime;

  part_list_entry() {}
  part_list_entry(ceph::buffer::list&& data,
		  uint64_t ofs,
		  ceph::real_time mtime)
    : data(std::move(data)), ofs(ofs), mtime(mtime) {}


  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(data, bl);
    encode(ofs, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(data, bl);
    decode(ofs, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(part_list_entry)
inline std::ostream& operator <<(std::ostream& m,
				 const part_list_entry& p) {
  using ceph::operator <<;
  return m << "data: " << p.data << ", "
	   << "ofs: " << p.ofs << ", "
	   << "mtime: " << p.mtime;
}

struct part_header {
  std::string tag;

  data_params params;

  std::uint64_t magic{0};

  std::uint64_t min_ofs{0};
  std::uint64_t last_ofs{0};
  std::uint64_t next_ofs{0};
  std::uint64_t min_index{0};
  std::uint64_t max_index{0};
  ceph::real_time max_time;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(params, bl);
    encode(magic, bl);
    encode(min_ofs, bl);
    encode(last_ofs, bl);
    encode(next_ofs, bl);
    encode(min_index, bl);
    encode(max_index, bl);
    encode(max_time, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(params, bl);
    decode(magic, bl);
    decode(min_ofs, bl);
    decode(last_ofs, bl);
    decode(next_ofs, bl);
    decode(min_index, bl);
    decode(max_index, bl);
    decode(max_time, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(part_header)
inline std::ostream& operator <<(std::ostream& m, const part_header& p) {
  using ceph::operator <<;
  return m << "tag: " << p.tag << ", "
	   << "params: {" << p.params << "}, "
	   << "magic: " << p.magic << ", "
	   << "min_ofs: " << p.min_ofs << ", "
	   << "last_ofs: " << p.last_ofs << ", "
	   << "next_ofs: " << p.next_ofs << ", "
	   << "min_index: " << p.min_index << ", "
	   << "max_index: " << p.max_index << ", "
	   << "max_time: " << p.max_time;
}
} // namespace rados::cls::fifo
