// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <charconv>
#include <cstdint>
#include <numeric>
#include <optional>
#include <string_view>

#include <fmt/format.h>

#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/buffer.h"

#include "common/random_string.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

#include "fifo.h"

namespace neorados::cls::fifo {
namespace bs = boost::system;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;

void create_meta(WriteOp& op, std::string_view id,
		 std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive,
		 std::uint64_t max_part_size,
		 std::uint64_t max_entry_size)
{
  fifo::op::create_meta cm;

  cm.id = id;
  cm.version = objv;
  cm.oid_prefix = oid_prefix;
  cm.max_part_size = max_part_size;
  cm.max_entry_size = max_entry_size;
  cm.exclusive = exclusive;

  cb::list in;
  encode(cm, in);
  op.exec(fifo::op::CLASS, fifo::op::CREATE_META, in);
}

void get_meta(ReadOp& op, std::optional<fifo::objv> objv,
	      bs::error_code* ec_out, fifo::info* info,
	      std::uint32_t* part_header_size,
	      std::uint32_t* part_entry_overhead)
{
  fifo::op::get_meta gm;
  gm.version = objv;
  cb::list in;
  encode(gm, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_META, in,
	  [ec_out, info, part_header_size,
	   part_entry_overhead](bs::error_code ec, const cb::list& bl) {
	    fifo::op::get_meta_reply reply;
	    if (!ec) try {
		auto iter = bl.cbegin();
		decode(reply, iter);
	      } catch (const cb::error& err) {
		ec = err.code();
	      }
	    if (ec_out) *ec_out = ec;
	    if (info) *info = std::move(reply.info);
	    if (part_header_size) *part_header_size = reply.part_header_size;
	    if (part_entry_overhead)
		*part_entry_overhead = reply.part_entry_overhead;
	  });
};

void update_meta(WriteOp& op, const fifo::objv& objv,
		 const fifo::update& update)
{
  fifo::op::update_meta um;

  um.version = objv;
  um.tail_part_num = update.tail_part_num();
  um.head_part_num = update.head_part_num();
  um.min_push_part_num = update.min_push_part_num();
  um.max_push_part_num = update.max_push_part_num();
  um.journal_entries_add = std::move(update).journal_entries_add();
  um.journal_entries_rm = std::move(update).journal_entries_rm();

  cb::list in;
  encode(um, in);
  op.exec(fifo::op::CLASS, fifo::op::UPDATE_META, in);
}

void part_init(WriteOp& op, std::string_view tag,
	       fifo::data_params params)
{
  fifo::op::init_part ip;

  ip.tag = tag;
  ip.params = params;

  cb::list in;
  encode(ip, in);
  op.exec(fifo::op::CLASS, fifo::op::INIT_PART, in);
}

void push_part(WriteOp& op, std::string_view tag,
	       std::deque<cb::list> data_bufs,
	       fu2::unique_function<void(bs::error_code, int)> f)
{
  fifo::op::push_part pp;

  pp.tag = tag;
  pp.data_bufs = data_bufs;
  pp.total_len = 0;

  for (const auto& bl : data_bufs)
    pp.total_len += bl.length();

  cb::list in;
  encode(pp, in);
  op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in,
	  [f = std::move(f)](bs::error_code ec, int r, const cb::list&) mutable {
	    std::move(f)(ec, r);
	  });
  op.returnvec();
}

void trim_part(WriteOp& op,
	       std::optional<std::string_view> tag,
	       std::uint64_t ofs)
{
  fifo::op::trim_part tp;

  tp.tag = tag;
  tp.ofs = ofs;

  bufferlist in;
  encode(tp, in);
  op.exec(fifo::op::CLASS, fifo::op::TRIM_PART, in);
}

void list_part(ReadOp& op,
	       std::optional<string_view> tag,
	       std::uint64_t ofs,
	       std::uint64_t max_entries,
	       bs::error_code* ec_out,
	       std::vector<fifo::part_list_entry>* entries,
	       bool* more,
	       bool* full_part,
	       std::string* ptag)
{
  fifo::op::list_part lp;

  lp.tag = tag;
  lp.ofs = ofs;
  lp.max_entries = max_entries;

  bufferlist in;
  encode(lp, in);
  op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in,
	  [entries, more, full_part, ptag, ec_out](bs::error_code ec,
						   const cb::list& bl) {
	    if (ec) {
	      if (ec_out) *ec_out = ec;
	      return;
	    }

	    fifo::op::list_part_reply reply;
	    auto iter = bl.cbegin();
	    try {
	      decode(reply, iter);
	    } catch (const cb::error& err) {
	      if (ec_out) *ec_out = ec;
	      return;
	    }

	    if (entries) *entries = std::move(reply.entries);
	    if (more) *more = reply.more;
	    if (full_part) *full_part = reply.full_part;
	    if (ptag) *ptag = reply.tag;
	  });
}

void get_part_info(ReadOp& op,
		   bs::error_code* out_ec,
		   fifo::part_header* header)
{
  fifo::op::get_part_info gpi;

  bufferlist in;
  encode(gpi, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in,
	  [out_ec, header](bs::error_code ec, const cb::list& bl) {
	    if (ec) {
	      if (out_ec) *out_ec = ec;
	    }
	    fifo::op::get_part_info_reply reply;
	    auto iter = bl.cbegin();
	    try {
	      decode(reply, iter);
	    } catch (const cb::error& err) {
	      if (out_ec) *out_ec = ec;
	      return;
	    }

	    if (header) *header = std::move(reply.header);
	  });
}

std::optional<marker> FIFO::to_marker(std::string_view s) {
  marker m;
  if (s.empty()) {
    m.num = info.tail_part_num;
    m.ofs = 0;
    return m;
  }

  auto pos = s.find(':');
  if (pos == string::npos) {
    return std::nullopt;
  }

  auto num = s.substr(0, pos);
  auto ofs = s.substr(pos + 1);

  auto r = std::from_chars(num.data(), num.data() + num.size(), m.num, 10);
  if ((r.ec != std::errc{}) || (r.ptr != num.data() + num.size())) {
    return std::nullopt;
  }
  r = std::from_chars(ofs.data(), ofs.data() + ofs.size(), m.ofs, 10);
  if ((r.ec != std::errc{}) || (r.ptr != ofs.data() + ofs.size())) {
    return std::nullopt;
  }
  return m;
}

bs::error_code FIFO::apply_update(fifo::info* info,
				  const fifo::objv& objv,
				  const fifo::update& update) {
  std::unique_lock l(m);
  auto err = info->apply_update(update);
  if (objv != info->version) {
    ldout(r->cct(), 0) << __func__ << "(): Raced locally!" << dendl;
    return errc::raced;
  }
  if (err) {
    ldout(r->cct(), 0) << __func__ << "(): ERROR: " << err << dendl;
    return errc::update_failed;
  }

  ++info->version.ver;

  return {};
}

std::string FIFO::generate_tag() const
{
  static constexpr auto HEADER_TAG_SIZE = 16;
  return gen_rand_alphanumeric_plain(r->cct(), HEADER_TAG_SIZE);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"
class error_category : public ceph::converting_category {
public:
  error_category(){}
  const char* name() const noexcept override;
  const char* message(int ev, char*, std::size_t) const noexcept override;
  std::string message(int ev) const override;
  bs::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const bs::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

const char* error_category::name() const noexcept {
  return "FIFO";
}

const char* error_category::message(int ev, char*, std::size_t) const noexcept {
  if (ev == 0)
    return "No error";

  switch (static_cast<errc>(ev)) {
  case errc::raced:
    return "Retry-race count exceeded";

  case errc::inconsistency:
    return "Inconsistent result! New head before old head";

  case errc::entry_too_large:
    return "Pushed entry too large";

  case errc::invalid_marker:
    return "Invalid marker string";

  case errc::update_failed:
    return "Update failed";
  }

  return "Unknown error";
}

std::string error_category::message(int ev) const {
  return message(ev, nullptr, 0);
}

bs::error_condition
error_category::default_error_condition(int ev) const noexcept {
  switch (static_cast<errc>(ev)) {
  case errc::raced:
    return bs::errc::operation_canceled;

  case errc::inconsistency:
    return bs::errc::io_error;

  case errc::entry_too_large:
    return bs::errc::value_too_large;

  case errc::invalid_marker:
    return bs::errc::invalid_argument;

  case errc::update_failed:
    return bs::errc::invalid_argument;
  }

  return { ev, *this };
}

bool error_category::equivalent(int ev, const bs::error_condition& c) const noexcept {
  return default_error_condition(ev) == c;
}

int error_category::from_code(int ev) const noexcept {
  switch (static_cast<errc>(ev)) {
  case errc::raced:
    return -ECANCELED;

  case errc::inconsistency:
    return -EIO;

  case errc::entry_too_large:
    return -E2BIG;

  case errc::invalid_marker:
    return -EINVAL;

  case errc::update_failed:
    return -EINVAL;

  }
  return -EDOM;
}

const bs::error_category& error_category() noexcept {
  static const class error_category c;
  return c;
}

}
