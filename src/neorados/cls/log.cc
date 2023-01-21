// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#include "log.h"

#include <string>
#include <utility>
#include <vector>

#include "include/buffer.h"
#include "common/ceph_time.h"

#include "include/neorados/RADOS.hpp"


#include "cls/log/cls_log_types.h"
#include "cls/log/cls_log_ops.h"

namespace neorados::cls::log {
using boost::system::error_code;
namespace buffer = ceph::buffer;

void add(neorados::WriteOp& op, std::vector<entry> entries)
{
  buffer::list in;
  ::cls::log::ops::add_op call;
  call.entries = std::move(entries);
  encode(call, in);
  op.exec("log", "add", in);
}

void add(neorados::WriteOp& op, entry e)
{
  bufferlist in;
  ::cls::log::ops::add_op call;
  call.entries.push_back(std::move(e));
  encode(call, in);
  op.exec("log", "add", in);
}

void add(neorados::WriteOp& op, ceph::real_time timestamp,
	 std::string section, std::string name,
	 buffer::list&& bl)
{
  bufferlist in;
  ::cls::log::ops::add_op call;
  call.entries.emplace_back(timestamp, std::move(section),
			    std::move(name), std::move(bl));
  encode(call, in);
  op.exec("log", "add", in);
}

void list(ReadOp& op, ceph::real_time from, ceph::real_time to,
	  std::optional<std::string> in_marker, std::span<entry> entries,
	  std::span<entry>* result,
	  std::optional<std::string>* const out_marker)
{
  bufferlist inbl;
  ::cls::log::ops::list_op call;
  call.from_time = from;
  call.to_time = to;
  call.marker = std::move(in_marker).value_or("");
  call.max_entries = entries.size();

  encode(call, inbl);
  op.exec("log", "list", inbl,
          [entries, result, out_marker](error_code ec, const buffer::list& bl) {
            ::cls::log::ops::list_ret ret;
            if (!ec) {
	      auto iter = bl.cbegin();
	      decode(ret, iter);
	      if (result) {
		*result = entries.first(ret.entries.size());
		std::move(ret.entries.begin(), ret.entries.end(),
			  entries.begin());
	      }
	      if (out_marker) {
		*out_marker = (ret.truncated ?
			       std::move(ret.marker) :
			       std::optional<std::string>{});
	      }
            }
          });
}

void trim(neorados::WriteOp& op, ceph::real_time from_time,
	  ceph::real_time to_time)
{
  bufferlist in;
  ::cls::log::ops::trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  encode(call, in);
  op.exec("log", "trim", in);
}

void trim(neorados::WriteOp& op, std::string from_marker,
	  std::string to_marker)
{
  bufferlist in;
  ::cls::log::ops::trim_op call;
  call.from_marker = std::move(from_marker);
  call.to_marker = std::move(to_marker);
  encode(call, in);
  op.exec("log", "trim", in);
}

void info(neorados::ReadOp& op, header* const h)
{
  buffer::list inbl;
  ::cls::log::ops::info_op call;

  encode(call, inbl);

  op.exec("log", "info", inbl,
          [h](error_code ec,
              const buffer::list& bl) {
            ::cls::log::ops::info_ret ret;
            if (!ec) {
	      auto iter = bl.cbegin();
	      decode(ret, iter);
	      if (h)
		*h = std::move(ret.header);
            }
          });
}
} // namespace neorados::cls::log
