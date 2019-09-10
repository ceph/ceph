// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "cls/log/cls_log_ops.h"

#include "log.h"

namespace RADOS::CLS::log {

void add_prepare_entry(cls_log_entry& entry, ceph::real_time timestamp,
                       std::string_view section, std::string_view name,
                       cb::list&& bl)
{
  entry.timestamp = timestamp;
  entry.section = section;
  entry.name = name;
  entry.data = std::move(bl);
}

void add(WriteOp& op, std::vector<cls_log_entry>&& entries, bool monotonic_inc)
{
  bufferlist in;
  cls_log_add_op call;
  call.entries = std::move(entries);
  encode(call, in);
  op.exec("log", "add", in);
}

void add(WriteOp& op, cls_log_entry&& entry)
{
  bufferlist in;
  cls_log_add_op call;
  call.entries.push_back(std::move(entry));
  encode(call, in);
  op.exec("log", "add", in);
}


void add(WriteOp& op, ceph::real_time timestamp, std::string_view section,
         std::string_view name, cb::list&& bl)
{
  cls_log_entry entry;

  add_prepare_entry(entry, timestamp, section, name, std::move(bl));
  add(op, std::move(entry));
}

WriteOp trim(ceph::real_time from_time, ceph::real_time to_time,
	     std::string_view from_marker, std::string_view to_marker)
{
  WriteOp op;
  cls_log_trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  call.from_marker = from_marker;
  call.to_marker = to_marker;
  cb::list in;
  encode(call, in);
  op.exec("log", "trim", in);
  return op;
}


ReadOp list(ceph::real_time from, ceph::real_time to,
	    std::string_view in_marker, int max_entries,
	    std::vector<cls_log_entry>* entries, std::string* marker,
	    bool* truncated, bs::error_code* oec)
{
  ReadOp op;
  cls_log_list_op call;
  call.from_time = from;
  call.to_time = to;
  call.marker = in_marker;
  call.max_entries = max_entries;
  bufferlist bl;
  encode(call, bl);

  op.exec("log", "list", bl,
	  [=](bs::error_code ec, cb::list bl) {
	    cls_log_list_ret ret;
	    if (!ec) try {
		auto iter = bl.cbegin();
		decode(ret, iter);
		if (entries)
		  *entries = std::move(ret.entries);
		if (marker)
		  *marker = std::move(ret.marker);
		if (truncated)
		  *truncated = std::move(ret.truncated);
	      } catch (const cb::error& err) {
		ec = err.code();
	      }
	    if (oec)
	      *oec = std::move(ec);
	  });
  return op;
}

ReadOp info(cls_log_header* header, bs::error_code* oec)
{
  ReadOp op;
  cls_log_info_op call;
  bufferlist bl;
  encode(call, bl);
  op.exec("log", "info", bl,
	  [=](bs::error_code ec, cb::list bl) {
	    cls_log_info_ret ret;
	    if (!ec) try {
		auto iter = bl.cbegin();
		decode(ret, iter);
		if (header)
		  *header = std::move(ret.header);
	      } catch (const cb::error& err) {
		ec = err.code();
	      }
	    if (oec)
	      *oec = ec;
	  });
  return op;
}

}
