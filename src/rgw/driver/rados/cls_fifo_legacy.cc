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

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <optional>
#include <string_view>

#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/random_string.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

#include "cls_fifo_legacy.h"

namespace rgw::cls::fifo {
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;

using ceph::from_error_code;

inline constexpr auto MAX_RACE_RETRIES = 10;

void create_meta(lr::ObjectWriteOperation* op,
		 std::string_view id,
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
  op->exec(fifo::op::CLASS, fifo::op::CREATE_META, in);
}

int get_meta(const DoutPrefixProvider *dpp, lr::IoCtx& ioctx, const std::string& oid,
	     std::optional<fifo::objv> objv, fifo::info* info,
	     std::uint32_t* part_header_size,
	     std::uint32_t* part_entry_overhead,
	     uint64_t tid, optional_yield y,
	     bool probe)
{
  lr::ObjectReadOperation op;
  fifo::op::get_meta gm;
  gm.version = objv;
  cb::list in;
  encode(gm, in);
  cb::list bl;

  op.exec(fifo::op::CLASS, fifo::op::GET_META, in,
	  &bl, nullptr);
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::get_meta_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (info) *info = std::move(reply.info);
      if (part_header_size) *part_header_size = reply.part_header_size;
      if (part_entry_overhead)
	*part_entry_overhead = reply.part_entry_overhead;
    } catch (const cb::error& err) {
      ldpp_dout(dpp, -1)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< " decode failed: " << err.what()
	<< " tid=" << tid << dendl;
      r = from_error_code(err.code());
    } else if (!(probe && (r == -ENOENT || r == -ENODATA))) {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " fifo::op::GET_META failed r=" << r << " tid=" << tid
      << dendl;
  }
  return r;
};

namespace {
void update_meta(lr::ObjectWriteOperation* op, const fifo::objv& objv,
		 const fifo::update& update)
{
  fifo::op::update_meta um;

  um.version = objv;
  um.tail_part_num = update.tail_part_num();
  um.head_part_num = update.head_part_num();
  um.min_push_part_num = update.min_push_part_num();
  um.max_push_part_num = update.max_push_part_num();
  um.journal_entries_add = update.journal_entries_add();
  um.journal_entries_rm = update.journal_entries_rm();

  cb::list in;
  encode(um, in);
  op->exec(fifo::op::CLASS, fifo::op::UPDATE_META, in);
}

void part_init(lr::ObjectWriteOperation* op, fifo::data_params params)
{
  fifo::op::init_part ip;

  ip.params = params;

  cb::list in;
  encode(ip, in);
  op->exec(fifo::op::CLASS, fifo::op::INIT_PART, in);
}

int push_part(const DoutPrefixProvider *dpp, lr::IoCtx& ioctx, const std::string& oid,
	      std::deque<cb::list> data_bufs, std::uint64_t tid,
	      optional_yield y)
{
  lr::ObjectWriteOperation op;
  fifo::op::push_part pp;

  op.assert_exists();

  pp.data_bufs = data_bufs;
  pp.total_len = 0;

  for (const auto& bl : data_bufs)
    pp.total_len += bl.length();

  cb::list in;
  encode(pp, in);
  auto retval = 0;
  op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in, nullptr, &retval);
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, y, lr::OPERATION_RETURNVEC);
  if (r < 0) {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " fifo::op::PUSH_PART failed r=" << r
      << " tid=" << tid << dendl;
    return r;
  }
  if (retval < 0) {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " error handling response retval=" << retval
      << " tid=" << tid << dendl;
  }
  return retval;
}

void push_part(lr::IoCtx& ioctx, const std::string& oid,
	       std::deque<cb::list> data_bufs, std::uint64_t tid,
	       lr::AioCompletion* c)
{
  lr::ObjectWriteOperation op;
  fifo::op::push_part pp;

  pp.data_bufs = data_bufs;
  pp.total_len = 0;

  for (const auto& bl : data_bufs)
    pp.total_len += bl.length();

  cb::list in;
  encode(pp, in);
  op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in);
  auto r = ioctx.aio_operate(oid, c, &op, lr::OPERATION_RETURNVEC);
  ceph_assert(r >= 0);
}

void trim_part(lr::ObjectWriteOperation* op,
	       std::uint64_t ofs, bool exclusive)
{
  fifo::op::trim_part tp;

  tp.ofs = ofs;
  tp.exclusive = exclusive;

  cb::list in;
  encode(tp, in);
  op->exec(fifo::op::CLASS, fifo::op::TRIM_PART, in);
}

int list_part(const DoutPrefixProvider *dpp, lr::IoCtx& ioctx, const std::string& oid,
	      std::uint64_t ofs, std::uint64_t max_entries,
	      std::vector<fifo::part_list_entry>* entries,
	      bool* more, bool* full_part,
	      std::uint64_t tid, optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::list_part lp;

  lp.ofs = ofs;
  lp.max_entries = max_entries;

  cb::list in;
  encode(lp, in);
  cb::list bl;
  op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in, &bl, nullptr);
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::list_part_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (entries) *entries = std::move(reply.entries);
      if (more) *more = reply.more;
      if (full_part) *full_part = reply.full_part;
    } catch (const cb::error& err) {
      ldpp_dout(dpp, -1)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< " decode failed: " << err.what()
	<< " tid=" << tid << dendl;
      r = from_error_code(err.code());
    } else if (r != -ENOENT) {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " fifo::op::LIST_PART failed r=" << r << " tid=" << tid
      << dendl;
  }
  return r;
}

struct list_entry_completion : public lr::ObjectOperationCompletion {
  CephContext* cct;
  int* r_out;
  std::vector<fifo::part_list_entry>* entries;
  bool* more;
  bool* full_part;
  std::uint64_t tid;

  list_entry_completion(CephContext* cct, int* r_out, std::vector<fifo::part_list_entry>* entries,
			bool* more, bool* full_part, std::uint64_t tid)
    : cct(cct), r_out(r_out), entries(entries), more(more),
      full_part(full_part), tid(tid) {}
  virtual ~list_entry_completion() = default;
  void handle_completion(int r, bufferlist& bl) override {
    if (r >= 0) try {
	fifo::op::list_part_reply reply;
	auto iter = bl.cbegin();
	decode(reply, iter);
	if (entries) *entries = std::move(reply.entries);
	if (more) *more = reply.more;
	if (full_part) *full_part = reply.full_part;
      } catch (const cb::error& err) {
	lderr(cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << " decode failed: " << err.what()
	  << " tid=" << tid << dendl;
	r = from_error_code(err.code());
      } else if (r < 0) {
      lderr(cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< " fifo::op::LIST_PART failed r=" << r << " tid=" << tid
	<< dendl;
    }
    if (r_out) *r_out = r;
  }
};

lr::ObjectReadOperation list_part(CephContext* cct,
				  std::uint64_t ofs,
				  std::uint64_t max_entries,
				  int* r_out,
				  std::vector<fifo::part_list_entry>* entries,
				  bool* more, bool* full_part,
				  std::uint64_t tid)
{
  lr::ObjectReadOperation op;
  fifo::op::list_part lp;

  lp.ofs = ofs;
  lp.max_entries = max_entries;

  cb::list in;
  encode(lp, in);
  op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in,
	  new list_entry_completion(cct, r_out, entries, more, full_part,
				    tid));
  return op;
}

int get_part_info(const DoutPrefixProvider *dpp, lr::IoCtx& ioctx, const std::string& oid,
		  fifo::part_header* header,
		  std::uint64_t tid, optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::get_part_info gpi;

  cb::list in;
  cb::list bl;
  encode(gpi, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in, &bl, nullptr);
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::get_part_info_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (header) *header = std::move(reply.header);
    } catch (const cb::error& err) {
      ldpp_dout(dpp, -1)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< " decode failed: " << err.what()
	<< " tid=" << tid << dendl;
      r = from_error_code(err.code());
    } else {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " fifo::op::GET_PART_INFO failed r=" << r << " tid=" << tid
      << dendl;
  }
  return r;
}

struct partinfo_completion : public lr::ObjectOperationCompletion {
  CephContext* cct;
  int* rp;
  fifo::part_header* h;
  std::uint64_t tid;
  partinfo_completion(CephContext* cct, int* rp, fifo::part_header* h,
		      std::uint64_t tid) :
    cct(cct), rp(rp), h(h), tid(tid) {
  }
  virtual ~partinfo_completion() = default;
  void handle_completion(int r, bufferlist& bl) override {
    if (r >= 0) try {
	fifo::op::get_part_info_reply reply;
	auto iter = bl.cbegin();
	decode(reply, iter);
	if (h) *h = std::move(reply.header);
      } catch (const cb::error& err) {
	r = from_error_code(err.code());
	lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " decode failed: " << err.what()
		   << " tid=" << tid << dendl;
      } else {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " fifo::op::GET_PART_INFO failed r=" << r << " tid=" << tid
		 << dendl;
    }
    if (rp) {
      *rp = r;
    }
  }
};

lr::ObjectReadOperation get_part_info(CephContext* cct,
				      fifo::part_header* header,
				      std::uint64_t tid, int* r = 0)
{
  lr::ObjectReadOperation op;
  fifo::op::get_part_info gpi;

  cb::list in;
  cb::list bl;
  encode(gpi, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in,
	  new partinfo_completion(cct, r, header, tid));
  return op;
}
}

std::optional<marker> FIFO::to_marker(std::string_view s)
{
  marker m;
  if (s.empty()) {
    m.num = info.tail_part_num;
    m.ofs = 0;
    return m;
  }

  auto pos = s.find(':');
  if (pos == s.npos) {
    return std::nullopt;
  }

  auto num = s.substr(0, pos);
  auto ofs = s.substr(pos + 1);

  auto n = ceph::parse<decltype(m.num)>(num);
  if (!n) {
    return std::nullopt;
  }
  m.num = *n;
  auto o = ceph::parse<decltype(m.ofs)>(ofs);
  if (!o) {
    return std::nullopt;
  }
  m.ofs = *o;
  return m;
}

int FIFO::apply_update(const DoutPrefixProvider *dpp,
                       fifo::info* info,
		       const fifo::objv& objv,
		       const fifo::update& update,
		       std::uint64_t tid)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::unique_lock l(m);
  if (objv != info->version) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " version mismatch, canceling: tid=" << tid << dendl;
    return -ECANCELED;
  }

  info->apply_update(update);
  return {};
}

int FIFO::_update_meta(const DoutPrefixProvider *dpp, const fifo::update& update,
		       fifo::objv version, bool* pcanceled,
		       std::uint64_t tid, optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectWriteOperation op;
  bool canceled = false;
  update_meta(&op, version, update);
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
  if (r >= 0 || r == -ECANCELED) {
    canceled = (r == -ECANCELED);
    if (!canceled) {
      r = apply_update(dpp, &info, version, update, tid);
      if (r < 0) canceled = true;
    }
    if (canceled) {
      r = read_meta(dpp, tid, y);
      canceled = r < 0 ? false : true;
    }
  }
  if (pcanceled) *pcanceled = canceled;
  if (canceled) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " canceled: tid=" << tid << dendl;
  }
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " returning error: r=" << r << " tid=" << tid << dendl;
  }
  return r;
}

struct Updater : public Completion<Updater> {
  FIFO* fifo;
  fifo::update update;
  fifo::objv version;
  bool reread = false;
  bool* pcanceled = nullptr;
  std::uint64_t tid;
  Updater(const DoutPrefixProvider *dpp, FIFO* fifo, lr::AioCompletion* super,
	  const fifo::update& update, fifo::objv version,
	  bool* pcanceled, std::uint64_t tid)
    : Completion(dpp, super), fifo(fifo), update(update), version(version),
      pcanceled(pcanceled) {}

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    if (reread)
      handle_reread(dpp, std::move(p), r);
    else
      handle_update(dpp, std::move(p), r);
  }

  void handle_update(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " handling async update_meta: tid="
			 << tid << dendl;
    if (r < 0 && r != -ECANCELED) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " update failed: r=" << r << " tid=" << tid << dendl;
      complete(std::move(p), r);
      return;
    }
    bool canceled = (r == -ECANCELED);
    if (!canceled) {
      int r = fifo->apply_update(dpp, &fifo->info, version, update, tid);
      if (r < 0) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			     << " update failed, marking canceled: r=" << r
			     << " tid=" << tid << dendl;
	canceled = true;
      }
    }
    if (canceled) {
      reread = true;
      fifo->read_meta(dpp, tid, call(std::move(p)));
      return;
    }
    if (pcanceled)
      *pcanceled = false;
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " completing: tid=" << tid << dendl;
    complete(std::move(p), 0);
  }

  void handle_reread(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " handling async read_meta: tid="
			 << tid << dendl;
    if (r < 0 && pcanceled) {
      *pcanceled = false;
    } else if (r >= 0 && pcanceled) {
      *pcanceled = true;
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " failed dispatching read_meta: r=" << r << " tid="
		       << tid << dendl;
    } else {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " completing: tid=" << tid << dendl;
    }
    complete(std::move(p), r);
  }
};

void FIFO::_update_meta(const DoutPrefixProvider *dpp, const fifo::update& update,
			fifo::objv version, bool* pcanceled,
			std::uint64_t tid, lr::AioCompletion* c)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectWriteOperation op;
  update_meta(&op, info.version, update);
  auto updater = std::make_unique<Updater>(dpp, this, c, update, version, pcanceled,
					   tid);
  auto r = ioctx.aio_operate(oid, Updater::call(std::move(updater)), &op);
  assert(r >= 0);
}

int FIFO::create_part(const DoutPrefixProvider *dpp, int64_t part_num, std::uint64_t tid,
		      optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectWriteOperation op;
  op.create(false); /* We don't need exclusivity, part_init ensures
		       we're creating from the same journal entry. */
  std::unique_lock l(m);
  part_init(&op, info.params);
  auto oid = info.part_oid(part_num);
  l.unlock();
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " part_init failed: r=" << r << " tid="
	       << tid << dendl;
  }
  return r;
}

int FIFO::remove_part(const DoutPrefixProvider *dpp, int64_t part_num, std::uint64_t tid,
		      optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectWriteOperation op;
  op.remove();
  std::unique_lock l(m);
  auto oid = info.part_oid(part_num);
  l.unlock();
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " remove failed: r=" << r << " tid="
	       << tid << dendl;
  }
  return r;
}

int FIFO::process_journal(const DoutPrefixProvider *dpp, std::uint64_t tid, optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::vector<fifo::journal_entry> processed;

  std::unique_lock l(m);
  auto tmpjournal = info.journal;
  auto new_tail = info.tail_part_num;
  auto new_head = info.head_part_num;
  auto new_max = info.max_push_part_num;
  l.unlock();

  int r = 0;
  for (auto& entry : tmpjournal) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " processing entry: entry=" << entry << " tid=" << tid
		   << dendl;
    switch (entry.op) {
      using enum fifo::journal_entry::Op;
    case create:
      r = create_part(dpp, entry.part_num, tid, y);
      if (entry.part_num > new_max) {
	new_max = entry.part_num;
      }
      break;
    case set_head:
      r = 0;
      if (entry.part_num > new_head) {
	new_head = entry.part_num;
      }
      break;
    case remove:
      r = remove_part(dpp, entry.part_num, tid, y);
      if (r == -ENOENT) r = 0;
      if (entry.part_num >= new_tail) {
	new_tail = entry.part_num + 1;
      }
      break;
    default:
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " unknown journaled op: entry=" << entry << " tid="
		 << tid << dendl;
      return -EIO;
    }

    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " processing entry failed: entry=" << entry
		 << " r=" << r << " tid=" << tid << dendl;
      return -r;
    }

    processed.push_back(std::move(entry));
  }

  // Postprocess
  bool canceled = true;

  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " postprocessing: i=" << i << " tid=" << tid << dendl;

    std::optional<int64_t> tail_part_num;
    std::optional<int64_t> head_part_num;
    std::optional<int64_t> max_part_num;

    std::unique_lock l(m);
    auto objv = info.version;
    if (new_tail > tail_part_num) tail_part_num = new_tail;
    if (new_head > info.head_part_num) head_part_num = new_head;
    if (new_max > info.max_push_part_num) max_part_num = new_max;
    l.unlock();

    if (processed.empty() &&
	!tail_part_num &&
	!max_part_num) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " nothing to update any more: i=" << i << " tid="
		     << tid << dendl;
      canceled = false;
      break;
    }
    auto u = fifo::update().tail_part_num(tail_part_num)
      .head_part_num(head_part_num).max_push_part_num(max_part_num)
      .journal_entries_rm(processed);
    r = _update_meta(dpp, u, objv, &canceled, tid, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " _update_meta failed: update=" << u
		 << " r=" << r << " tid=" << tid << dendl;
      break;
    }

    if (canceled) {
      std::vector<fifo::journal_entry> new_processed;
      std::unique_lock l(m);
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " update canceled, retrying: i=" << i << " tid="
		     << tid << dendl;
      for (auto& e : processed) {
	if (info.journal.contains(e)) {
	  new_processed.push_back(e);
	}
      }
      processed = std::move(new_processed);
    }
  }
  if (r == 0 && canceled) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " canceled too many times, giving up: tid=" << tid << dendl;
    r = -ECANCELED;
  }
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " failed, r=: " << r << " tid=" << tid << dendl;
  }
  return r;
}

int FIFO::_prepare_new_part(const DoutPrefixProvider *dpp,
			    std::int64_t new_part_num, bool is_head,
			    std::uint64_t tid, optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::unique_lock l(m);
  using enum fifo::journal_entry::Op;
  std::vector<fifo::journal_entry> jentries{{ create, new_part_num }};
  if (info.journal.contains({create, new_part_num}) &&
      (!is_head || info.journal.contains({set_head, new_part_num}))) {
    l.unlock();
    ldpp_dout(dpp, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
		  << " new part journaled, but not processed: tid="
		  << tid << dendl;
    auto r = process_journal(dpp, tid, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " process_journal failed: r=" << r << " tid=" << tid << dendl;
    }
    return r;
  }
  auto version = info.version;

  if (is_head) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " needs new head: tid=" << tid << dendl;
    jentries.push_back({ set_head, new_part_num });
  }
  l.unlock();

  int r = 0;
  bool canceled = true;
  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
    canceled = false;
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " updating metadata: i=" << i << " tid=" << tid << dendl;
    auto u = fifo::update{}.journal_entries_add(jentries);
    r = _update_meta(dpp, u, version, &canceled, tid, y);
    if (r >= 0 && canceled) {
      std::unique_lock l(m);
      version = info.version;
      auto found = (info.journal.contains({create, new_part_num}) ||
		    info.journal.contains({set_head, new_part_num}));
      if ((info.max_push_part_num >= new_part_num &&
	   info.head_part_num >= new_part_num)) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " raced, but journaled and processed: i=" << i
		       << " tid=" << tid << dendl;
	return 0;
      }
      if (found) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " raced, journaled but not processed: i=" << i
		       << " tid=" << tid << dendl;
	canceled = false;
      }
      l.unlock();
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " _update_meta failed: update=" << u << " r=" << r
		 << " tid=" << tid << dendl;
      return r;
    }
  }
  if (canceled) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " canceled too many times, giving up: tid=" << tid << dendl;
    return -ECANCELED;
  }
  r = process_journal(dpp, tid, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " process_journal failed: r=" << r << " tid=" << tid << dendl;
  }
  return r;
}

int FIFO::_prepare_new_head(const DoutPrefixProvider *dpp,
			    std::int64_t new_head_part_num,
			    std::uint64_t tid, optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " entering: tid=" << tid << dendl;
  std::unique_lock l(m);
  auto max_push_part_num = info.max_push_part_num;
  auto version = info.version;
  l.unlock();

  int r = 0;
  if (max_push_part_num < new_head_part_num) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " need new part: tid=" << tid << dendl;
    r = _prepare_new_part(dpp, new_head_part_num, true, tid, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " _prepare_new_part failed: r=" << r
		 << " tid=" << tid << dendl;
      return r;
    }
    std::unique_lock l(m);
    if (info.max_push_part_num < new_head_part_num) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " inconsistency, push part less than head part: "
		 << " tid=" << tid << dendl;
      return -EIO;
    }
    l.unlock();
    return 0;
  }

  using enum fifo::journal_entry::Op;
  fifo::journal_entry jentry;
  jentry.op = set_head;
  jentry.part_num = new_head_part_num;

  r = 0;
  bool canceled = true;
  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
    canceled = false;
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " updating metadata: i=" << i << " tid=" << tid << dendl;
    auto u = fifo::update{}.journal_entries_add({{ jentry }});
    r = _update_meta(dpp, u, version, &canceled, tid, y);
    if (r >= 0 && canceled) {
      std::unique_lock l(m);
      auto found = (info.journal.contains({create, new_head_part_num}) ||
		    info.journal.contains({set_head, new_head_part_num}));
      version = info.version;
      if ((info.head_part_num >= new_head_part_num)) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " raced, but journaled and processed: i=" << i
		       << " tid=" << tid << dendl;
	return 0;
      }
      if (found) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " raced, journaled but not processed: i=" << i
		       << " tid=" << tid << dendl;
	canceled = false;
      }
      l.unlock();
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " _update_meta failed: update=" << u << " r=" << r
		 << " tid=" << tid << dendl;
      return r;
    }
  }
  if (canceled) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " canceled too many times, giving up: tid=" << tid << dendl;
    return -ECANCELED;
  }
  r = process_journal(dpp, tid, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " process_journal failed: r=" << r << " tid=" << tid << dendl;
  }
  return r;
}

struct NewPartPreparer : public Completion<NewPartPreparer> {
  FIFO* f;
  std::vector<fifo::journal_entry> jentries;
  int i = 0;
  std::int64_t new_part_num;
  bool canceled = false;
  uint64_t tid;

  NewPartPreparer(const DoutPrefixProvider *dpp, FIFO* f, lr::AioCompletion* super,
		  std::vector<fifo::journal_entry> jentries,
		  std::int64_t new_part_num,
		  std::uint64_t tid)
    : Completion(dpp, super), f(f), jentries(std::move(jentries)),
      new_part_num(new_part_num), tid(tid) {}

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " entering: tid=" << tid << dendl;
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << " _update_meta failed:  r=" << r
		    << " tid=" << tid << dendl;
      complete(std::move(p), r);
      return;
    }

    if (canceled) {
      using enum fifo::journal_entry::Op;
      std::unique_lock l(f->m);
      auto found = (f->info.journal.contains({create, new_part_num}) ||
		    f->info.journal.contains({set_head, new_part_num}));
      auto max_push_part_num = f->info.max_push_part_num;
      auto head_part_num = f->info.head_part_num;
      auto version = f->info.version;
      l.unlock();
      if ((max_push_part_num >= new_part_num &&
	   head_part_num >= new_part_num)) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			  << " raced, but journaled and processed: i=" << i
			  << " tid=" << tid << dendl;
	complete(std::move(p), 0);
	return;
      }
      if (i >= MAX_RACE_RETRIES) {
	complete(std::move(p), -ECANCELED);
	return;
      }
      if (!found) {
	++i;
	f->_update_meta(dpp, fifo::update{}
			.journal_entries_add(jentries),
                        version, &canceled, tid, call(std::move(p)));
	return;
      } else {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			  << " raced, journaled but not processed: i=" << i
			  << " tid=" << tid << dendl;
	canceled = false;
      }
      // Fall through. We still need to process the journal.
    }
    f->process_journal(dpp, tid, super());
    return;
  }
};

void FIFO::_prepare_new_part(const DoutPrefixProvider *dpp, std::int64_t new_part_num,
			     bool is_head, std::uint64_t tid, lr::AioCompletion* c)
{
  std::unique_lock l(m);
  using enum fifo::journal_entry::Op;
  std::vector<fifo::journal_entry> jentries{{create, new_part_num}};
  if (info.journal.contains({create, new_part_num}) &&
      (!is_head || info.journal.contains({set_head, new_part_num}))) {
    l.unlock();
    ldpp_dout(dpp, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
		  << " new part journaled, but not processed: tid="
		  << tid << dendl;
    process_journal(dpp, tid, c);
    return;
  }
  auto version = info.version;

  if (is_head) {
    jentries.push_back({ set_head, new_part_num });
  }
  l.unlock();

  auto n = std::make_unique<NewPartPreparer>(dpp, this, c, jentries,
					     new_part_num, tid);
  auto np = n.get();
  _update_meta(dpp, fifo::update{}.journal_entries_add(jentries), version,
	       &np->canceled, tid, NewPartPreparer::call(std::move(n)));
}

struct NewHeadPreparer : public Completion<NewHeadPreparer> {
  FIFO* f;
  int i = 0;
  bool newpart;
  std::int64_t new_head_part_num;
  bool canceled = false;
  std::uint64_t tid;

  NewHeadPreparer(const DoutPrefixProvider *dpp, FIFO* f, lr::AioCompletion* super,
		  bool newpart, std::int64_t new_head_part_num,
		  std::uint64_t tid)
    : Completion(dpp, super), f(f), newpart(newpart),
      new_head_part_num(new_head_part_num), tid(tid) {}

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    if (newpart)
      handle_newpart(std::move(p), r);
    else
      handle_update(dpp, std::move(p), r);
  }

  void handle_newpart(Ptr&& p, int r) {
    if (r < 0) {
      lderr(f->cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << " _prepare_new_part failed: r=" << r
		    << " tid=" << tid << dendl;
      complete(std::move(p), r);
      return;
    }
    std::unique_lock l(f->m);
    if (f->info.max_push_part_num < new_head_part_num) {
      l.unlock();
      lderr(f->cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << " _prepare_new_part failed: r=" << r
		    << " tid=" << tid << dendl;
      complete(std::move(p), -EIO);
    } else {
      l.unlock();
      complete(std::move(p), 0);
    }
  }

  void handle_update(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " entering: tid=" << tid << dendl;
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << " _update_meta failed:  r=" << r
		    << " tid=" << tid << dendl;
      complete(std::move(p), r);
      return;
    }

    if (canceled) {
      using enum fifo::journal_entry::Op;
      std::unique_lock l(f->m);
      auto found = (f->info.journal.contains({create, new_head_part_num }) ||
		    f->info.journal.contains({set_head, new_head_part_num }));
      auto head_part_num = f->info.head_part_num;
      auto version = f->info.version;

      l.unlock();
      if ((head_part_num >= new_head_part_num)) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			  << " raced, but journaled and processed: i=" << i
			  << " tid=" << tid << dendl;
	complete(std::move(p), 0);
	return;
      }
      if (i >= MAX_RACE_RETRIES) {
	complete(std::move(p), -ECANCELED);
	return;
      }
      if (!found) {
	++i;
	fifo::journal_entry jentry;
	jentry.op = set_head;
	jentry.part_num = new_head_part_num;
	f->_update_meta(dpp, fifo::update{}
			.journal_entries_add({{jentry}}),
                        version, &canceled, tid, call(std::move(p)));
	return;
      } else {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			  << " raced, journaled but not processed: i=" << i
			  << " tid=" << tid << dendl;
	canceled = false;
      }
      // Fall through. We still need to process the journal.
    }
    f->process_journal(dpp, tid, super());
    return;
  }
};

void FIFO::_prepare_new_head(const DoutPrefixProvider *dpp, std::int64_t new_head_part_num,
			     std::uint64_t tid, lr::AioCompletion* c)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::unique_lock l(m);
  auto max_push_part_num = info.max_push_part_num;
  auto version = info.version;
  l.unlock();

  if (max_push_part_num < new_head_part_num) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " need new part: tid=" << tid << dendl;
    auto n = std::make_unique<NewHeadPreparer>(dpp, this, c, true, new_head_part_num,
					       tid);
    _prepare_new_part(dpp, new_head_part_num, true, tid,
		      NewHeadPreparer::call(std::move(n)));
  } else {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " updating head: tid=" << tid << dendl;
    auto n = std::make_unique<NewHeadPreparer>(dpp, this, c, false, new_head_part_num,
					       tid);
    auto np = n.get();
    using enum fifo::journal_entry::Op;
    fifo::journal_entry jentry;
    jentry.op = set_head;
    jentry.part_num = new_head_part_num;
    _update_meta(dpp, fifo::update{}.journal_entries_add({{jentry}}), version,
		 &np->canceled, tid, NewHeadPreparer::call(std::move(n)));
  }
}

int FIFO::push_entries(const DoutPrefixProvider *dpp, const std::deque<cb::list>& data_bufs,
		       std::uint64_t tid, optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::unique_lock l(m);
  auto head_part_num = info.head_part_num;
  const auto part_oid = info.part_oid(head_part_num);
  l.unlock();

  auto r = push_part(dpp, ioctx, part_oid, data_bufs, tid, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " push_part failed: r=" << r << " tid=" << tid << dendl;
  }
  return r;
}

void FIFO::push_entries(const std::deque<cb::list>& data_bufs,
			std::uint64_t tid, lr::AioCompletion* c)
{
  std::unique_lock l(m);
  auto head_part_num = info.head_part_num;
  const auto part_oid = info.part_oid(head_part_num);
  l.unlock();

  push_part(ioctx, part_oid, data_bufs, tid, c);
}

int FIFO::trim_part(const DoutPrefixProvider *dpp, int64_t part_num, uint64_t ofs,
		    bool exclusive, std::uint64_t tid,
		    optional_yield y)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectWriteOperation op;
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  rgw::cls::fifo::trim_part(&op, ofs, exclusive);
  auto r = rgw_rados_operate(dpp, ioctx, part_oid, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " trim_part failed: r=" << r << " tid=" << tid << dendl;
  }
  return 0;
}

void FIFO::trim_part(const DoutPrefixProvider *dpp, int64_t part_num, uint64_t ofs,
		     bool exclusive, std::uint64_t tid,
		     lr::AioCompletion* c)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectWriteOperation op;
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  rgw::cls::fifo::trim_part(&op, ofs, exclusive);
  auto r = ioctx.aio_operate(part_oid, c, &op);
  ceph_assert(r >= 0);
}

int FIFO::open(const DoutPrefixProvider *dpp, lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
	       optional_yield y, std::optional<fifo::objv> objv,
	       bool probe)
{
  ldpp_dout(dpp, 20)
    << __PRETTY_FUNCTION__ << ":" << __LINE__
    << " entering" << dendl;
  fifo::info info;
  std::uint32_t size;
  std::uint32_t over;
  int r = get_meta(dpp, ioctx, std::move(oid), objv, &info, &size, &over, 0, y,
		   probe);
  if (r < 0) {
    if (!(probe && (r == -ENOENT || r == -ENODATA))) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " get_meta failed: r=" << r << dendl;
    }
    return r;
  }
  std::unique_ptr<FIFO> f(new FIFO(std::move(ioctx), oid));
  f->info = info;
  f->part_header_size = size;
  f->part_entry_overhead = over;
  // If there are journal entries, process them, in case
  // someone crashed mid-transaction.
  if (!info.journal.empty()) {
    ldpp_dout(dpp, 20)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " processing leftover journal" << dendl;
    r = f->process_journal(dpp, 0, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " process_journal failed: r=" << r << dendl;
      return r;
    }
  }
  *fifo = std::move(f);
  return 0;
}

int FIFO::create(const DoutPrefixProvider *dpp, lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
		 optional_yield y, std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive, std::uint64_t max_part_size,
		 std::uint64_t max_entry_size)
{
  ldpp_dout(dpp, 20)
    << __PRETTY_FUNCTION__ << ":" << __LINE__
    << " entering" << dendl;
  lr::ObjectWriteOperation op;
  create_meta(&op, oid, objv, oid_prefix, exclusive, max_part_size,
	      max_entry_size);
  auto r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " create_meta failed: r=" << r << dendl;
    return r;
  }
  r = open(dpp, std::move(ioctx), std::move(oid), fifo, y, objv);
  return r;
}

int FIFO::read_meta(const DoutPrefixProvider *dpp, std::uint64_t tid, optional_yield y) {
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  fifo::info _info;
  std::uint32_t _phs;
  std::uint32_t _peo;

  auto r = get_meta(dpp, ioctx, oid, std::nullopt, &_info, &_phs, &_peo, tid, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " get_meta failed: r=" << r << " tid=" << tid << dendl;
    return r;
  }
  std::unique_lock l(m);
  // We have a newer version already!
  if (_info.version.same_or_later(this->info.version)) {
    info = std::move(_info);
    part_header_size = _phs;
    part_entry_overhead = _peo;
  }
  return 0;
}

int FIFO::read_meta(const DoutPrefixProvider *dpp, optional_yield y) {
  std::unique_lock l(m);
  auto tid = ++next_tid;
  l.unlock();
  return read_meta(dpp, tid, y);
}

struct Reader : public Completion<Reader> {
  FIFO* fifo;
  cb::list bl;
  std::uint64_t tid;
  Reader(const DoutPrefixProvider *dpp, FIFO* fifo, lr::AioCompletion* super, std::uint64_t tid)
    : Completion(dpp, super), fifo(fifo), tid(tid) {}

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " entering: tid=" << tid << dendl;
    if (r >= 0) try {
	fifo::op::get_meta_reply reply;
	auto iter = bl.cbegin();
	decode(reply, iter);
	std::unique_lock l(fifo->m);
	if (reply.info.version.same_or_later(fifo->info.version)) {
	  fifo->info = std::move(reply.info);
	  fifo->part_header_size = reply.part_header_size;
	  fifo->part_entry_overhead = reply.part_entry_overhead;
	}
      } catch (const cb::error& err) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " failed to decode response err=" << err.what()
		   << " tid=" << tid << dendl;
	r = from_error_code(err.code());
      } else {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " read_meta failed r=" << r
		 << " tid=" << tid << dendl;
    }
    complete(std::move(p), r);
  }
};

void FIFO::read_meta(const DoutPrefixProvider *dpp, std::uint64_t tid, lr::AioCompletion* c)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  lr::ObjectReadOperation op;
  fifo::op::get_meta gm;
  cb::list in;
  encode(gm, in);
  auto reader = std::make_unique<Reader>(dpp, this, c, tid);
  auto rp = reader.get();
  auto r = ioctx.aio_exec(oid, Reader::call(std::move(reader)), fifo::op::CLASS,
			  fifo::op::GET_META, in, &rp->bl);
  assert(r >= 0);
}

const fifo::info& FIFO::meta() const {
  return info;
}

std::pair<std::uint32_t, std::uint32_t> FIFO::get_part_layout_info() const {
  return {part_header_size, part_entry_overhead};
}

int FIFO::push(const DoutPrefixProvider *dpp, const cb::list& bl, optional_yield y) {
  return push(dpp, std::vector{ bl }, y);
}

void FIFO::push(const DoutPrefixProvider *dpp, const cb::list& bl, lr::AioCompletion* c) {
  push(dpp, std::vector{ bl }, c);
}

int FIFO::push(const DoutPrefixProvider *dpp, const std::vector<cb::list>& data_bufs, optional_yield y)
{
  std::unique_lock l(m);
  auto tid = ++next_tid;
  auto max_entry_size = info.params.max_entry_size;
  auto need_new_head = info.need_new_head();
  auto head_part_num = info.head_part_num;
  l.unlock();
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  if (data_bufs.empty()) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " empty push, returning success tid=" << tid << dendl;
    return 0;
  }

  // Validate sizes
  for (const auto& bl : data_bufs) {
    if (bl.length() > max_entry_size) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entry bigger than max_entry_size tid=" << tid << dendl;
      return -E2BIG;
    }
  }

  int r = 0;
  if (need_new_head) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " need new head tid=" << tid << dendl;
    r = _prepare_new_head(dpp, head_part_num + 1, tid, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " _prepare_new_head failed: r=" << r
		 << " tid=" << tid << dendl;
      return r;
    }
  }

  std::deque<cb::list> remaining(data_bufs.begin(), data_bufs.end());
  std::deque<cb::list> batch;

  uint64_t batch_len = 0;
  auto retries = 0;
  bool canceled = true;
  while ((!remaining.empty() || !batch.empty()) &&
	 (retries <= MAX_RACE_RETRIES)) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " preparing push: remaining=" << remaining.size()
		   << " batch=" << batch.size() << " retries=" << retries
		   << " tid=" << tid << dendl;
    std::unique_lock l(m);
    head_part_num = info.head_part_num;
    auto max_part_size = info.params.max_part_size;
    auto overhead = part_entry_overhead;
    l.unlock();

    while (!remaining.empty() &&
	   (remaining.front().length() + batch_len <= max_part_size)) {
      /* We can send entries with data_len up to max_entry_size,
	 however, we want to also account the overhead when
	 dealing with multiple entries. Previous check doesn't
	 account for overhead on purpose. */
      batch_len += remaining.front().length() + overhead;
      batch.push_back(std::move(remaining.front()));
      remaining.pop_front();
    }
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " prepared push: remaining=" << remaining.size()
		   << " batch=" << batch.size() << " retries=" << retries
		   << " batch_len=" << batch_len
		   << " tid=" << tid << dendl;

    auto r = push_entries(dpp, batch, tid, y);
    if (r == -ERANGE) {
      canceled = true;
      ++retries;
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " need new head tid=" << tid << dendl;
      r = _prepare_new_head(dpp, head_part_num + 1, tid, y);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " prepare_new_head failed: r=" << r
		   << " tid=" << tid << dendl;
	return r;
      }
      r = 0;
      continue;
    }
    if (r == -ENOENT) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " racing client trimmed part, rereading metadata "
			 << "tid=" << tid << dendl;
      canceled = true;
      ++retries;
      r = read_meta(dpp, y);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " read_meta failed: r=" << r
		   << " tid=" << tid << dendl;
	return r;
      }
      r = 0;
      continue;
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " push_entries failed: r=" << r
		 << " tid=" << tid << dendl;
      return r;
    }
    // Made forward progress!
    canceled = false;
    retries = 0;
    batch_len = 0;
    if (r == ssize(batch)) {
      batch.clear();
    } else  {
      batch.erase(batch.begin(), batch.begin() + r);
      for (const auto& b : batch) {
	batch_len +=  b.length() + part_entry_overhead;
      }
    }
  }
  if (canceled) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " canceled too many times, giving up: tid=" << tid << dendl;
    return -ECANCELED;
  }
  return 0;
}

struct Pusher : public Completion<Pusher> {
  FIFO* f;
  std::deque<cb::list> remaining;
  std::deque<cb::list> batch;
  int i = 0;
  std::int64_t head_part_num;
  std::uint64_t tid;
  enum { pushing, new_heading, meta_reading } state = pushing;

  void prep_then_push(const DoutPrefixProvider *dpp, Ptr&& p, const unsigned successes) {
    std::unique_lock l(f->m);
    auto max_part_size = f->info.params.max_part_size;
    auto part_entry_overhead = f->part_entry_overhead;
    head_part_num = f->info.head_part_num;
    l.unlock();

    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " preparing push: remaining=" << remaining.size()
		      << " batch=" << batch.size() << " i=" << i
		      << " tid=" << tid << dendl;

    uint64_t batch_len = 0;
    if (successes > 0) {
      if (successes == batch.size()) {
	batch.clear();
      } else  {
	batch.erase(batch.begin(), batch.begin() + successes);
	for (const auto& b : batch) {
	  batch_len +=  b.length() + part_entry_overhead;
	}
      }
    }

    if (batch.empty() && remaining.empty()) {
      complete(std::move(p), 0);
      return;
    }

    while (!remaining.empty() &&
	   (remaining.front().length() + batch_len <= max_part_size)) {

      /* We can send entries with data_len up to max_entry_size,
	 however, we want to also account the overhead when
	 dealing with multiple entries. Previous check doesn't
	 account for overhead on purpose. */
      batch_len += remaining.front().length() + part_entry_overhead;
      batch.push_back(std::move(remaining.front()));
      remaining.pop_front();
    }
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " prepared push: remaining=" << remaining.size()
		      << " batch=" << batch.size() << " i=" << i
		      << " batch_len=" << batch_len
		      << " tid=" << tid << dendl;
    push(std::move(p));
  }

  void push(Ptr&& p) {
    f->push_entries(batch, tid, call(std::move(p)));
  }

  void new_head(const DoutPrefixProvider *dpp, Ptr&& p) {
    state = new_heading;
    f->_prepare_new_head(dpp, head_part_num + 1, tid, call(std::move(p)));
  }

  void read_meta(const DoutPrefixProvider *dpp, Ptr&& p) {
    ++i;
    state = meta_reading;
    f->read_meta(dpp, tid, call(std::move(p)));
  }

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    switch (state) {
    case pushing:
      if (r == -ERANGE) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " need new head tid=" << tid << dendl;
	new_head(dpp, std::move(p));
	return;
      }
      if (r == -ENOENT) {
	if (i > MAX_RACE_RETRIES) {
	  ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			     << " racing client deleted part, but we're out"
			     << " of retries: tid=" << tid << dendl;
	  complete(std::move(p), r);
	}
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " racing client deleted part: tid=" << tid << dendl;
	read_meta(dpp, std::move(p));
	return;
      }
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " push_entries failed: r=" << r
		      << " tid=" << tid << dendl;
	complete(std::move(p), r);
	return;
      }
      i = 0; // We've made forward progress, so reset the race counter!
      prep_then_push(dpp, std::move(p), r);
      break;

    case new_heading:
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " prepare_new_head failed: r=" << r
		      << " tid=" << tid << dendl;
	complete(std::move(p), r);
	return;
      }
      state = pushing;
      handle_new_head(dpp, std::move(p), r);
      break;

    case meta_reading:
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " read_meta failed: r=" << r
		      << " tid=" << tid << dendl;
	complete(std::move(p), r);
	return;
      }
      state = pushing;
      prep_then_push(dpp, std::move(p), r);
      break;
    }
  }

  void handle_new_head(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    if (r == -ECANCELED) {
      if (p->i == MAX_RACE_RETRIES) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " canceled too many times, giving up: tid=" << tid << dendl;
	complete(std::move(p), -ECANCELED);
	return;
      }
      ++p->i;
    } else if (r) {
      complete(std::move(p), r);
      return;
    }

    if (p->batch.empty()) {
      prep_then_push(dpp, std::move(p), 0);
      return;
    } else {
      push(std::move(p));
      return;
    }
  }

  Pusher(const DoutPrefixProvider *dpp, FIFO* f, std::deque<cb::list>&& remaining,
	 std::int64_t head_part_num, std::uint64_t tid,
	 lr::AioCompletion* super)
    : Completion(dpp, super), f(f), remaining(std::move(remaining)),
      head_part_num(head_part_num), tid(tid) {}
};

void FIFO::push(const DoutPrefixProvider *dpp, const std::vector<cb::list>& data_bufs,
		lr::AioCompletion* c)
{
  std::unique_lock l(m);
  auto tid = ++next_tid;
  auto max_entry_size = info.params.max_entry_size;
  auto need_new_head = info.need_new_head();
  auto head_part_num = info.head_part_num;
  l.unlock();
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  auto p = std::make_unique<Pusher>(dpp, this, std::deque<cb::list>(data_bufs.begin(), data_bufs.end()),
				    head_part_num, tid, c);
  // Validate sizes
  for (const auto& bl : data_bufs) {
    if (bl.length() > max_entry_size) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entry bigger than max_entry_size tid=" << tid << dendl;
      Pusher::complete(std::move(p), -E2BIG);
      return;
    }
  }

  if (data_bufs.empty() ) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " empty push, returning success tid=" << tid << dendl;
    Pusher::complete(std::move(p), 0);
    return;
  }

  if (need_new_head) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " need new head tid=" << tid << dendl;
    p->new_head(dpp, std::move(p));
  } else {
    p->prep_then_push(dpp, std::move(p), 0);
  }
}

int FIFO::list(const DoutPrefixProvider *dpp, int max_entries,
	       std::optional<std::string_view> markstr,
	       std::vector<list_entry>* presult, bool* pmore,
	       optional_yield y)
{
  std::unique_lock l(m);
  auto tid = ++next_tid;
  std::int64_t part_num = info.tail_part_num;
  l.unlock();
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::uint64_t ofs = 0;
  if (markstr) {
    auto marker = to_marker(*markstr);
    if (!marker) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " invalid marker string: " << markstr
		 << " tid= "<< tid << dendl;
      return -EINVAL;
    }
    part_num = marker->num;
    ofs = marker->ofs;
  }

  std::vector<list_entry> result;
  result.reserve(max_entries);
  bool more = false;

  std::vector<fifo::part_list_entry> entries;
  int r = 0;
  while (max_entries > 0) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " max_entries=" << max_entries << " tid=" << tid << dendl;
    bool part_more = false;
    bool part_full = false;

    std::unique_lock l(m);
    auto part_oid = info.part_oid(part_num);
    l.unlock();

    r = list_part(dpp, ioctx, part_oid, ofs, max_entries, &entries,
		  &part_more, &part_full, tid, y);
    if (r == -ENOENT) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " missing part, rereading metadata"
		     << " tid= "<< tid << dendl;
      r = read_meta(dpp, tid, y);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " read_meta failed: r=" << r
		   << " tid= "<< tid << dendl;
	return r;
      }
      if (part_num < info.tail_part_num) {
	/* raced with trim? restart */
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " raced with trim, restarting: tid=" << tid << dendl;
	max_entries += result.size();
	result.clear();
	std::unique_lock l(m);
	part_num = info.tail_part_num;
	l.unlock();
	ofs = 0;
	continue;
      }
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " assuming part was not written yet, so end of data: "
		     << "tid=" << tid << dendl;
      more = false;
      r = 0;
      break;
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " list_entries failed: r=" << r
		 << " tid= "<< tid << dendl;
      return r;
    }
    more = part_full || part_more;
    for (auto& entry : entries) {
      list_entry e;
      e.data = std::move(entry.data);
      e.marker = marker{part_num, entry.ofs}.to_string();
      e.mtime = entry.mtime;
      result.push_back(std::move(e));
      --max_entries;
      if (max_entries == 0)
	break;
    }
    entries.clear();
    if (max_entries > 0 &&
	part_more) {
    }

    if (!part_full) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " head part is not full, so we can assume we're done: "
		     << "tid=" << tid << dendl;
      break;
    }
    if (!part_more) {
      ++part_num;
      ofs = 0;
    }
  }
  if (presult)
    *presult = std::move(result);
  if (pmore)
    *pmore =  more;
  return 0;
}

int FIFO::trim(const DoutPrefixProvider *dpp, std::string_view markstr, bool exclusive, optional_yield y)
{
  bool overshoot = false;
  auto marker = to_marker(markstr);
  if (!marker) {
    return -EINVAL;
  }
  auto part_num = marker->num;
  auto ofs = marker->ofs;
  std::unique_lock l(m);
  auto tid = ++next_tid;
  auto hn = info.head_part_num;
  const auto max_part_size = info.params.max_part_size;
  if (part_num > hn) {
    l.unlock();
    auto r = read_meta(dpp, tid, y);
    if (r < 0) {
      return r;
    }
    l.lock();
    auto hn = info.head_part_num;
    if (part_num > hn) {
      overshoot = true;
      part_num = hn;
      ofs = max_part_size;
    }
  }
  if (part_num < info.tail_part_num) {
    return -ENODATA;
  }
  auto pn = info.tail_part_num;
  l.unlock();
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;

  int r = 0;
  while (pn < part_num) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " pn=" << pn << " tid=" << tid << dendl;
    std::unique_lock l(m);
    l.unlock();
    r = trim_part(dpp, pn, max_part_size, false, tid, y);
    if (r < 0 && r == -ENOENT) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " trim_part failed: r=" << r
		 << " tid= "<< tid << dendl;
      return r;
    }
    ++pn;
  }
  r = trim_part(dpp, part_num, ofs, exclusive, tid, y);
  if (r < 0 && r != -ENOENT) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " trim_part failed: r=" << r
	       << " tid= "<< tid << dendl;
    return r;
  }

  l.lock();
  auto tail_part_num = info.tail_part_num;
  auto objv = info.version;
  l.unlock();
  bool canceled = tail_part_num < part_num;
  int retries = 0;
  while ((tail_part_num < part_num) &&
	 canceled &&
	 (retries <= MAX_RACE_RETRIES)) {
    r = _update_meta(dpp, fifo::update{}.tail_part_num(part_num), objv, &canceled,
		     tid, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " _update_meta failed: r=" << r
		 << " tid= "<< tid << dendl;
      return r;
    }
    if (canceled) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " canceled: retries=" << retries
		     << " tid=" << tid << dendl;
      l.lock();
      tail_part_num = info.tail_part_num;
      objv = info.version;
      l.unlock();
      ++retries;
    }
  }
  if (canceled) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " canceled too many times, giving up: tid=" << tid << dendl;
    return -EIO;
  }
  return overshoot ? -ENODATA : 0;
}

struct Trimmer : public Completion<Trimmer> {
  FIFO* fifo;
  std::int64_t part_num;
  std::uint64_t ofs;
  std::int64_t pn;
  bool exclusive;
  std::uint64_t tid;
  bool update = false;
  bool reread = false;
  bool canceled = false;
  bool overshoot = false;
  int retries = 0;

  Trimmer(const DoutPrefixProvider *dpp, FIFO* fifo, std::int64_t part_num, std::uint64_t ofs, std::int64_t pn,
	  bool exclusive, lr::AioCompletion* super, std::uint64_t tid)
    : Completion(dpp, super), fifo(fifo), part_num(part_num), ofs(ofs), pn(pn),
      exclusive(exclusive), tid(tid) {}

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " entering: tid=" << tid << dendl;

    if (reread) {
      reread = false;
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " read_meta failed: r="
		   << r << " tid=" << tid << dendl;
	complete(std::move(p), r);
	return;
      }
      std::unique_lock l(fifo->m);
      auto hn = fifo->info.head_part_num;
      const auto max_part_size = fifo->info.params.max_part_size;
      const auto tail_part_num = fifo->info.tail_part_num;
      l.unlock();
      if (part_num > hn) {
	part_num = hn;
	ofs = max_part_size;
	overshoot = true;
      }
      if (part_num < tail_part_num) {
	complete(std::move(p), -ENODATA);
	return;
      }
      pn = tail_part_num;
      if (pn < part_num) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " pn=" << pn << " tid=" << tid << dendl;
	fifo->trim_part(dpp, pn++, max_part_size, false, tid,
			call(std::move(p)));
      } else {
	update = true;
	canceled = tail_part_num < part_num;
	fifo->trim_part(dpp, part_num, ofs, exclusive, tid, call(std::move(p)));
      }
      return;
    }

    if (r == -ENOENT) {
      r = 0;
    }

    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << (update ? " update_meta " : " trim ") << "failed: r="
		 << r << " tid=" << tid << dendl;
      complete(std::move(p), r);
      return;
    }

    if (!update) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " handling preceding trim callback: tid=" << tid << dendl;
      retries = 0;
      if (pn < part_num) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " pn=" << pn << " tid=" << tid << dendl;
	std::unique_lock l(fifo->m);
	const auto max_part_size = fifo->info.params.max_part_size;
	l.unlock();
	fifo->trim_part(dpp, pn++, max_part_size, false, tid,
			call(std::move(p)));
	return;
      }

      std::unique_lock l(fifo->m);
      const auto tail_part_num = fifo->info.tail_part_num;
      l.unlock();
      update = true;
      canceled = tail_part_num < part_num;
      fifo->trim_part(dpp, part_num, ofs, exclusive, tid, call(std::move(p)));
      return;
    }

    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " handling update-needed callback: tid=" << tid << dendl;
    std::unique_lock l(fifo->m);
    auto tail_part_num = fifo->info.tail_part_num;
    auto objv = fifo->info.version;
    l.unlock();
    if ((tail_part_num < part_num) &&
	canceled) {
      if (retries > MAX_RACE_RETRIES) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " canceled too many times, giving up: tid=" << tid << dendl;
	complete(std::move(p), -EIO);
	return;
      }
      ++retries;
      fifo->_update_meta(dpp, fifo::update{}
			 .tail_part_num(part_num), objv, &canceled,
                         tid, call(std::move(p)));
    } else {
      complete(std::move(p), overshoot ? -ENODATA : 0);
    }
  }
};

void FIFO::trim(const DoutPrefixProvider *dpp, std::string_view markstr, bool exclusive,
		lr::AioCompletion* c) {
  auto marker = to_marker(markstr);
  auto realmark = marker.value_or(::rgw::cls::fifo::marker{});
  std::unique_lock l(m);
  const auto hn = info.head_part_num;
  const auto max_part_size = info.params.max_part_size;
  const auto pn = info.tail_part_num;
  const auto part_oid = info.part_oid(pn);
  auto tid = ++next_tid;
  l.unlock();
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  auto trimmer = std::make_unique<Trimmer>(dpp, this, realmark.num, realmark.ofs,
					   pn, exclusive, c, tid);
  if (!marker) {
    Trimmer::complete(std::move(trimmer), -EINVAL);
    return;
  }
  ++trimmer->pn;
  auto ofs = marker->ofs;
  if (marker->num > hn) {
    trimmer->reread = true;
    read_meta(dpp, tid, Trimmer::call(std::move(trimmer)));
    return;
  }
  if (pn < marker->num) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " pn=" << pn << " tid=" << tid << dendl;
    ofs = max_part_size;
  } else {
    trimmer->update = true;
  }
  trim_part(dpp, pn, ofs, exclusive, tid, Trimmer::call(std::move(trimmer)));
}

int FIFO::get_part_info(const DoutPrefixProvider *dpp, int64_t part_num,
			fifo::part_header* header,
			optional_yield y)
{
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  auto tid = ++next_tid;
  l.unlock();
  auto r = rgw::cls::fifo::get_part_info(dpp, ioctx, part_oid, header, tid, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " get_part_info failed: r="
	       << r << " tid=" << tid << dendl;
  }
  return r;
}

void FIFO::get_part_info(int64_t part_num,
			 fifo::part_header* header,
			 lr::AioCompletion* c)
{
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  auto tid = ++next_tid;
  l.unlock();
  auto op = rgw::cls::fifo::get_part_info(cct, header, tid);
  auto r = ioctx.aio_operate(part_oid, c, &op, nullptr);
  ceph_assert(r >= 0);
}

struct InfoGetter : Completion<InfoGetter> {
  FIFO* fifo;
  fifo::part_header header;
  fu2::function<void(int r, fifo::part_header&&)> f;
  std::uint64_t tid;
  bool headerread = false;

  InfoGetter(const DoutPrefixProvider *dpp, FIFO* fifo, fu2::function<void(int r, fifo::part_header&&)> f,
	     std::uint64_t tid, lr::AioCompletion* super)
    : Completion(dpp, super), fifo(fifo), f(std::move(f)), tid(tid) {}
  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    if (!headerread) {
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " read_meta failed: r="
			 << r << " tid=" << tid << dendl;
	if (f)
	  f(r, {});
	complete(std::move(p), r);
	return;
      }

      auto info = fifo->meta();
      auto hpn = info.head_part_num;
      if (hpn < 0) {
	ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			     << " no head, returning empty partinfo r="
			     << r << " tid=" << tid << dendl;
	if (f)
	  f(0, {});
	complete(std::move(p), r);
	return;
      }
      headerread = true;
      auto op = rgw::cls::fifo::get_part_info(fifo->cct, &header, tid);
      std::unique_lock l(fifo->m);
      auto oid = fifo->info.part_oid(hpn);
      l.unlock();
      r = fifo->ioctx.aio_operate(oid, call(std::move(p)), &op,
				  nullptr);
      ceph_assert(r >= 0);
      return;
    }

    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " get_part_info failed: r="
		       << r << " tid=" << tid << dendl;
    }

    if (f)
      f(r, std::move(header));
    complete(std::move(p), r);
    return;
  }
};

void FIFO::get_head_info(const DoutPrefixProvider *dpp, fu2::unique_function<void(int r,
						   fifo::part_header&&)> f,
			 lr::AioCompletion* c)
{
  std::unique_lock l(m);
  auto tid = ++next_tid;
  l.unlock();
  auto ig = std::make_unique<InfoGetter>(dpp, this, std::move(f), tid, c);
  read_meta(dpp, tid, InfoGetter::call(std::move(ig)));
}

struct JournalProcessor : public Completion<JournalProcessor> {
private:
  FIFO* const fifo;

  std::vector<fifo::journal_entry> processed;
  decltype(fifo->info.journal) journal;
  decltype(journal)::iterator iter;
  std::int64_t new_tail;
  std::int64_t new_head;
  std::int64_t new_max;
  int race_retries = 0;
  bool first_pp = true;
  bool canceled = false;
  std::uint64_t tid;

  enum {
    entry_callback,
    pp_callback,
  } state;

  void create_part(const DoutPrefixProvider *dpp, Ptr&& p, int64_t part_num) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    state = entry_callback;
    lr::ObjectWriteOperation op;
    op.create(false); /* We don't need exclusivity, part_init ensures
			 we're creating from the  same journal entry. */
    std::unique_lock l(fifo->m);
    part_init(&op, fifo->info.params);
    auto oid = fifo->info.part_oid(part_num);
    l.unlock();
    auto r = fifo->ioctx.aio_operate(oid, call(std::move(p)), &op);
    ceph_assert(r >= 0);
    return;
  }

  void remove_part(const DoutPrefixProvider *dpp, Ptr&& p, int64_t part_num) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    state = entry_callback;
    lr::ObjectWriteOperation op;
    op.remove();
    std::unique_lock l(fifo->m);
    auto oid = fifo->info.part_oid(part_num);
    l.unlock();
    auto r = fifo->ioctx.aio_operate(oid, call(std::move(p)), &op);
    ceph_assert(r >= 0);
    return;
  }

  void finish_je(const DoutPrefixProvider *dpp, Ptr&& p, int r,
		 const fifo::journal_entry& entry) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;

    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " finishing entry: entry=" << entry
			 << " tid=" << tid << dendl;

    using enum fifo::journal_entry::Op;
    if (entry.op == remove && r == -ENOENT)
      r = 0;

    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " processing entry failed: entry=" << entry
		       << " r=" << r << " tid=" << tid << dendl;
      complete(std::move(p), r);
      return;
    } else {
      switch (entry.op) {
      case unknown:
      case set_head:
	// Can't happen. Filtered out in process.
	complete(std::move(p), -EIO);
	return;

      case create:
	if (entry.part_num > new_max) {
	  new_max = entry.part_num;
	}
	break;
      case remove:
	if (entry.part_num >= new_tail) {
	  new_tail = entry.part_num + 1;
	}
	break;
      }
      processed.push_back(entry);
    }
    ++iter;
    process(dpp, std::move(p));
  }

  void postprocess(const DoutPrefixProvider *dpp, Ptr&& p) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    if (processed.empty()) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " nothing to update any more: race_retries="
			   << race_retries << " tid=" << tid << dendl;
      complete(std::move(p), 0);
      return;
    }
    pp_run(dpp, std::move(p), 0, false);
  }

public:

  JournalProcessor(const DoutPrefixProvider *dpp, FIFO* fifo, std::uint64_t tid, lr::AioCompletion* super)
    : Completion(dpp, super), fifo(fifo), tid(tid) {
    std::unique_lock l(fifo->m);
    journal = fifo->info.journal;
    iter = journal.begin();
    new_tail = fifo->info.tail_part_num;
    new_head = fifo->info.head_part_num;
    new_max = fifo->info.max_push_part_num;
  }

  void pp_run(const DoutPrefixProvider *dpp, Ptr&& p, int r, bool canceled) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    std::optional<int64_t> tail_part_num;
    std::optional<int64_t> head_part_num;
    std::optional<int64_t> max_part_num;

    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " failed, r=: " << r << " tid=" << tid << dendl;
      complete(std::move(p), r);
    }


    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " postprocessing: race_retries="
			 << race_retries << " tid=" << tid << dendl;

    if (!first_pp && r == 0 && !canceled) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " nothing to update any more: race_retries="
			   << race_retries << " tid=" << tid << dendl;
      complete(std::move(p), 0);
      return;
    }

    first_pp = false;

    if (canceled) {
      if (race_retries >= MAX_RACE_RETRIES) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " canceled too many times, giving up: tid="
			 << tid << dendl;
	complete(std::move(p), -ECANCELED);
	return;
      }
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " update canceled, retrying: race_retries="
			   << race_retries << " tid=" << tid << dendl;

      ++race_retries;

      std::vector<fifo::journal_entry> new_processed;
      std::unique_lock l(fifo->m);
      for (auto& e : processed) {
	if (fifo->info.journal.contains(e)) {
	  new_processed.push_back(e);
	}
      }
      processed = std::move(new_processed);
    }

    std::unique_lock l(fifo->m);
    auto objv = fifo->info.version;
    if (new_tail > fifo->info.tail_part_num) {
      tail_part_num = new_tail;
    }

    if (new_head > fifo->info.head_part_num) {
      head_part_num = new_head;
    }

    if (new_max > fifo->info.max_push_part_num) {
      max_part_num = new_max;
    }
    l.unlock();

    if (processed.empty() &&
	!tail_part_num &&
	!max_part_num) {
      /* nothing to update anymore */
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " nothing to update any more: race_retries="
			   << race_retries << " tid=" << tid << dendl;
      complete(std::move(p), 0);
      return;
    }
    state = pp_callback;
    fifo->_update_meta(dpp, fifo::update{}
		       .tail_part_num(tail_part_num)
		       .head_part_num(head_part_num)
		       .max_push_part_num(max_part_num)
		       .journal_entries_rm(processed),
                       objv, &this->canceled, tid, call(std::move(p)));
    return;
  }

  JournalProcessor(const JournalProcessor&) = delete;
  JournalProcessor& operator =(const JournalProcessor&) = delete;
  JournalProcessor(JournalProcessor&&) = delete;
  JournalProcessor& operator =(JournalProcessor&&) = delete;

  void process(const DoutPrefixProvider *dpp, Ptr&& p) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    while (iter != journal.end()) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			   << " processing entry: entry=" << *iter
			   << " tid=" << tid << dendl;
      const auto entry = *iter;
      switch (entry.op) {
	using enum fifo::journal_entry::Op;
      case create:
	create_part(dpp, std::move(p), entry.part_num);
	return;
      case set_head:
	if (entry.part_num > new_head) {
	  new_head = entry.part_num;
	}
	processed.push_back(entry);
	++iter;
	continue;
      case remove:
	remove_part(dpp, std::move(p), entry.part_num);
	return;
      default:
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " unknown journaled op: entry=" << entry << " tid="
			 << tid << dendl;
	complete(std::move(p), -EIO);
	return;
      }
    }
    postprocess(dpp, std::move(p));
    return;
  }

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " entering: tid=" << tid << dendl;
    switch (state) {
    case entry_callback:
      finish_je(dpp, std::move(p), r, *iter);
      return;
    case pp_callback:
      auto c = canceled;
      canceled = false;
      pp_run(dpp, std::move(p), r, c);
      return;
    }

    abort();
  }

};

void FIFO::process_journal(const DoutPrefixProvider *dpp, std::uint64_t tid, lr::AioCompletion* c) {
  auto p = std::make_unique<JournalProcessor>(dpp, this, tid, c);
  p->process(dpp, std::move(p));
}

struct Lister : Completion<Lister> {
  FIFO* f;
  std::vector<list_entry> result;
  bool more = false;
  std::int64_t part_num;
  std::uint64_t ofs;
  int max_entries;
  int r_out = 0;
  std::vector<fifo::part_list_entry> entries;
  bool part_more = false;
  bool part_full = false;
  std::vector<list_entry>* entries_out;
  bool* more_out;
  std::uint64_t tid;

  bool read = false;

  void complete(Ptr&& p, int r) {
    if (r >= 0) {
      if (more_out) *more_out = more;
      if (entries_out) *entries_out = std::move(result);
    }
    Completion::complete(std::move(p), r);
  }

public:
  Lister(const DoutPrefixProvider *dpp, FIFO* f, std::int64_t part_num, std::uint64_t ofs, int max_entries,
	 std::vector<list_entry>* entries_out, bool* more_out,
	 std::uint64_t tid, lr::AioCompletion* super)
    : Completion(dpp, super), f(f), part_num(part_num), ofs(ofs), max_entries(max_entries),
      entries_out(entries_out), more_out(more_out), tid(tid) {
    result.reserve(max_entries);
  }

  Lister(const Lister&) = delete;
  Lister& operator =(const Lister&) = delete;
  Lister(Lister&&) = delete;
  Lister& operator =(Lister&&) = delete;

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    if (read)
      handle_read(std::move(p), r);
    else
      handle_list(dpp, std::move(p), r);
  }

  void list(Ptr&& p) {
    if (max_entries > 0) {
      part_more = false;
      part_full = false;
      entries.clear();

      std::unique_lock l(f->m);
      auto part_oid = f->info.part_oid(part_num);
      l.unlock();

      read = false;
      auto op = list_part(f->cct, ofs, max_entries, &r_out,
			  &entries, &part_more, &part_full, tid);
      f->ioctx.aio_operate(part_oid, call(std::move(p)), &op, nullptr);
    } else {
      complete(std::move(p), 0);
    }
  }

  void handle_read(Ptr&& p, int r) {
    read = false;
    if (r >= 0) r = r_out;
    r_out = 0;

    if (r < 0) {
      complete(std::move(p), r);
      return;
    }

    if (part_num < f->info.tail_part_num) {
      /* raced with trim? restart */
      max_entries += result.size();
      result.clear();
      part_num = f->info.tail_part_num;
      ofs = 0;
      list(std::move(p));
      return;
    }
    /* assuming part was not written yet, so end of data */
    more = false;
    complete(std::move(p), 0);
    return;
  }

  void handle_list(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    if (r >= 0) r = r_out;
    r_out = 0;
    std::unique_lock l(f->m);
    auto part_oid = f->info.part_oid(part_num);
    l.unlock();
    if (r == -ENOENT) {
      read = true;
      f->read_meta(dpp, tid, call(std::move(p)));
      return;
    }
    if (r < 0) {
      complete(std::move(p), r);
      return;
    }

    more = part_full || part_more;
    for (auto& entry : entries) {
      list_entry e;
      e.data = std::move(entry.data);
      e.marker = marker{part_num, entry.ofs}.to_string();
      e.mtime = entry.mtime;
      result.push_back(std::move(e));
    }
    max_entries -= entries.size();
    entries.clear();
    if (max_entries > 0 && part_more) {
      list(std::move(p));
      return;
    }

    if (!part_full) { /* head part is not full */
      complete(std::move(p), 0);
      return;
    }
    ++part_num;
    ofs = 0;
    list(std::move(p));
  }
};

void FIFO::list(const DoutPrefixProvider *dpp, int max_entries,
		std::optional<std::string_view> markstr,
		std::vector<list_entry>* out,
		bool* more,
		lr::AioCompletion* c) {
  std::unique_lock l(m);
  auto tid = ++next_tid;
  std::int64_t part_num = info.tail_part_num;
  l.unlock();
  std::uint64_t ofs = 0;
  std::optional<::rgw::cls::fifo::marker> marker;

  if (markstr) {
    marker = to_marker(*markstr);
    if (marker) {
      part_num = marker->num;
      ofs = marker->ofs;
    }
  }

  auto ls = std::make_unique<Lister>(dpp, this, part_num, ofs, max_entries, out,
				     more, tid, c);
  if (markstr && !marker) {
    auto l = ls.get();
    l->complete(std::move(ls), -EINVAL);
  } else {
    ls->list(std::move(ls));
  }
}
}
