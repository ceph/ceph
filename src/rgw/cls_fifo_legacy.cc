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

#include "include/rados/librados.hpp"

#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/random_string.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

#include "librados/AioCompletionImpl.h"

#include "rgw_tools.h"

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

int get_meta(lr::IoCtx& ioctx, const std::string& oid,
	     std::optional<fifo::objv> objv, fifo::info* info,
	     std::uint32_t* part_header_size,
	     std::uint32_t* part_entry_overhead,
	     optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::get_meta gm;
  gm.version = objv;
  cb::list in;
  encode(gm, in);
  cb::list bl;

  op.exec(fifo::op::CLASS, fifo::op::GET_META, in,
	  &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::get_meta_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (info) *info = std::move(reply.info);
      if (part_header_size) *part_header_size = reply.part_header_size;
      if (part_entry_overhead)
	*part_entry_overhead = reply.part_entry_overhead;
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  return r;
};

void update_meta(lr::ObjectWriteOperation* op, const fifo::objv& objv,
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
  op->exec(fifo::op::CLASS, fifo::op::UPDATE_META, in);
}

void part_init(lr::ObjectWriteOperation* op, std::string_view tag,
	       fifo::data_params params)
{
  fifo::op::init_part ip;

  ip.tag = tag;
  ip.params = params;

  cb::list in;
  encode(ip, in);
  op->exec(fifo::op::CLASS, fifo::op::INIT_PART, in);
}

int push_part(lr::IoCtx& ioctx, const std::string& oid, std::string_view tag,
	      std::deque<cb::list> data_bufs, optional_yield y)
{
  lr::ObjectWriteOperation op;
  fifo::op::push_part pp;

  pp.tag = tag;
  pp.data_bufs = data_bufs;
  pp.total_len = 0;

  for (const auto& bl : data_bufs)
    pp.total_len += bl.length();

  cb::list in;
  encode(pp, in);
  auto retval = 0;
  op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in, nullptr, &retval);
  auto r = rgw_rados_operate(ioctx, oid, &op, lr::OPERATION_RETURNVEC, y);
  return r < 0 ? r : retval;
}

void trim_part(lr::ObjectWriteOperation* op,
	       std::optional<std::string_view> tag,
	       std::uint64_t ofs)
{
  fifo::op::trim_part tp;

  tp.tag = tag;
  tp.ofs = ofs;

  cb::list in;
  encode(tp, in);
  op->exec(fifo::op::CLASS, fifo::op::TRIM_PART, in);
}

int list_part(lr::IoCtx& ioctx, const std::string& oid,
	      std::optional<std::string_view> tag, std::uint64_t ofs,
	      std::uint64_t max_entries,
	      std::vector<fifo::part_list_entry>* entries,
	      bool* more, bool* full_part, std::string* ptag,
	      optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::list_part lp;

  lp.tag = tag;
  lp.ofs = ofs;
  lp.max_entries = max_entries;

  cb::list in;
  encode(lp, in);
  cb::list bl;
  op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in, &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::list_part_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (entries) *entries = std::move(reply.entries);
      if (more) *more = reply.more;
      if (full_part) *full_part = reply.full_part;
      if (ptag) *ptag = reply.tag;
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  return r;
}

int get_part_info(lr::IoCtx& ioctx, const std::string& oid,
		  fifo::part_header* header, optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::get_part_info gpi;

  cb::list in;
  cb::list bl;
  encode(gpi, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in, &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::get_part_info_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (header) *header = std::move(reply.header);
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  return r;
}

static void complete(lr::AioCompletion* c_, int r)
{
  auto c = c_->pc;
  c->lock.lock();
  c->rval = r;
  c->complete = true;
  c->lock.unlock();

  auto cb_complete = c->callback_complete;
  auto cb_complete_arg = c->callback_complete_arg;
  if (cb_complete)
    cb_complete(c, cb_complete_arg);

  auto cb_safe = c->callback_safe;
  auto cb_safe_arg = c->callback_safe_arg;
  if (cb_safe)
    cb_safe(c, cb_safe_arg);

  c->lock.lock();
  c->callback_complete = NULL;
  c->callback_safe = NULL;
  c->cond.notify_all();
  c->put_unlock();
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

std::string FIFO::generate_tag() const
{
  static constexpr auto HEADER_TAG_SIZE = 16;
  return gen_rand_alphanumeric_plain(static_cast<CephContext*>(ioctx.cct()),
				     HEADER_TAG_SIZE);
}


int FIFO::apply_update(fifo::info* info,
		       const fifo::objv& objv,
		       const fifo::update& update)
{
  std::unique_lock l(m);
  auto err = info->apply_update(update);
  if (objv != info->version) {
    return -ECANCELED;
  }
  if (err) {
    return -ECANCELED;
  }

  ++info->version.ver;

  return {};
}

int FIFO::_update_meta(const fifo::update& update,
		       fifo::objv version, bool* pcanceled,
		       optional_yield y)
{
  lr::ObjectWriteOperation op;
  bool canceled = false;
  update_meta(&op, info.version, update);
  auto r = rgw_rados_operate(ioctx, oid, &op, y);
  if (r >= 0 || r == -ECANCELED) {
    canceled = (r == -ECANCELED);
    if (!canceled) {
      r = apply_update(&info, version, update);
      if (r < 0) canceled = true;
    }
    if (canceled) {
      r = read_meta(y);
      canceled = r < 0 ? false : true;
    }
  }
  if (pcanceled) *pcanceled = canceled;
  return r;
}

struct Updater {
  FIFO* fifo;
  lr::AioCompletion* super;
  lr::AioCompletion* cur = lr::Rados::aio_create_completion(
    static_cast<void*>(this), &FIFO::update_callback);
  fifo::update update;
  fifo::objv version;
  bool reread = false;
  bool* pcanceled = nullptr;
  Updater(FIFO* fifo, lr::AioCompletion* super,
	  const fifo::update& update, fifo::objv version,
	  bool* pcanceled)
    : fifo(fifo), super(super), update(update), version(version),
      pcanceled(pcanceled) {
    super->pc->get();
  }
  ~Updater() {
    cur->release();
  }
};


void FIFO::update_callback(lr::completion_t, void* arg)
{
  auto updater = static_cast<Updater*>(arg);
  if (!updater->reread) {
    int r = updater->cur->get_return_value();
    if (r < 0 && r != -ECANCELED) {
      complete(updater->super, r);
      delete updater;
      return;
    }
    bool canceled = (r == -ECANCELED);
    if (!canceled) {
      int r = updater->fifo->apply_update(&updater->fifo->info, updater->version,
					  updater->update);
      if (r < 0)
	canceled = true;
    }
    if (!canceled) {
      if (updater->pcanceled)
	*updater->pcanceled = false;
      complete(updater->super, 0);
    } else {
      updater->cur->release();
      updater->cur = lr::Rados::aio_create_completion(
	arg, &FIFO::update_callback);
      updater->reread = true;
      auto r = updater->fifo->read_meta(updater->cur);
      if (r < 0) {
	complete(updater->super, r);
	delete updater;
      }
    }
  } else {
    int r = updater->cur->get_return_value();
    if (r < 0 && updater->pcanceled) {
      *updater->pcanceled = false;
    } else if (r >= 0 && updater->pcanceled) {
      *updater->pcanceled = true;
    }
    complete(updater->super, r);
    delete updater;
  }
}

int FIFO::_update_meta(const fifo::update& update,
		       fifo::objv version, bool* pcanceled,
		       lr::AioCompletion* c)
{
  lr::ObjectWriteOperation op;
  update_meta(&op, info.version, update);
  auto updater = new Updater(this, c, update, version, pcanceled);
  auto r = ioctx.aio_operate(oid, c, &op);
  if (r < 0) {
    delete updater;
  }
  return r;
}

int FIFO::create_part(int64_t part_num, std::string_view tag, optional_yield y)
{
  lr::ObjectWriteOperation op;
  op.create(false); /* We don't need exclusivity, part_init ensures
		       we're creating from the  same journal entry. */
  std::unique_lock l(m);
  part_init(&op, tag, info.params);
  auto oid = info.part_oid(part_num);
  l.unlock();
  return rgw_rados_operate(ioctx, oid, &op, y);
}

int FIFO::remove_part(int64_t part_num, std::string_view tag, optional_yield y)
{
  lr::ObjectWriteOperation op;
  op.remove();
  std::unique_lock l(m);
  auto oid = info.part_oid(part_num);
  l.unlock();
  return rgw_rados_operate(ioctx, oid, &op, y);
}

int FIFO::process_journal(optional_yield y)
{
  std::vector<fifo::journal_entry> processed;

  std::unique_lock l(m);
  auto tmpjournal = info.journal;
  auto new_tail = info.tail_part_num;
  auto new_head = info.head_part_num;
  auto new_max = info.max_push_part_num;
  l.unlock();

  int r;
  for (auto& [n, entry] : tmpjournal) {
    switch (entry.op) {
    case fifo::journal_entry::Op::create:
      r = create_part(entry.part_num, entry.part_tag, y);
      if (entry.part_num > new_max) {
	new_max = entry.part_num;
      }
      break;
    case fifo::journal_entry::Op::set_head:
      if (entry.part_num > new_head) {
	new_head = entry.part_num;
      }
      break;
    case fifo::journal_entry::Op::remove:
      r = remove_part(entry.part_num, entry.part_tag, y);
      if (r == -ENOENT) r = 0;
      if (entry.part_num >= new_tail) {
	new_tail = entry.part_num + 1;
      }
      break;
    default:
      return -EIO;
    }

    if (r < 0) {
      return -EIO;
    }

    processed.push_back(std::move(entry));
  }

  // Postprocess

  bool canceled = true;

  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
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
      /* nothing to update anymore */
      canceled = false;
      break;
    }
    _update_meta(fifo::update()
		 .tail_part_num(tail_part_num)
		 .head_part_num(head_part_num)
		 .max_push_part_num(max_part_num)
		 .journal_entries_rm(processed),
		 objv, &canceled, y);
    if (r < 0) break;

    if (canceled) {
      std::vector<fifo::journal_entry> new_processed;
      std::unique_lock l(m);
      for (auto& e : processed) {
	auto jiter = info.journal.find(e.part_num);
	/* journal entry was already processed */
	if (jiter == info.journal.end() ||
	    !(jiter->second == e)) {
	  continue;
	}
	new_processed.push_back(e);
      }
      processed = std::move(new_processed);
    }
  }
  if (canceled)
    r = -ECANCELED;
  return r;
}

int FIFO::_prepare_new_part(bool is_head, optional_yield y)
{
  std::unique_lock l(m);
  std::vector jentries = { info.next_journal_entry(generate_tag()) };
  std::int64_t new_head_part_num = info.head_part_num;
  auto version = info.version;

  if (is_head) {
    auto new_head_jentry = jentries.front();
    new_head_jentry.op = fifo::journal_entry::Op::set_head;
    new_head_part_num = jentries.front().part_num;
    jentries.push_back(std::move(new_head_jentry));
  }
  l.unlock();

  int r = 0;
  bool canceled = true;
  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
    canceled = false;
    r = _update_meta(fifo::update{}.journal_entries_add(jentries),
			 version, &canceled, y);
    if (r >= 0 && canceled) {
      std::unique_lock l(m);
      auto found = (info.journal.find(jentries.front().part_num) !=
		    info.journal.end());
      if ((info.max_push_part_num >= jentries.front().part_num &&
	   info.head_part_num >= new_head_part_num)) {
	// We don't even need to process the journal.
	return 0;
      }
      if (found) {
	// Journaled, but not processed.
	canceled = false;
      }
      l.unlock();
    }
  }
  if (canceled)
    r = -ECANCELED;
  if (r >= 0)
    r = process_journal(y);
  return r;
}

int FIFO::_prepare_new_head(optional_yield y) {
  std::unique_lock l(m);
  std::int64_t new_head_num = info.head_part_num + 1;
  auto max_push_part_num = info.max_push_part_num;
  auto version = info.version;
  l.unlock();

  int r = 0;
  if (max_push_part_num < new_head_num) {
    r = _prepare_new_part(true, y);
    if (r >= 0) {
      std::unique_lock l(m);
      if (info.max_push_part_num < new_head_num) {
	r = -EIO;
      }
      l.unlock();
    }
  } else {
    bool canceled = true;
    for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
      _update_meta(fifo::update{}.head_part_num(new_head_num),
		   version, &canceled, y);
      if (r < 0)
	break;
      std::unique_lock l(m);
      auto head_part_num = info.head_part_num;
      version = info.version;
      l.unlock();
      if (canceled && (head_part_num >= new_head_num)) {
	// Race, but someone did it for us
	canceled = false;
      }
    }
    if (canceled)
      r = -ECANCELED;
  }
  return 0;
}

int FIFO::push_entries(const std::deque<cb::list>& data_bufs,
		       optional_yield y)
{
  std::unique_lock l(m);
  auto head_part_num = info.head_part_num;
  auto tag = info.head_tag;
  const auto part_oid = info.part_oid(head_part_num);
  l.unlock();

  return push_part(ioctx, part_oid, tag, data_bufs, y);
}

int FIFO::trim_part(int64_t part_num, uint64_t ofs,
		    std::optional<std::string_view> tag,
		    optional_yield y)
{
  lr::ObjectWriteOperation op;
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  rgw::cls::fifo::trim_part(&op, tag, ofs);
  return rgw_rados_operate(ioctx, part_oid, &op, y);
}

int FIFO::trim_part(int64_t part_num, uint64_t ofs,
		    std::optional<std::string_view> tag,
		    lr::AioCompletion* c)
{
  lr::ObjectWriteOperation op;
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  rgw::cls::fifo::trim_part(&op, tag, ofs);
  return ioctx.aio_operate(part_oid, c, &op);
}

int FIFO::open(lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
	       optional_yield y, std::optional<fifo::objv> objv)
{
  fifo::info info;
  std::uint32_t size;
  std::uint32_t over;
  int r = get_meta(ioctx, std::move(oid), objv, &info, &size, &over, y);
  if (r >= 0) {
    std::unique_ptr<FIFO> f(new FIFO(std::move(ioctx), oid));
    f->info = info;
    f->part_header_size = size;
    f->part_entry_overhead = over;
    // If there are journal entries, process them, in case
    // someone crashed mid-transaction.
    if (!info.journal.empty()) {
      r = f->process_journal(y);
      if (r >= 0 && fifo)
	*fifo = std::move(f);
    } else {
      *fifo = std::move(f);
    }
  }
  return r;
}

int FIFO::create(lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
		 optional_yield y, std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive, std::uint64_t max_part_size,
		 std::uint64_t max_entry_size)
{
  lr::ObjectWriteOperation op;
  create_meta(&op, oid, objv, oid_prefix, exclusive, max_part_size,
	      max_entry_size);
  int r = rgw_rados_operate(ioctx, oid, &op, y);
  if (r >= 0) {
    r = open(std::move(ioctx), std::move(oid), fifo, y, objv);
  }
  return r;
}

int FIFO::read_meta(optional_yield y) {
  fifo::info _info;
  std::uint32_t _phs;
  std::uint32_t _peo;

  auto r = get_meta(ioctx, oid, nullopt, &_info, &_phs, &_peo, y);
  if (r >= 0) {
    std::unique_lock l(m);
    // We have a newer version already!
    if (_info.version.same_or_later(this->info.version)) {
      info = std::move(_info);
      part_header_size = _phs;
      part_entry_overhead = _peo;
    }
  }
  return r;
}

struct Reader {
  FIFO* fifo;
  cb::list bl;
  lr::AioCompletion* super;
  lr::AioCompletion* cur = lr::Rados::aio_create_completion(
    static_cast<void*>(this), &FIFO::read_callback);
  Reader(FIFO* fifo, lr::AioCompletion* super)
    : fifo(fifo), super(super) {
    super->pc->get();
  }
  ~Reader() {
    cur->release();
  }
};

void FIFO::read_callback(lr::completion_t, void* arg)
{
  auto reader = static_cast<Reader*>(arg);
  auto r = reader->cur->get_return_value();
  if (r >= 0) try {
      fifo::op::get_meta_reply reply;
      auto iter = reader->bl.cbegin();
      decode(reply, iter);
      std::unique_lock l(reader->fifo->m);
      if (reply.info.version.same_or_later(reader->fifo->info.version)) {
	reader->fifo->info = std::move(reply.info);
	reader->fifo->part_header_size = reply.part_header_size;
	reader->fifo->part_entry_overhead = reply.part_entry_overhead;
      }
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  complete(reader->super, r);
  delete reader;
}

int FIFO::read_meta(lr::AioCompletion* c)
{
  lr::ObjectReadOperation op;
  fifo::op::get_meta gm;
  cb::list in;
  encode(gm, in);
  auto reader = new Reader(this, c);
  auto r = ioctx.aio_exec(oid, reader->cur, fifo::op::CLASS,
			  fifo::op::GET_META, in, &reader->bl);
  if (r < 0) {
    delete reader;
  }
  return r;
}


const fifo::info& FIFO::meta() const {
  return info;
}

std::pair<std::uint32_t, std::uint32_t> FIFO::get_part_layout_info() const {
  return {part_header_size, part_entry_overhead};
}

int FIFO::push(const cb::list& bl, optional_yield y) {
  return push(std::vector{ bl }, y);
}

int FIFO::push(const std::vector<cb::list>& data_bufs, optional_yield y)
{
  std::unique_lock l(m);
  auto max_entry_size = info.params.max_entry_size;
  auto need_new_head = info.need_new_head();
  l.unlock();
  if (data_bufs.empty()) {
    return 0;
  }

  // Validate sizes
  for (const auto& bl : data_bufs)
    if (bl.length() > max_entry_size)
      return -E2BIG;

  int r = 0;
  if (need_new_head) {
    r = _prepare_new_head(y);
    if (r < 0)
      return r;
  }

  std::deque remaining(data_bufs.begin(), data_bufs.end());
  std::deque<cb::list> batch;

  uint64_t batch_len = 0;
  auto retries = 0;
  bool canceled = true;
  while ((!remaining.empty() || !batch.empty()) &&
	 (retries <= MAX_RACE_RETRIES)) {
    std::unique_lock l(m);
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

    auto r = push_entries(batch, y);
    if (r >= 0) {
      // Made forward progress!
      canceled = false;
      retries = 0;
      if (static_cast<unsigned>(r) == batch.size()) {
	batch.clear();
      } else  {
	batch.erase(batch.begin(), batch.begin() + r);
	for (const auto& b : batch) {
	  batch_len +=  b.length() + part_entry_overhead;
	}
      }
    } else if (r == -ERANGE) {
      canceled = true;
      ++retries;
      r = _prepare_new_head(y);
    }
    if (r < 0)
      break;
  }
  if (canceled)
    r = -ECANCELED;
  return r;
}

int FIFO::list(int max_entries,
	       std::optional<std::string_view> markstr,
	       std::vector<list_entry>* presult, bool* pmore,
	       optional_yield y)
{
  std::unique_lock l(m);
  std::int64_t part_num = info.tail_part_num;
  l.unlock();
  std::uint64_t ofs = 0;
  if (markstr) {
    auto marker = to_marker(*markstr);
    if (!marker) return -EINVAL;
    part_num = marker->num;
    ofs = marker->ofs;
  }

  std::vector<list_entry> result;
  result.reserve(max_entries);
  bool more = false;

  std::vector<fifo::part_list_entry> entries;
  int r = 0;
  while (max_entries > 0) {
    bool part_more = false;
    bool part_full = false;

    std::unique_lock l(m);
    auto part_oid = info.part_oid(part_num);
    l.unlock();

    list_part(ioctx, part_oid, {}, ofs, max_entries, &entries,
	      &part_more, &part_full, nullptr, y);
    if (r == -ENOENT) {
      r = read_meta(y);
      if (r < 0) return r;
      if (part_num < info.tail_part_num) {
	/* raced with trim? restart */
	max_entries += result.size();
	result.clear();
	std::unique_lock l(m);
	part_num = info.tail_part_num;
	l.unlock();
	ofs = 0;
	continue;
      }
      /* assuming part was not written yet, so end of data */
      more = false;
      r = 0;
      break;
    }
    if (r < 0) {
      break;
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
      /* head part is not full, so we can assume we're done. */
      break;
    }
    if (!part_more) {
      ++part_num;
      ofs = 0;
    }
  }
  if (r >= 0) {
    if (presult) *presult = std::move(result);
    if (pmore) *pmore =  more;
  }
  return r;
}

int FIFO::trim(std::string_view markstr, optional_yield y)
{
  auto marker = to_marker(markstr);
  if (!marker) {
    return -EINVAL;
  }
  auto part_num = marker->num;
  auto ofs = marker->ofs;
  std::unique_lock l(m);
  auto pn = info.tail_part_num;
  l.unlock();

  int r;
  while (pn < part_num) {
    std::unique_lock l(m);
    auto max_part_size = info.params.max_part_size;
    l.unlock();
    r = trim_part(pn, max_part_size, std::nullopt, y);
    if (r < 0 && r == -ENOENT) {
      return r;
    }
    ++pn;
  }
  r = trim_part(part_num, ofs, std::nullopt, y);
  if (r < 0 && r != -ENOENT) {
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
    r = _update_meta(fifo::update{}.tail_part_num(part_num), objv, &canceled,
		     y);
    if (canceled) {
      l.lock();
      tail_part_num = info.tail_part_num;
      objv = info.version;
      l.unlock();
      ++retries;
    }
  }
  if (canceled) {
    r = -EIO;
  }
  return r;
}

struct Trimmer {
  FIFO* fifo;
  std::int64_t part_num;
  std::uint64_t ofs;
  std::int64_t pn;
  lr::AioCompletion* super;
  lr::AioCompletion* cur = lr::Rados::aio_create_completion(
    static_cast<void*>(this), &FIFO::trim_callback);
  bool update = false;
  bool canceled = false;
  int retries = 0;

  Trimmer(FIFO* fifo, std::int64_t part_num, std::uint64_t ofs, std::int64_t pn,
	  lr::AioCompletion* super)
    : fifo(fifo), part_num(part_num), ofs(ofs), pn(pn), super(super) {
    super->pc->get();
  }
  ~Trimmer() {
    cur->release();
  }
};

void FIFO::trim_callback(lr::completion_t, void* arg)
{
  auto trimmer = static_cast<Trimmer*>(arg);
  int r = trimmer->cur->get_return_value();
  if (r == -ENOENT)
    r = 0;

  if (r < 0) {
    complete(trimmer->super, r);
    delete trimmer;
  } else if (!trimmer->update) {
    if (trimmer->pn < trimmer->part_num) {
      std::unique_lock l(trimmer->fifo->m);
      const auto max_part_size = trimmer->fifo->info.params.max_part_size;
      const auto part_oid = trimmer->fifo->info.part_oid(trimmer->pn);
      l.unlock();
      trimmer->cur->release();
      trimmer->cur = lr::Rados::aio_create_completion(arg, &FIFO::trim_callback);
      ++trimmer->pn;
      r = trimmer->fifo->trim_part(trimmer->pn, max_part_size, std::nullopt,
				   trimmer->cur);
      if (r < 0) {
	complete(trimmer->super, r);
	delete trimmer;
      }
    } else {
      std::unique_lock l(trimmer->fifo->m);
      const auto tail_part_num = trimmer->fifo->info.tail_part_num;
      l.unlock();
      trimmer->cur->release();
      trimmer->cur = lr::Rados::aio_create_completion(arg, &FIFO::trim_callback);
      trimmer->update = true;
      trimmer->canceled = tail_part_num < trimmer->part_num;
      r = trimmer->fifo->trim_part(trimmer->part_num, trimmer->ofs,
				   std::nullopt, trimmer->cur);
      if (r < 0) {
	complete(trimmer->super, r);
	delete trimmer;
      }
    }
  } else {
    std::unique_lock l(trimmer->fifo->m);
    auto tail_part_num = trimmer->fifo->info.tail_part_num;
    auto objv = trimmer->fifo->info.version;
    l.unlock();
    if ((tail_part_num < trimmer->part_num) &&
	trimmer->canceled) {
      if (trimmer->retries <= MAX_RACE_RETRIES) {
	complete(trimmer->super, -EIO);
	delete trimmer;
      } else {
	trimmer->cur->release();
	trimmer->cur = lr::Rados::aio_create_completion(arg,
							&FIFO::trim_callback);
	++trimmer->retries;
	auto r = trimmer->fifo->_update_meta(fifo::update{}
					     .tail_part_num(trimmer->part_num),
			                     objv, &trimmer->canceled,
                                             trimmer->cur);
	if (r < 0) {
	  complete(trimmer->super, r);
	  delete trimmer;
	}
      }
    } else {
      complete(trimmer->super, 0);
    }
  }
}

int FIFO::trim(std::string_view markstr, lr::AioCompletion* c) {
  auto marker = to_marker(markstr);
  if (!marker) {
    return -EINVAL;
  }
  std::unique_lock l(m);
  const auto max_part_size = info.params.max_part_size;
  const auto pn = info.tail_part_num;
  const auto part_oid = info.part_oid(pn);
  l.unlock();
  auto trimmer = new Trimmer(this, marker->num, marker->ofs, pn, c);
  ++trimmer->pn;
  auto r = trimmer->fifo->trim_part(trimmer->pn, max_part_size, std::nullopt,
				    trimmer->cur);
  if (r < 0) {
    complete(trimmer->super, r);
    delete trimmer;
  }
  return 0;
}

int FIFO::get_part_info(int64_t part_num,
			fifo::part_header* header,
			optional_yield y)
{
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  return rgw::cls::fifo::get_part_info(ioctx, part_oid, header, y);
}


}
