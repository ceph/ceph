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

#ifndef CEPH_NEORADOS_CLS_FIFIO_H
#define CEPH_NEORADOS_CLS_FIFIO_H

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"
#include "include/buffer.h"

#include "common/allocate_unique.h"
#include "common/async/bind_handler.h"
#include "common/async/bind_like.h"
#include "common/async/completion.h"
#include "common/async/forward_handler.h"

#include "common/dout.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

namespace neorados::cls::fifo {
namespace ba = boost::asio;
namespace bs = boost::system;
namespace ca = ceph::async;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;

inline constexpr auto dout_subsys = ceph_subsys_rados;
inline constexpr std::uint64_t default_max_part_size = 4 * 1024 * 1024;
inline constexpr std::uint64_t default_max_entry_size = 32 * 1024;
inline constexpr auto MAX_RACE_RETRIES = 10;


const boost::system::error_category& error_category() noexcept;

enum class errc {
  raced = 1,
  inconsistency,
  entry_too_large,
  invalid_marker,
  update_failed
};
}

namespace boost::system {
template<>
struct is_error_code_enum<::neorados::cls::fifo::errc> {
  static const bool value = true;
};
template<>
struct is_error_condition_enum<::neorados::cls::fifo::errc> {
  static const bool value = false;
};
}

namespace neorados::cls::fifo {
//  explicit conversion:
inline bs::error_code make_error_code(errc e) noexcept {
  return { static_cast<int>(e), error_category() };
}

inline bs::error_code make_error_category(errc e) noexcept {
  return { static_cast<int>(e), error_category() };
}

void create_meta(WriteOp& op, std::string_view id,
		 std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive = false,
		 std::uint64_t max_part_size = default_max_part_size,
		 std::uint64_t max_entry_size = default_max_entry_size);
void get_meta(ReadOp& op, std::optional<fifo::objv> objv,
	      bs::error_code* ec_out, fifo::info* info,
	      std::uint32_t* part_header_size,
	      std::uint32_t* part_entry_overhead);

void update_meta(WriteOp& op, const fifo::objv& objv,
		 const fifo::update& desc);

void part_init(WriteOp& op, std::string_view tag,
	       fifo::data_params params);

void push_part(WriteOp& op, std::string_view tag,
	       std::deque<cb::list> data_bufs,
	       fu2::unique_function<void(bs::error_code, int)>);
void trim_part(WriteOp& op, std::optional<std::string_view> tag,
	       std::uint64_t ofs);
void list_part(ReadOp& op,
	       std::optional<std::string_view> tag,
	       std::uint64_t ofs,
	       std::uint64_t max_entries,
	       bs::error_code* ec_out,
	       std::vector<fifo::part_list_entry>* entries,
	       bool* more,
	       bool* full_part,
	       std::string* ptag);
void get_part_info(ReadOp& op,
		   bs::error_code* out_ec,
		   fifo::part_header* header);

struct marker {
  std::int64_t num = 0;
  std::uint64_t ofs = 0;

  marker() = default;
  marker(std::int64_t num, std::uint64_t ofs) : num(num), ofs(ofs) {}

  std::string to_string() {
    return fmt::format("{}:{}", num, ofs);
  }
};

struct list_entry {
  cb::list data;
  std::string marker;
  ceph::real_time mtime;
};

using part_info = fifo::part_header;

template<typename Handler>
class JournalProcessor;

class FIFO {
  template<typename Handler>
  friend class JournalProcessor;
  RADOS* const r;
  const IOContext ioc;
  const Object oid;
  std::mutex m;

  fifo::info info;

  std::uint32_t part_header_size = 0xdeadbeef;
  std::uint32_t part_entry_overhead = 0xdeadbeef;

  ba::executor executor;

  std::optional<marker> to_marker(std::string_view s);

  template<typename Handler, typename T>
  static void assoc_delete(const Handler& handler, T* t) {
    typename std::allocator_traits<typename ba::associated_allocator<Handler>::type>
      ::template rebind_alloc<T> a(
	ba::get_associated_allocator(handler));
    a.destroy(t);
    a.deallocate(t, 1);
  }

  FIFO(RADOS& r,
       IOContext ioc,
       Object oid,
       ba::executor executor)
    : r(&r), ioc(std::move(ioc)), oid(oid), executor(executor) {}

  std::string generate_tag() const;

  template <typename T>
  struct ExecDecodeCB {
    bs::error_code ec;
    T result;
    void operator()(bs::error_code e, const cb::list& r) {
      if (e) {
        ec = e;
        return;
      }
      try {
        auto p = r.begin();
        using ceph::decode;
        decode(result, p);
      } catch (const cb::error& err) {
        ec = err.code();
      }
    }
  };

  template<typename Handler>
  class MetaReader {
    Handler handler;
    using allocator_type = boost::asio::associated_allocator_t<Handler>;
    using decoder_type = ExecDecodeCB<fifo::op::get_meta_reply>;
    using decoder_ptr = ceph::allocated_unique_ptr<decoder_type, allocator_type>;
    decoder_ptr decoder;
  public:
    MetaReader(Handler&& handler, decoder_ptr&& decoder)
      : handler(std::move(handler)), decoder(std::move(decoder)) {}

    void operator ()(bs::error_code ec) {
      if (!ec) {
        ec = decoder->ec;
      }
      auto reply = std::move(decoder->result);
      decoder.reset(); // free handler-allocated memory before dispatching

      std::move(handler)(ec, std::move(reply.info),
			 std::move(reply.part_header_size),
			 std::move(reply.part_entry_overhead));
    }
  };

  template<typename Handler>
  static void _read_meta(RADOS* r, const Object& oid, const IOContext& ioc,
			 std::optional<fifo::objv> objv,
			 Handler&& handler, /* error_code, info, uint64,
					       uint64 */
			 std::optional<ba::executor> executor = std::nullopt){
    fifo::op::get_meta gm;

    gm.version = objv;

    cb::list in;
    encode(gm, in);
    ReadOp op;

    auto a = ba::get_associated_allocator(handler);
    auto reply =
      ceph::allocate_unique<ExecDecodeCB<fifo::op::get_meta_reply>>(a);

    auto e = ba::get_associated_executor(handler);
    op.exec(fifo::op::CLASS, fifo::op::GET_META, in, std::ref(*reply));
    r->execute(oid, ioc, std::move(op), nullptr,
	       ca::bind_ea(e, a, MetaReader(std::move(handler),
					    std::move(reply))));
  };

  template<typename Handler>
  void _read_meta(Handler&& handler /* error_code */) {
    auto e = ba::get_associated_executor(handler, get_executor());
    auto a = ba::get_associated_allocator(handler);
    _read_meta(r, oid, ioc,
	       nullopt,
	       ca::bind_ea(
		 e, a,
		 [this,
		  handler = std::move(handler)](bs::error_code ec,
						fifo::info&& info,
						std::uint64_t phs,
						std::uint64_t peo) mutable {
		   std::unique_lock l(m);
		   if (ec) {
		     l.unlock();
		     std::move(handler)(ec);
		     return;
		   }
		   // We have a newer version already!
		   if (!info.version.same_or_later(this->info.version)) {
		     l.unlock();
		     std::move(handler)(bs::error_code{});
		     return;
		   }
		   this->info = std::move(info);
		   part_header_size = phs;
		   part_entry_overhead = peo;
		   l.unlock();
		   std::move(handler)(bs::error_code{});
		 }), get_executor());
  }

  bs::error_code apply_update(fifo::info* info,
			      const fifo::objv& objv,
			      const fifo::update& update);


  template<typename Handler>
  void _update_meta(const fifo::update& update,
		    fifo::objv version,
		    Handler&& handler /* error_code, bool */) {
    WriteOp op;

    cls::fifo::update_meta(op, info.version, update);

    auto a = ba::get_associated_allocator(handler);
    auto e = ba::get_associated_executor(handler, get_executor());

    r->execute(
      oid, ioc, std::move(op),
      ca::bind_ea(
	e, a,
	[this, e, a, version, update,
	 handler = std::move(handler)](bs::error_code ec) mutable {
	  if (ec && ec != bs::errc::operation_canceled) {
	    std::move(handler)(ec, bool{});
	    return;
	  }

	  auto canceled = (ec == bs::errc::operation_canceled);

	  if (!canceled) {
	    ec = apply_update(&info,
			      version,
			      update);
	    if (ec) {
	      canceled = true;
	    }
	  }

	  if (canceled) {
	    _read_meta(
	      ca::bind_ea(
		e, a,
		[handler = std::move(handler)](bs::error_code ec) mutable {
		  std::move(handler)(ec, ec ? false : true);
		}));
	    return;
	  }
	  std::move(handler)(ec, false);
	  return;
	}));
  }

  template<typename Handler>
  auto _process_journal(Handler&& handler /* error_code */) {
    auto a = ba::get_associated_allocator(std::ref(handler));
    auto j = ceph::allocate_unique<JournalProcessor<Handler>>(
      a, this, std::move(handler));
    auto p = j.release();
    p->process();
  }

  template<typename Handler>
  class NewPartPreparer {
    FIFO* f;
    Handler handler;
    std::vector<fifo::journal_entry> jentries;
    int i;
    std::int64_t new_head_part_num;

  public:

    void operator ()(bs::error_code ec, bool canceled) {
      if (ec) {
	std::move(handler)(ec);
	return;
      }

      if (canceled) {
	std::unique_lock l(f->m);
	auto iter = f->info.journal.find(jentries.front().part_num);
	auto max_push_part_num = f->info.max_push_part_num;
	auto head_part_num = f->info.head_part_num;
	auto version = f->info.version;
	auto found = (iter != f->info.journal.end());
	l.unlock();
	if ((max_push_part_num >= jentries.front().part_num &&
	    head_part_num >= new_head_part_num)) {
	  /* raced, but new part was already written */
	  std::move(handler)(bs::error_code{});
	  return;
	}
	if (i >= MAX_RACE_RETRIES) {
	  std::move(handler)(errc::raced);
	  return;
	}
	if (!found) {
	  auto e = ba::get_associated_executor(handler, f->get_executor());
	  auto a = ba::get_associated_allocator(handler);
	  f->_update_meta(fifo::update{}
			  .journal_entries_add(jentries),
                          version,
			  ca::bind_ea(
			    e, a,
			    NewPartPreparer(f, std::move(handler),
					    jentries,
					    i + 1, new_head_part_num)));
	  return;
	}
	// Fall through. We still need to process the journal.
      }
      f->_process_journal(std::move(handler));
      return;
    }

    NewPartPreparer(FIFO* f,
		    Handler&& handler,
		    std::vector<fifo::journal_entry> jentries,
		    int i, std::int64_t new_head_part_num)
      : f(f), handler(std::move(handler)), jentries(std::move(jentries)),
	i(i), new_head_part_num(new_head_part_num) {}
  };

  template<typename Handler>
  void _prepare_new_part(bool is_head,
			 Handler&& handler /* error_code */) {
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

    auto e = ba::get_associated_executor(handler, get_executor());
    auto a = ba::get_associated_allocator(handler);
    _update_meta(fifo::update{}.journal_entries_add(jentries),
		 version,
		 ca::bind_ea(
		   e, a,
		   NewPartPreparer(this, std::move(handler),
				   jentries, 0, new_head_part_num)));
  }

  template<typename Handler>
  class NewHeadPreparer {
    FIFO* f;
    Handler handler;
    int i;
    std::int64_t new_head_num;

  public:

    void operator ()(bs::error_code ec, bool canceled) {
      std::unique_lock l(f->m);
      auto head_part_num = f->info.head_part_num;
      auto version = f->info.version;
      l.unlock();

      if (ec) {
	std::move(handler)(ec);
	return;
      }
      if (canceled) {
	if (i >= MAX_RACE_RETRIES) {
	  std::move(handler)(errc::raced);
	  return;
	}

	// Raced, but there's still work to do!
	if (head_part_num < new_head_num) {
	  auto e = ba::get_associated_executor(handler, f->get_executor());
	  auto a = ba::get_associated_allocator(handler);
	  f->_update_meta(fifo::update{}.head_part_num(new_head_num),
			  version,
			  ca::bind_ea(
			    e, a,
			    NewHeadPreparer(f, std::move(handler),
					    i + 1,
					    new_head_num)));
	  return;
	}
      }
      // Either we succeeded, or we were raced by someone who did it for us.
      std::move(handler)(bs::error_code{});
      return;
    }

    NewHeadPreparer(FIFO* f,
		    Handler&& handler,
		    int i, std::int64_t new_head_num)
      : f(f), handler(std::move(handler)), i(i), new_head_num(new_head_num) {}
  };

  template<typename Handler>
  void _prepare_new_head(Handler&& handler /* error_code */) {
    std::unique_lock l(m);
    int64_t new_head_num = info.head_part_num + 1;
    auto max_push_part_num = info.max_push_part_num;
    auto version = info.version;
    l.unlock();

    if (max_push_part_num < new_head_num) {
      auto e = ba::get_associated_executor(handler, get_executor());
      auto a = ba::get_associated_allocator(handler);
      _prepare_new_part(
	true,
	ca::bind_ea(
	  e, a,
	  [this, new_head_num,
	   handler = std::move(handler)](bs::error_code ec) mutable {
	    if (ec) {
	      handler(ec);
	      return;
	    }
	    std::unique_lock l(m);
	    if (info.max_push_part_num < new_head_num) {
	      l.unlock();
	      ldout(r->cct(), 0)
		<< "ERROR: " << __func__
		<< ": after new part creation: meta_info.max_push_part_num="
		<< info.max_push_part_num << " new_head_num="
		<< info.max_push_part_num << dendl;
	      std::move(handler)(errc::inconsistency);
	    } else {
	      l.unlock();
	      std::move(handler)(bs::error_code{});
	    }
	  }));
      return;
    }
    auto e = ba::get_associated_executor(handler, get_executor());
    auto a = ba::get_associated_allocator(handler);
    _update_meta(fifo::update{}.head_part_num(new_head_num),
		 version,
		 ca::bind_ea(
		   e, a,
		   NewHeadPreparer(this, std::move(handler), 0,
				   new_head_num)));
  }

  template<typename T>
  struct ExecHandleCB {
    bs::error_code ec;
    T result;
    void operator()(bs::error_code e, const T& t) {
      if (e) {
        ec = e;
        return;
      }
      result = t;
    }
  };

  template<typename Handler>
  class EntryPusher {
    Handler handler;
    using allocator_type = boost::asio::associated_allocator_t<Handler>;
    using decoder_type = ExecHandleCB<int>;
    using decoder_ptr = ceph::allocated_unique_ptr<decoder_type, allocator_type>;
    decoder_ptr decoder;

  public:

    EntryPusher(Handler&& handler, decoder_ptr&& decoder)
      : handler(std::move(handler)), decoder(std::move(decoder)) {}

    void operator ()(bs::error_code ec) {
      if (!ec) {
        ec = decoder->ec;
      }
      auto reply = std::move(decoder->result);
      decoder.reset(); // free handler-allocated memory before dispatching

      std::move(handler)(ec, std::move(reply));
    }
  };

  template<typename Handler>
  auto push_entries(const std::deque<cb::list>& data_bufs,
		    Handler&& handler /* error_code, int */) {
    WriteOp op;
    std::unique_lock l(m);
    auto head_part_num = info.head_part_num;
    auto tag = info.head_tag;
    auto oid = info.part_oid(head_part_num);
    l.unlock();

    auto a = ba::get_associated_allocator(handler);
    auto reply = ceph::allocate_unique<ExecHandleCB<int>>(a);

    auto e = ba::get_associated_executor(handler, get_executor());
    push_part(op, tag, data_bufs, std::ref(*reply));
    return r->execute(oid, ioc, std::move(op),
		      ca::bind_ea(e, a, EntryPusher(std::move(handler),
						    std::move(reply))));
  }

  template<typename CT>
  auto trim_part(int64_t part_num,
		 uint64_t ofs,
		 std::optional<std::string_view> tag,
		 CT&& ct) {
    WriteOp op;
    cls::fifo::trim_part(op, tag, ofs);
    return r->execute(info.part_oid(part_num), ioc, std::move(op),
		      std::forward<CT>(ct));
  }


public:

  FIFO(const FIFO&) = delete;
  FIFO& operator =(const FIFO&) = delete;
  FIFO(FIFO&&) = delete;
  FIFO& operator =(FIFO&&) = delete;

  template<typename CT>
  static auto open(RADOS& r, const IOContext& ioc, Object oid, CT&& ct,
		   std::optional<fifo::objv> objv = std::nullopt,
		   std::optional<ba::executor> executor = std::nullopt) {
    ba::async_completion<CT, void(bs::error_code,
				  std::unique_ptr<FIFO>)> init(ct);
    auto e = ba::get_associated_executor(init.completion_handler,
					 executor.value_or(r.get_executor()));
    auto a = ba::get_associated_allocator(init.completion_handler);
    _read_meta(
      &r, oid, ioc, objv,
      ca::bind_ea(
	e, a,
	[&r, ioc, oid, executor, handler = std::move(init.completion_handler)]
	(bs::error_code ec, fifo::info info,
	 std::uint32_t size, std::uint32_t over) mutable {
	  std::unique_ptr<FIFO> f(
	    new FIFO(r, ioc, oid, executor.value_or(r.get_executor())));
	  f->info = info;
	  f->part_header_size = size;
	  f->part_entry_overhead = over;
	  // If there are journal entries, process them, in case
	  // someone crashed mid-transaction.
	  if (!ec && !info.journal.empty()) {
	    auto e = ba::get_associated_executor(handler, f->get_executor());
	    auto a = ba::get_associated_allocator(handler);
	    auto g = f.get();
	    g->_process_journal(
	      ca::bind_ea(
		e, a,
		[f = std::move(f),
		 handler = std::move(handler)](bs::error_code ec) mutable {
		  std::move(handler)(ec, std::move(f));
		}));
	    return;
	  }
	  std::move(handler)(ec, std::move(f));
	  return;
	}));
    return init.result.get();
  }

  template<typename CT>
  static auto create(RADOS& r, const IOContext& ioc, Object oid, CT&& ct,
		     std::optional<fifo::objv> objv = std::nullopt,
		     std::optional<std::string_view> oid_prefix = std::nullopt,
		     bool exclusive = false,
		     std::uint64_t max_part_size = default_max_part_size,
		     std::uint64_t max_entry_size = default_max_entry_size,
		     std::optional<ba::executor> executor = std::nullopt) {
    ba::async_completion<CT, void(bs::error_code,
				  std::unique_ptr<FIFO>)> init(ct);
    WriteOp op;
    create_meta(op, oid, objv, oid_prefix, exclusive, max_part_size,
		max_entry_size);
    auto e = ba::get_associated_executor(init.completion_handler,
					 executor.value_or(r.get_executor()));
    auto a = ba::get_associated_allocator(init.completion_handler);
    r.execute(
      oid, ioc, std::move(op),
      ca::bind_ea(
	e, a,
	[objv, &r, ioc, oid, executor, handler = std::move(init.completion_handler)]
	(bs::error_code ec) mutable {
	  if (ec) {
	    std::move(handler)(ec, nullptr);
	    return;
	  }
	  auto e = ba::get_associated_executor(
	    handler, executor.value_or(r.get_executor()));
	  auto a = ba::get_associated_allocator(handler);
	  _read_meta(
	    &r, oid, ioc, objv,
	    ca::bind_ea(
	      e, a,
	      [&r, ioc, executor, oid, handler = std::move(handler)]
	      (bs::error_code ec, fifo::info info,
	       std::uint32_t size, std::uint32_t over) mutable {
		std::unique_ptr<FIFO> f(
		  new FIFO(r, ioc, oid, executor.value_or(r.get_executor())));
		f->info = info;
		f->part_header_size = size;
		f->part_entry_overhead = over;
		if (!ec && !info.journal.empty()) {
		  auto e = ba::get_associated_executor(handler,
						       f->get_executor());
		  auto a = ba::get_associated_allocator(handler);
		  auto g = f.get();
		  g->_process_journal(
		    ca::bind_ea(
		      e, a,
		      [f = std::move(f), handler = std::move(handler)]
		      (bs::error_code ec) mutable {
			std::move(handler)(ec, std::move(f));
		      }));
		  return;
		}
		std::move(handler)(ec, std::move(f));
	      }));
	    }));
    return init.result.get();
  }

  template<typename CT>
  auto read_meta(CT&& ct, std::optional<fifo::objv> objv = std::nullopt) {
    std::unique_lock l(m);
    auto version = info.version;
    l.unlock();
    ba::async_completion<CT, void(bs::error_code)> init(ct);
    auto e = ba::get_associated_executor(init.completion_handler,
					 get_executor());
    auto a = ba::get_associated_allocator(init.completion_handler);
    _read_meta(
      r, oid, ioc, objv,
      ca::bind_ea(
	e, a,
	[this, version, handler = std::move(init.completion_handler)]
	(bs::error_code ec, fifo::info newinfo,
	 std::uint32_t size, std::uint32_t over) mutable {
	  std::unique_lock l(m);
	  if (version == info.version) {
	    info = newinfo;
	    part_header_size = size;
	    part_entry_overhead = over;
	  }
	  l.unlock();
	  return std::move(handler)(ec);
	}));
    return init.result.get();
  }

  const fifo::info& meta() const {
    return info;
  }

  std::pair<std::uint32_t, std::uint32_t> get_part_layout_info() {
    return {part_header_size, part_entry_overhead};
  }

  template<typename CT>
  auto push(const cb::list& bl, CT&& ct) {
    return push(std::vector{ bl }, std::forward<CT>(ct));
  }

  template<typename Handler>
  class Pusher {
    FIFO* f;
    std::deque<cb::list> remaining;
    std::deque<cb::list> batch;
    int i;
    Handler handler;

    void prep_then_push(const unsigned successes) {
      std::unique_lock l(f->m);
      auto max_part_size = f->info.params.max_part_size;
      auto part_entry_overhead = f->part_entry_overhead;
      l.unlock();

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
	std::move(handler)(bs::error_code{});
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
      push();
    }

    void push() {
      auto e = ba::get_associated_executor(handler, f->get_executor());
      auto a = ba::get_associated_allocator(handler);
      f->push_entries(batch,
		      ca::bind_ea(e, a,
				  Pusher(f, std::move(remaining),
					 batch, i,
					 std::move(handler))));
    }

  public:

    // Initial call!
    void operator ()() {
      prep_then_push(0);
    }

    // Called with response to push_entries
    void operator ()(bs::error_code ec, int r) {
      if (ec == bs::errc::result_out_of_range) {
	auto e = ba::get_associated_executor(handler, f->get_executor());
	auto a = ba::get_associated_allocator(handler);
	f->_prepare_new_head(
	  ca::bind_ea(e, a,
		      Pusher(f, std::move(remaining),
			     std::move(batch), i,
			     std::move(handler))));
	return;
      }
      if (ec) {
	std::move(handler)(ec);
	return;
      }
      i = 0; // We've made forward progress, so reset the race counter!
      prep_then_push(r);
    }

    // Called with response to prepare_new_head
    void operator ()(bs::error_code ec) {
      if (ec == bs::errc::operation_canceled) {
	if (i == MAX_RACE_RETRIES) {
	  ldout(f->r->cct(), 0)
	    << "ERROR: " << __func__
	    << "(): race check failed too many times, likely a bug" << dendl;
	  std::move(handler)(make_error_code(errc::raced));
	  return;
	}
	++i;
      } else if (ec) {
	std::move(handler)(ec);
	return;
      }

      if (batch.empty()) {
	prep_then_push(0);
	return;
      } else {
	push();
	return;
      }
    }

    Pusher(FIFO* f, std::deque<cb::list>&& remaining,
	   std::deque<cb::list> batch, int i,
	   Handler&& handler)
      : f(f), remaining(std::move(remaining)),
	batch(std::move(batch)), i(i),
	handler(std::move(handler)) {}
  };

  template<typename CT>
  auto push(const std::vector<cb::list>& data_bufs,
	    CT&& ct) {
    ba::async_completion<CT, void(bs::error_code)> init(ct);
    std::unique_lock l(m);
    auto max_entry_size = info.params.max_entry_size;
    auto need_new_head = info.need_new_head();
    l.unlock();
    auto e = ba::get_associated_executor(init.completion_handler,
					 get_executor());
    auto a = ba::get_associated_allocator(init.completion_handler);
    if (data_bufs.empty() ) {
      // Can't fail if you don't try.
      e.post(ca::bind_handler(std::move(init.completion_handler),
				  bs::error_code{}), a);
      return init.result.get();
    }

    // Validate sizes
    for (const auto& bl : data_bufs) {
      if (bl.length() > max_entry_size) {
	ldout(r->cct(), 10) << __func__ << "(): entry too large: "
			    << bl.length() << " > "
			    << info.params.max_entry_size << dendl;
	e.post(ca::bind_handler(std::move(init.completion_handler),
				    errc::entry_too_large), a);
	return init.result.get();
      }
    }

    auto p = ca::bind_ea(e, a,
			 Pusher(this, {data_bufs.begin(), data_bufs.end()},
				  {}, 0, std::move(init.completion_handler)));

    if (need_new_head) {
      _prepare_new_head(std::move(p));
    } else {
      e.dispatch(std::move(p), a);
    }
    return init.result.get();
  }

  template<typename Handler>
  class Lister {
    FIFO* f;
    std::vector<list_entry> result;
    bool more = false;
    std::int64_t part_num;
    std::uint64_t ofs;
    int max_entries;
    bs::error_code ec_out;
    std::vector<fifo::part_list_entry> entries;
    bool part_more = false;
    bool part_full = false;
    Handler handler;

    void handle(bs::error_code ec) {
      auto h = std::move(handler);
      auto m = more;
      auto r = std::move(result);

      FIFO::assoc_delete(h, this);
      std::move(h)(ec, std::move(r), m);
    }

  public:
    Lister(FIFO* f, std::int64_t part_num, std::uint64_t ofs, int max_entries,
	   Handler&& handler)
      : f(f), part_num(part_num), ofs(ofs), max_entries(max_entries),
	handler(std::move(handler)) {
      result.reserve(max_entries);
    }


    Lister(const Lister&) = delete;
    Lister& operator =(const Lister&) = delete;
    Lister(Lister&&) = delete;
    Lister& operator =(Lister&&) = delete;

    void list() {
      if (max_entries > 0) {
	ReadOp op;
	ec_out.clear();
	part_more = false;
	part_full = false;
	entries.clear();

	std::unique_lock l(f->m);
	auto part_oid = f->info.part_oid(part_num);
	l.unlock();

	list_part(op,
		  {},
		  ofs,
		  max_entries,
		  &ec_out,
		  &entries,
		  &part_more,
		  &part_full,
		  nullptr);
	auto e = ba::get_associated_executor(handler, f->get_executor());
	auto a = ba::get_associated_allocator(handler);
	f->r->execute(
	  part_oid,
	  f->ioc,
	  std::move(op),
	  nullptr,
	  ca::bind_ea(
	    e, a,
	    [t = std::unique_ptr<Lister>(this), this,
	     part_oid](bs::error_code ec) mutable {
	      t.release();
	      if (ec == bs::errc::no_such_file_or_directory) {
		auto e = ba::get_associated_executor(handler,
						     f->get_executor());
		auto a = ba::get_associated_allocator(handler);
		f->_read_meta(
		  ca::bind_ea(
		    e, a,
		    [this](bs::error_code ec) mutable {
		      if (ec) {
			handle(ec);
			return;
		      }

		      if (part_num < f->info.tail_part_num) {
			/* raced with trim? restart */
			max_entries += result.size();
			result.clear();
			part_num = f->info.tail_part_num;
			ofs = 0;
			list();
		      }
		      /* assuming part was not written yet, so end of data */
		      more = false;
		      handle({});
		      return;
		    }));
		return;
	      }
	      if (ec) {
		ldout(f->r->cct(), 0)
		  << __func__
		  << "(): list_part() on oid=" << part_oid
		  << " returned ec=" << ec.message() << dendl;
		handle(ec);
		return;
	      }
	      if (ec_out) {
		ldout(f->r->cct(), 0)
		  << __func__
		  << "(): list_part() on oid=" << f->info.part_oid(part_num)
		  << " returned ec=" << ec_out.message() << dendl;
		handle(ec_out);
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
	      if (max_entries > 0 &&
		  part_more) {
		list();
		return;
	      }

	      if (!part_full) { /* head part is not full */
		handle({});
		return;
	      }
	      ++part_num;
	      ofs = 0;
	    list();
	    }));
      } else {
	handle({});
	return;
      }
    }
  };

  template<typename CT>
  auto list(int max_entries,
	    std::optional<std::string_view> markstr,
	    CT&& ct) {
    ba::async_completion<CT, void(bs::error_code,
				  std::vector<list_entry>, bool)> init(ct);
    std::unique_lock l(m);
    std::int64_t part_num = info.tail_part_num;
    l.unlock();
    std::uint64_t ofs = 0;
    auto a = ba::get_associated_allocator(init.completion_handler);
    auto e = ba::get_associated_executor(init.completion_handler);

    if (markstr) {
      auto marker = to_marker(*markstr);
      if (!marker) {
	ldout(r->cct(), 0) << __func__
			   << "(): failed to parse marker (" << *markstr
			   << ")" << dendl;
	e.post(ca::bind_handler(std::move(init.completion_handler),
				errc::invalid_marker,
				std::vector<list_entry>{}, false), a);
	return init.result.get();
      }
      part_num = marker->num;
      ofs = marker->ofs;
    }

    using handler_type = decltype(init.completion_handler);
    auto ls = ceph::allocate_unique<Lister<handler_type>>(
      a, this, part_num, ofs, max_entries,
      std::move(init.completion_handler));
    ls.release()->list();
    return init.result.get();
  }

  template<typename Handler>
  class Trimmer {
    FIFO* f;
    std::int64_t part_num;
    std::uint64_t ofs;
    Handler handler;
    std::int64_t pn;
    int i = 0;

    void handle(bs::error_code ec) {
      auto h = std::move(handler);

      FIFO::assoc_delete(h, this);
      return std::move(h)(ec);
    }

    void update() {
      std::unique_lock l(f->m);
      auto objv = f->info.version;
      l.unlock();
      auto a = ba::get_associated_allocator(handler);
      auto e = ba::get_associated_executor(handler, f->get_executor());
      f->_update_meta(
	fifo::update{}.tail_part_num(part_num),
	objv,
	ca::bind_ea(
	  e, a,
	  [this, t = std::unique_ptr<Trimmer>(this)](bs::error_code ec,
						     bool canceled) mutable {
	    t.release();
	    if (canceled)
	      if (i >= MAX_RACE_RETRIES) {
		ldout(f->r->cct(), 0)
		  << "ERROR: " << __func__
		  << "(): race check failed too many times, likely a bug"
		  << dendl;
		handle(errc::raced);
		return;
	      }
	    std::unique_lock l(f->m);
	    auto tail_part_num = f->info.tail_part_num;
	    l.unlock();
	    if (tail_part_num < part_num) {
	      ++i;
	      update();
	      return;
	    }
	    handle({});
	    return;
	  }));
    }

  public:
    Trimmer(FIFO* f, std::int64_t part_num, std::uint64_t ofs,
	    Handler&& handler)
      : f(f), part_num(part_num), ofs(ofs), handler(std::move(handler)) {
      std::unique_lock l(f->m);
      pn = f->info.tail_part_num;
    }

    void trim() {
      auto a = ba::get_associated_allocator(handler);
      auto e = ba::get_associated_executor(handler, f->get_executor());
      if (pn < part_num) {
	std::unique_lock l(f->m);
	auto max_part_size = f->info.params.max_part_size;
	l.unlock();
	f->trim_part(
	  pn, max_part_size, std::nullopt,
	  ca::bind_ea(
	    e, a,
	    [t = std::unique_ptr<Trimmer>(this),
	     this](bs::error_code ec) mutable {
	      t.release();
	      if (ec && ec != bs::errc::no_such_file_or_directory) {
		ldout(f->r->cct(), 0)
		  << __func__ << "(): ERROR: trim_part() on part="
		  << pn << " returned ec=" << ec.message() << dendl;
		handle(ec);
		return;
	      }
	      ++pn;
	      trim();
	    }));
	return;
      }
      f->trim_part(
	part_num, ofs, std::nullopt,
	ca::bind_ea(
	  e, a,
	  [t = std::unique_ptr<Trimmer>(this),
	    this](bs::error_code ec) mutable {
	    t.release();
	    if (ec && ec != bs::errc::no_such_file_or_directory) {
	      ldout(f->r->cct(), 0)
		<< __func__ << "(): ERROR: trim_part() on part=" << part_num
		<< " returned ec=" << ec.message() << dendl;
	      handle(ec);
	      return;
	    }
	    std::unique_lock l(f->m);
	    auto tail_part_num = f->info.tail_part_num;
	    l.unlock();
	    if (part_num <= tail_part_num) {
	      /* don't need to modify meta info */
	      handle({});
	      return;
	    }
	    update();
	  }));
    }
  };

  template<typename CT>
  auto trim(std::string_view markstr, CT&& ct) {
    auto m = to_marker(markstr);
    ba::async_completion<CT, void(bs::error_code)> init(ct);
    auto a = ba::get_associated_allocator(init.completion_handler);
    auto e = ba::get_associated_executor(init.completion_handler);
    if (!m) {
      ldout(r->cct(), 0) << __func__ << "(): failed to parse marker: marker="
			 << markstr << dendl;
      e.post(ca::bind_handler(std::move(init.completion_handler),
			      errc::invalid_marker), a);
      return init.result.get();
    } else {
      using handler_type = decltype(init.completion_handler);
      auto t = ceph::allocate_unique<Trimmer<handler_type>>(
	a, this, m->num, m->ofs, std::move(init.completion_handler));
      t.release()->trim();
    }
    return init.result.get();
  }

  template<typename Handler>
  class PartInfoGetter {
    Handler handler;
    using allocator_type = boost::asio::associated_allocator_t<Handler>;
    using decoder_type = ExecDecodeCB<fifo::op::get_part_info_reply>;
    using decoder_ptr = ceph::allocated_unique_ptr<decoder_type, allocator_type>;
    decoder_ptr decoder;
  public:
    PartInfoGetter(Handler&& handler, decoder_ptr&& decoder)
      : handler(std::move(handler)), decoder(std::move(decoder)) {}

    void operator ()(bs::error_code ec) {
      if (!ec) {
        ec = decoder->ec;
      }
      auto reply = std::move(decoder->result);
      decoder.reset(); // free handler-allocated memory before dispatching

      auto p = ca::bind_handler(std::move(handler),
				ec, std::move(reply.header));
      std::move(p)();
    }
  };

  template<typename CT>
  auto get_part_info(int64_t part_num,
		     CT&& ct) {

    ba::async_completion<CT, void(bs::error_code, part_info)> init(ct);
    fifo::op::get_part_info gpi;
    cb::list in;
    encode(gpi, in);
    ReadOp op;
    auto e = ba::get_associated_executor(init.completion_handler,
					 get_executor());
    auto a = ba::get_associated_allocator(init.completion_handler);
    auto reply = ceph::allocate_unique<
      ExecDecodeCB<fifo::op::get_part_info_reply>>(a);

    op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in,
	    std::ref(*reply));
    std::unique_lock l(m);
    auto part_oid = info.part_oid(part_num);
    l.unlock();
    r->execute(part_oid, ioc, std::move(op), nullptr,
	       ca::bind_ea(e, a,
			   PartInfoGetter(std::move(init.completion_handler),
					  std::move(reply))));
    return init.result.get();
  }

  using executor_type = ba::executor;
  ba::executor get_executor() const {
    return executor;
  }
};

template<typename Handler>
class JournalProcessor {
private:
  FIFO* const fifo;
  Handler handler;
  using Allocator = std::pmr::polymorphic_allocator<JournalProcessor>;

  std::vector<fifo::journal_entry> processed;
  std::multimap<std::int64_t, fifo::journal_entry> journal;
  std::multimap<std::int64_t, fifo::journal_entry>::iterator iter;
  std::int64_t new_tail;
  std::int64_t new_head;
  std::int64_t new_max;
  int race_retries = 0;

  template<typename CT>
  auto create_part(int64_t part_num, std::string_view tag, CT&& ct) {
    WriteOp op;
    op.create(false); /* We don't need exclusivity, part_init ensures
			 we're creating from the  same journal entry. */
    std::unique_lock l(fifo->m);
    part_init(op, tag, fifo->info.params);
    auto oid = fifo->info.part_oid(part_num);
    l.unlock();
    return fifo->r->execute(oid, fifo->ioc,
			    std::move(op), std::forward<CT>(ct));
  }

  template<typename CT>
  auto remove_part(int64_t part_num, std::string_view tag, CT&& ct) {
    WriteOp op;
    op.remove();
    std::unique_lock l(fifo->m);
    auto oid = fifo->info.part_oid(part_num);
    l.unlock();
    return fifo->r->execute(oid, fifo->ioc,
			    std::move(op), std::forward<CT>(ct));
  }

  template<typename PP>
  void process_journal_entry(const fifo::journal_entry& entry,
			     PP&& pp) {
    switch (entry.op) {
    case fifo::journal_entry::Op::unknown:
      std::move(pp)(errc::inconsistency);
      return;
      break;

    case fifo::journal_entry::Op::create:
      create_part(entry.part_num, entry.part_tag, std::move(pp));
      return;
      break;
    case fifo::journal_entry::Op::set_head:
      ba::post(ba::get_associated_executor(handler, fifo->get_executor()),
		      [pp = std::move(pp)]() mutable {
			std::move(pp)(bs::error_code{});
		      });
      return;
      break;
    case fifo::journal_entry::Op::remove:
      remove_part(entry.part_num, entry.part_tag, std::move(pp));
      return;
      break;
    }
    std::move(pp)(errc::inconsistency);
    return;
  }

  auto journal_entry_finisher(const fifo::journal_entry& entry) {
    auto a = ba::get_associated_allocator(handler);
    auto e = ba::get_associated_executor(handler, fifo->get_executor());
    return
      ca::bind_ea(
	e, a,
	[t = std::unique_ptr<JournalProcessor>(this), this,
	 entry](bs::error_code ec) mutable {
	  t.release();
	  if (entry.op == fifo::journal_entry::Op::remove &&
	      ec == bs::errc::no_such_file_or_directory)
	    ec.clear();

	  if (ec) {
	    ldout(fifo->r->cct(), 0)
	      << __func__
	      << "(): ERROR: failed processing journal entry for part="
	      << entry.part_num << " with error " << ec.message()
	      << " Bug or inconsistency." << dendl;
	    handle(errc::inconsistency);
	    return;
	  } else {
	    switch (entry.op) {
	    case fifo::journal_entry::Op::unknown:
	      // Can't happen. Filtered out in process_journal_entry.
	      abort();
	      break;

	    case fifo::journal_entry::Op::create:
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
	      if (entry.part_num >= new_tail) {
		new_tail = entry.part_num + 1;
	      }
	      break;
	    }
	    processed.push_back(entry);
	  }
	  ++iter;
	  process();
	});
  }

  struct JournalPostprocessor {
    std::unique_ptr<JournalProcessor> j_;
    bool first;
    void operator ()(bs::error_code ec, bool canceled) {
      std::optional<int64_t> tail_part_num;
      std::optional<int64_t> head_part_num;
      std::optional<int64_t> max_part_num;

      auto j = j_.release();

      if (!first && !ec && !canceled) {
	j->handle({});
	return;
      }

      if (canceled) {
	if (j->race_retries >= MAX_RACE_RETRIES) {
	  ldout(j->fifo->r->cct(), 0) << "ERROR: " << __func__ <<
	    "(): race check failed too many times, likely a bug" << dendl;
	  j->handle(errc::raced);
	  return;
	}

	++j->race_retries;

	std::vector<fifo::journal_entry> new_processed;
	std::unique_lock l(j->fifo->m);
	for (auto& e : j->processed) {
	  auto jiter = j->fifo->info.journal.find(e.part_num);
	  /* journal entry was already processed */
	  if (jiter == j->fifo->info.journal.end() ||
	      !(jiter->second == e)) {
	    continue;
	  }
	  new_processed.push_back(e);
	}
	j->processed = std::move(new_processed);
      }

      std::unique_lock l(j->fifo->m);
      auto objv = j->fifo->info.version;
      if (j->new_tail > j->fifo->info.tail_part_num) {
	tail_part_num = j->new_tail;
      }

      if (j->new_head > j->fifo->info.head_part_num) {
	head_part_num = j->new_head;
      }

      if (j->new_max > j->fifo->info.max_push_part_num) {
	max_part_num = j->new_max;
      }
      l.unlock();

      if (j->processed.empty() &&
	  !tail_part_num &&
	  !max_part_num) {
	/* nothing to update anymore */
	j->handle({});
	return;
      }
      auto a = ba::get_associated_allocator(j->handler);
      auto e = ba::get_associated_executor(j->handler, j->fifo->get_executor());
      j->fifo->_update_meta(fifo::update{}
			    .tail_part_num(tail_part_num)
		            .head_part_num(head_part_num)
		            .max_push_part_num(max_part_num)
			    .journal_entries_rm(j->processed),
                            objv,
                            ca::bind_ea(
			      e, a,
			      JournalPostprocessor{j, false}));
      return;
    }

    JournalPostprocessor(JournalProcessor* j, bool first)
      : j_(j), first(first) {}
  };

  void postprocess() {
    if (processed.empty()) {
      handle({});
      return;
    }
    JournalPostprocessor(this, true)({}, false);
  }

  void handle(bs::error_code ec) {
    auto e = ba::get_associated_executor(handler, fifo->get_executor());
    auto a = ba::get_associated_allocator(handler);
    auto h = std::move(handler);
    FIFO::assoc_delete(h, this);
    e.dispatch(ca::bind_handler(std::move(h), ec), a);
    return;
  }

public:

  JournalProcessor(FIFO* fifo, Handler&& handler)
    : fifo(fifo), handler(std::move(handler)) {
    std::unique_lock l(fifo->m);
    journal = fifo->info.journal;
    iter = journal.begin();
    new_tail = fifo->info.tail_part_num;
    new_head = fifo->info.head_part_num;
    new_max = fifo->info.max_push_part_num;
  }

  JournalProcessor(const JournalProcessor&) = delete;
  JournalProcessor& operator =(const JournalProcessor&) = delete;
  JournalProcessor(JournalProcessor&&) = delete;
  JournalProcessor& operator =(JournalProcessor&&) = delete;

  void process() {
    if (iter != journal.end()) {
      const auto entry = iter->second;
      process_journal_entry(entry,
			    journal_entry_finisher(entry));
      return;
    } else {
      postprocess();
      return;
    }
  }
};
}

#endif // CEPH_RADOS_CLS_FIFIO_H
