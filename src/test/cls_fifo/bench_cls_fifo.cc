// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <cerrno>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <string_view>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <boost/program_options.hpp>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <spawn/spawn.hpp>

#include "include/neorados/RADOS.hpp"

#include "neorados/cls/fifo.h"

namespace ba = boost::asio;
namespace bs = boost::system;
namespace bpo = boost::program_options;
namespace cb = ceph::buffer;
namespace R = neorados;
namespace RCf = neorados::cls::fifo;
namespace s = spawn;
namespace sc = std::chrono;

namespace {
static constexpr auto PUSH = 0x01;
static constexpr auto PULL = 0x02;
static constexpr auto BOTH = PUSH | PULL;
static constexpr auto CLEAN = 0x04;

struct benchmark {
  std::uint32_t entries = 0;
  sc::duration<double> elapsed = 0ns;

  std::uint64_t ratio() const {
    return entries/std::max(elapsed,
			    sc::duration<double>(1ns)).count();
  }
  benchmark() = default;
  benchmark(std::uint32_t entries, sc::duration<double> elapsed)
    : entries(entries), elapsed(elapsed) {}
};

benchmark push(RCf::FIFO& f, const std::uint32_t count,
	       const std::uint32_t entry_size, const std::uint32_t push_entries,
	       s::yield_context y)
{
  cb::list entry;
  entry.push_back(cb::create_small_page_aligned(entry_size));
  entry.zero();

  std::vector entries(std::min(count, push_entries), entry);
  auto remaining = count;
  auto start = sc::steady_clock::now();
  while (remaining) {
    if (entries.size() > remaining) {
      entries.resize(remaining);
    }
    f.push(entries, y);
    remaining -= entries.size();
  }
  auto finish = sc::steady_clock::now();
  return benchmark(count, (finish - start));
}

benchmark pull(RCf::FIFO& f, const std::uint32_t count,
	       const std::uint32_t pull_entries, s::yield_context y)
{
  auto remaining = count;
  std::uint32_t got = 0;

  auto start = sc::steady_clock::now();
  while (remaining) {
    auto [result, more] = f.list(std::min(remaining, pull_entries),
				 std::nullopt, y);
    if (result.empty())
      break;
    got += result.size();
    remaining -= result.size();
    f.trim(result.back().marker, y);
  }
  auto finish = sc::steady_clock::now();
  return benchmark(got, (finish - start));
}

void concurpull(const std::string& oid, const std::int64_t pool,
		const std::uint32_t count, const std::uint32_t pull_entries,
		std::promise<benchmark> notify, const bool* const exit_early)
{
  ba::io_context c;
  benchmark bench;
  std::exception_ptr ex;
  s::spawn(
    c,
    [&](s::yield_context y) {
      try {
	auto r = R::RADOS::Builder{}.build(c, y);
	R::IOContext ioc(pool);
	auto f = RCf::FIFO::open(r, ioc, oid, y);
	auto remaining = count;
	std::uint32_t got = 0;

	auto start = sc::steady_clock::now();
	while (remaining) {
	  if (*exit_early) break;
	  auto [result, more] =
	    f->list(std::min(remaining, pull_entries), std::nullopt, y);
	  if (result.empty()) {
	    // We just keep going assuming they'll push more.
	    continue;
	  }
	  got += result.size();
	  remaining -= result.size();
	  if (*exit_early) break;
	  f->trim(result.back().marker, y);
	}
	auto finish = sc::steady_clock::now();
	bench.entries = got;
	bench.elapsed = finish - start;
      } catch (const std::exception&) {
	ex = std::current_exception();
      }
    });
  c.run();
  if (ex) {
    notify.set_exception(std::current_exception());
  } else {
    notify.set_value(bench);
  }
}

void clean(R::RADOS& r, const R::IOContext& ioc, RCf::FIFO& f,
	   s::yield_context y)
{
  f.read_meta(y);
  const auto info = f.meta();
  if (info.head_part_num > -1) {
    for (auto i = info.tail_part_num; i <= info.head_part_num; ++i) {
      R::WriteOp op;
      op.remove();
      r.execute(info.part_oid(i), ioc, std::move(op), y);
    }
  }
  R::WriteOp op;
  op.remove();
  r.execute(info.id, ioc, std::move(op), y);
}
}

int main(int argc, char* argv[])
{
  const std::string_view prog(argv[0]);
  std::string command;
  try {
    std::uint32_t count = 0;
    std::string oid;
    std::string pool;
    std::uint32_t entry_size = 0;
    std::uint32_t push_entries = 0;
    std::uint32_t pull_entries = 0;
    std::uint64_t max_part_size = 0;
    std::uint64_t max_entry_size = 0;

    bpo::options_description desc(fmt::format("{} options", prog));
    desc.add_options()
      ("help", "show help")
      ("oid", bpo::value<std::string>(&oid)->default_value("fifo"s),
       "the base oid for the fifo")
      ("pool", bpo::value<std::string>(&pool)->default_value("fifo_benchmark"s),
       "the base oid for the fifo")
      ("count", bpo::value<std::uint32_t>(&count)->default_value(1024),
       "total count of items")
      ("entry-size", bpo::value<std::uint32_t>(&entry_size)->default_value(64),
       "size of entries to push")
      ("push-entries",
       bpo::value<std::uint32_t>(&push_entries)
       ->default_value(512), "entries to push per call")
      ("max-part-size", bpo::value<std::uint64_t>(&max_part_size)
       ->default_value(RCf::default_max_part_size),
       "maximum entry size allowed by FIFO")
      ("max-entry-size", bpo::value<std::uint64_t>(&max_entry_size)
       ->default_value(RCf::default_max_entry_size),
       "maximum entry size allowed by FIFO")
      ("pull-entries",
       bpo::value<uint32_t>(&pull_entries)
       ->default_value(512), "entries to pull per call")
      ("command", bpo::value<std::string>(&command),
       "the operation to perform");

    bpo::positional_options_description p;
    p.add("command", 1);

    bpo::variables_map vm;

    bpo::store(bpo::command_line_parser(argc, argv).
	       options(desc).positional(p).run(), vm);

    bpo::notify(vm);

    if (vm.count("help")) {
      fmt::print(std::cout, "{}", desc);
      fmt::print(std::cout, "\n{} commands:\n", prog);
      fmt::print(std::cout, "    push\t\t\t push entries into fifo\n");
      fmt::print(std::cout, "    pull\t\t\t retrieve and trim entries\n");
      fmt::print(std::cout, "    both\t\t\t both at once, in two threads\n");

      return 0;
    }


    if (vm.find("command") == vm.end()) {
      fmt::print(std::cerr, "{}: a command is required\n", prog);
      return 1;
    }

    int op = 0;
    if (command == "push"s) {
      op = PUSH;
    } else if (command == "pull"s) {
      op = PULL;
    } else if (command == "both"s) {
      op = BOTH;
    } else if (command == "clean"s) {
      op = CLEAN;
    } else {
      fmt::print(std::cerr, "{}: {} is not a valid command\n",
		 prog, command);
      return 1;
    }

    if (!(op & PULL) && !vm["pull-entries"].defaulted()) {
      fmt::print(std::cerr, "{}: pull-entries is only meaningful when pulling\n",
		 prog);
      return 1;
    }

    if (!(op & PUSH)) {
      for (const auto& p : { "entry-size"s, "push-entries"s, "max-part-size"s,
	    "max-entry-size"s }) {
	if (!vm[p].defaulted()) {
	  fmt::print(std::cerr, "{}: {} is only meaningful when pushing\n",
		     prog, p);
	  return 1;
	}
      }
    }

    if (!(op & BOTH) && !vm["count"].defaulted()) {
      fmt::print(std::cerr, "{}: count is only meaningful when pulling, pushing, or both\n",
		 prog);
      return 1;
    }


    if (count == 0) {
      fmt::print(std::cerr, "{}: count must be nonzero\n", prog);
      return 1;
    }

    if ((op & PULL) && (pull_entries == 0)) {
      fmt::print(std::cerr,
		 "{}: pull-entries must be nonzero\n", prog);
      return 1;
    }

    if (op & PUSH) {
      if (entry_size == 0) {
	fmt::print(std::cerr, "{}: entry-size must be nonzero\n", prog);
	return 1;
      }
      if (push_entries== 0) {
	fmt::print(std::cerr, "{}: push-entries must be nonzero\n", prog);
	return 1;
      }
      if (max_entry_size == 0) {
	fmt::print(std::cerr, "{}: max-entry-size must be nonzero\n", prog);
	return 1;
      }
      if (max_part_size == 0) {
	fmt::print(std::cerr, "{}: max-part-size must be nonzero\n", prog);
	return 1;
      }
      if (entry_size > max_entry_size) {
	fmt::print(std::cerr,
		   "{}: entry-size may not be greater than max-entry-size\n",
		   prog);
	return 1;
      }
      if (max_entry_size >= max_part_size) {
	fmt::print(std::cerr,
		   "{}: max-entry-size may be less than max-part-size\n",
		   prog);
	return 1;
      }
    }

    ba::io_context c;
    benchmark pushmark, pullmark;
    s::spawn(
      c,
      [&](s::yield_context y) {
	auto r = R::RADOS::Builder{}.build(c, y);
	bs::error_code ec;
	std::int64_t pid;
	pid = r.lookup_pool(pool, y[ec]);
	if (ec) {
	  r.create_pool(pool, std::nullopt, y);
	  pid = r.lookup_pool(pool, y);
	}
	const R::IOContext ioc(pid);
	auto f = RCf::FIFO::create(r, ioc, oid, y, std::nullopt,
				   std::nullopt, false, max_part_size,
				   max_entry_size);

	switch (op) {
	case PUSH:
	  pushmark = push(*f, count, entry_size, push_entries, y);
	  break;

	case PULL:
	  pullmark = pull(*f, count, pull_entries, y);
	  break;

	case BOTH: {
	  std::promise<benchmark> notify;
	  bool exit_early = false;

	  auto notifier = notify.get_future();
	  std::thread t(concurpull, oid, pid, count, pull_entries,
			std::move(notify), &exit_early);
	  t.detach();
	  try {
	    pushmark = push(*f, count, entry_size, push_entries, y);
	  } catch (const std::exception&) {
	    exit_early = true;
	    notifier.wait();
	    throw;
	  }
	  pullmark = notifier.get();
	}
	}

	if (op & CLEAN)
	  clean(r, ioc, *f, y);
      });
    c.run();
    if (op & PUSH) {
      fmt::print("Pushed {} in {} at {}/s\n",
		 pushmark.entries, pushmark.elapsed, pushmark.ratio());
    }
    if (op & PULL) {
      if (pullmark.entries == count) {
	fmt::print(std::cout, "Pulled {} in {} at {}/s\n",
		   pullmark.entries, pullmark.elapsed, pullmark.ratio());
      } else {
	fmt::print(std::cout, "Pulled {} (of {} requested), in {} at {}/s\n",
		   pullmark.entries, count, pullmark.elapsed, pullmark.ratio());
      }
    }
  } catch (const std::exception& e) {
    if (command.empty()) {
      fmt::print(std::cerr, "{}: {}\n", prog, e.what());
    } else {
      fmt::print(std::cerr, "{}: {}: {}\n", prog, command, e.what());
    }
    return 1;
  }

  return 0;
}
