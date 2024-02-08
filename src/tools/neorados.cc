// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <cassert>
#include <coroutine>
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>

#include <boost/io/ios_state.hpp>
#include <boost/program_options.hpp>
#include <boost/system/system_error.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "include/buffer.h" // :(

#include "include/neorados/RADOS.hpp"

using namespace std::literals;

namespace ba = boost::asio;
namespace bs = boost::system;
namespace R = neorados;

std::string verstr(const std::tuple<uint32_t, uint32_t, uint32_t>& v)
{
  const auto [maj, min, p] = v;
  return fmt::format("v{}.{}.{}", maj, min, p);
}

template<typename V>
void printseq(const V& v, std::ostream& m)
{
  std::for_each(v.cbegin(), v.cend(),
		[&m](const auto& e) {
		  fmt::print(m, "{}\n", e);
		});
}

template<typename V, typename F>
void printseq(const V& v, std::ostream& m, F&& f)
{
  std::for_each(v.cbegin(), v.cend(),
		[&m, &f](const auto& e) {
		  fmt::print(m, "{}\n", f(e));
		});
}

ba::awaitable<R::IOContext> lookup_pool(R::RADOS& r, const std::string& pname)
{
  bs::error_code ec;
  auto p = co_await r.lookup_pool(pname,
				  ba::redirect_error(ba::use_awaitable, ec));
  if (ec)
    throw bs::system_error(
      ec, fmt::format("when looking up '{}'", pname));
  co_return R::IOContext(p);
}


ba::awaitable<void> lspools(R::RADOS& r, const std::vector<std::string>&)
{
  const auto l = co_await r.list_pools(ba::use_awaitable);
  printseq(l, std::cout, [](const auto& p) -> const std::string& {
			   return p.second;
			 });
  co_return;
}


ba::awaitable<void> ls(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  const auto pool = (co_await lookup_pool(r, pname)).set_ns(R::all_nspaces);
  std::vector<R::Entry> ls;
  R::Cursor next = R::Cursor::begin();
  bs::error_code ec;
  do {
    std::tie(ls, next) =
      co_await r.enumerate_objects(pool, next, R::Cursor::end(), 1000, {},
				   ba::redirect_error(ba::use_awaitable, ec));
    if (ec)
      throw bs::system_error(ec, fmt::format("when listing {}", pname));
    printseq(ls, std::cout);
    ls.clear();
  } while (next != R::Cursor::end());
  co_return;
}

ba::awaitable<void> mkpool(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  bs::error_code ec;
  co_await r.create_pool(pname, std::nullopt,
			 ba::redirect_error(ba::use_awaitable, ec));
  if (ec)
    throw bs::system_error(ec, fmt::format("when creating pool '{}'", pname));
  co_return;
}

ba::awaitable<void> rmpool(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  bs::error_code ec;
  co_await r.delete_pool(pname, ba::redirect_error(ba::use_awaitable, ec));
  if (ec)
    throw bs::system_error(ec, fmt::format("when removing pool '{}'", pname));
  co_return;
}

ba::awaitable<void> create(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  const R::Object obj = p[1];
  const auto pool = co_await lookup_pool(r, pname);

  bs::error_code ec;
  R::WriteOp op;
  op.create(true);
  co_await r.execute(obj, pool, std::move(op),
		     ba::redirect_error(ba::use_awaitable, ec));
  if (ec)
    throw bs::system_error(ec,
			   fmt::format(
			     "when creating object '{}' in pool '{}'",
			     obj, pname));
  co_return;
}

inline constexpr std::size_t io_size = 4 << 20;

ba::awaitable<void> write(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  const R::Object obj(p[1]);
  const auto pool = co_await lookup_pool(r, pname);

  bs::error_code ec;
  std::unique_ptr<char[]> buf = std::make_unique<char[]>(io_size);
  std::size_t off = 0;
  boost::io::ios_exception_saver ies(std::cin);

  std::cin.exceptions(std::istream::badbit);
  std::cin.clear();

  while (!std::cin.eof()) {
    auto curoff = off;
    std::cin.read(buf.get(), io_size);
    auto len = std::cin.gcount();
    off += len;
    if (len == 0)
      break; // Nothin' to do.

    ceph::buffer::list bl;
    bl.append(buffer::create_static(len, buf.get()));
    R::WriteOp op;
    op.write(curoff, std::move(bl));
    co_await r.execute(obj, pool, std::move(op),
		       ba::redirect_error(ba::use_awaitable, ec));

    if (ec)
      throw bs::system_error(ec, fmt::format(
			       "when writing object '{}' in pool '{}'",
			       obj, pname));
  }
  co_return;
}

ba::awaitable<void> read(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  const R::Object obj(p[1]);
  const auto pool = co_await lookup_pool(r, pname);

  bs::error_code ec;
  std::uint64_t len;
  {
    R::ReadOp op;
    op.stat(&len, nullptr);
    co_await r.execute(obj, pool, std::move(op),
		       nullptr, ba::redirect_error(ba::use_awaitable, ec));
    if (ec)
      throw bs::system_error(
	ec,
	fmt::format("when getting length of object '{}' in pool '{}'",
		    obj, pname));
  }

  std::size_t off = 0;
  ceph::buffer::list bl;
  while (auto toread = std::min(len - off, io_size)) {
    R::ReadOp op;
    op.read(off, toread, &bl);
    co_await r.execute(obj, pool, std::move(op), nullptr,
		       ba::redirect_error(ba::use_awaitable, ec));
    if (ec)
      throw bs::system_error(
	ec,
	fmt::format("when reading from object '{}' in pool '{}'",
		    obj, pool));

    off += bl.length();
    bl.write_stream(std::cout);
    bl.clear();
  }
  co_return;
}

ba::awaitable<void> rm(R::RADOS& r, const std::vector<std::string>& p)
{
  const auto& pname = p[0];
  const R::Object obj = p[1];
  const auto pool = co_await lookup_pool(r, pname);

  bs::error_code ec;
  R::WriteOp op;
  op.remove();
  co_await r.execute(obj, pool, std::move(op),
		     ba::redirect_error(ba::use_awaitable, ec));
  if (ec)
    throw bs::system_error(ec, fmt::format(
			     "when removing object '{}' in pool '{}'",
			     obj, pname));
  co_return;
}

static constexpr auto version = std::make_tuple(0ul, 0ul, 1ul);

using cmdfunc =
  ba::awaitable<void> (*)(R::RADOS& r, const std::vector<std::string>& p);

struct cmdesc {
  std::string_view name;
  std::size_t arity;
  cmdfunc f;
  std::string_view usage;
  std::string_view desc;
};

const std::array commands = {
  // Pools operations ;)

  cmdesc{ "lspools"sv,
	  0, &lspools,
	  ""sv,
	  "List all pools"sv },

  // Pool operations

  cmdesc{ "ls"sv,
	  1, &ls,
	  "POOL"sv,
	  "list all objects in POOL"sv },
  cmdesc{ "mkpool"sv,
	  1, &mkpool,
	  "POOL"sv,
	  "create POOL"sv },
  cmdesc{ "rmpool"sv,
	  1, &rmpool,
	  "POOL"sv,
	  "remove POOL"sv },

  // Object operations

  cmdesc{ "create"sv,
	  2, &create,
	  "POOL OBJECT"sv,
	  "exclusively create OBJECT in POOL"sv },
  cmdesc{ "write"sv,
	  2, &write,
	  "POOL OBJECT"sv,
	  "write to OBJECT in POOL from standard input"sv },
  cmdesc{ "read"sv,
	  2, &read,
	  "POOL OBJECT"sv,
	  "read contents of OBJECT in POOL to standard out"sv },
  cmdesc{ "rm"sv,
	  2, &rm,
	  "POOL OBJECT"sv,
	  "remove OBJECT in POOL"sv }
};

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<boost::program_options::options_description> : fmt::ostream_formatter {};
#endif // FMT_VERSION

int main(int argc, char* argv[])
{
  const std::string_view prog(argv[0]);
  std::string command;
  namespace po = boost::program_options;
  try {
    std::vector<std::string> parameters;

    po::options_description desc(fmt::format("{} options", prog));
    desc.add_options()
      ("help", "show help")
      ("version", "show version")
      ("command", po::value<std::string>(&command), "the operation to perform")
      ("parameters", po::value<std::vector<std::string>>(&parameters),
       "parameters to the command");

    po::positional_options_description p;
    p.add("command", 1);
    p.add("parameters", -1);

    po::variables_map vm;

    po::store(po::command_line_parser(argc, argv).
	      options(desc).positional(p).run(), vm);

    po::notify(vm);

    if (vm.count("help")) {
      fmt::print("{}", desc);
      fmt::print("Commands:\n");
      for (const auto& cmd : commands) {
	fmt::print("    {} {}{}{}\n",
		   cmd.name, cmd.usage,
		   cmd.name.length() + cmd.usage.length() < 13 ?
		   "\t\t"sv : "\t"sv,
		   cmd.desc);
      }
      return 0;
    }

    if (vm.count("version")) {
      fmt::print(
	"{}: RADOS command exerciser, {},\n"
	"RADOS library version {}\n"
	"Copyright (C) 2019 Red Hat <contact@redhat.com>\n"
	"This is free software; you can redistribute it and/or\n"
	"modify it under the terms of the GNU Lesser General Public\n"
	"License version 2.1, as published by the Free Software\n"
	"Foundation.  See file COPYING.\n", prog,
	verstr(version), verstr(R::RADOS::version()));
      return 0;
    }

    if (vm.find("command") == vm.end()) {
      fmt::print(std::cerr, "{}: a command is required\n", prog);
      return 1;
    }

    ba::io_context c;

    if (auto ci = std::find_if(commands.begin(), commands.end(),
			       [&command](const cmdesc& c) {
				 return c.name == command;
			       }); ci != commands.end()) {
      if (parameters.size() < ci->arity) {
	fmt::print(std::cerr, "{}: {}: too few arguments\n\t{} {}\n",
		   prog, command, ci->name, ci->usage);
	return 1;
      }
      if (parameters.size() > ci->arity) {
	fmt::print(std::cerr, "{}: {}: too many arguments\n\t{} {}\n",
		   prog, command, ci->name, ci->usage);
	return 1;
      }
      ba::co_spawn(c,
		   [&]() -> ba::awaitable<void> {
		     auto r = co_await R::RADOS::Builder{}.build(
		       c, ba::use_awaitable);
		     co_await ci->f(r, parameters);
		   }, [](std::exception_ptr e) {
		     if (e) std::rethrow_exception(e);
		   });
    } else {
      fmt::print(std::cerr, "{}: {}: unknown command\n", prog, command);
      return 1;
    }
    c.run();
  } catch (const std::exception& e) {
    fmt::print(std::cerr, "{}: {}: {}\n", prog, command, e.what());
    return 1;
  }

  return 0;
}
