// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef NEORADOS_RADOS_HPP
#define NEORADOS_RADOS_HPP

#include <cstddef>
#include <memory>
#include <tuple>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include <boost/asio.hpp>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/uuid/uuid.hpp>

#include <boost/system/error_code.hpp>

#include <fmt/format.h>
#if FMT_VERSION < 90000
#include <fmt/ostream.h>
#endif // FMT_VERSION < 90000


// Will be in C++20!

#include "include/expected.hpp"

// Had better be in C++20. Why is this not in Boost?

#include "include/function2.hpp"

// Things broken out so we can decode them in Objecter.

#include "include/neorados/RADOS_Decodable.hpp"

// Needed for type erasure and template support. We can't really avoid
// it.

#include "common/async/completion.h"

// These are needed for RGW, but in general as a 'shiny new interface'
// we should try to use forward declarations and provide standard alternatives.

#include "include/common_fwd.h"

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"

#include "common/ceph_time.h"

namespace neorados {
class Object;
class IOContext;
}
namespace std {
template<>
struct hash<neorados::Object>;
template<>
struct hash<neorados::IOContext>;
}

namespace neorados {
namespace detail {
class Client;
}

class RADOS;

// Exists mostly so that repeated operations on the same object don't
// have to pay for the string copy to construct an object_t.

class Object final {
  friend RADOS;
  friend std::hash<Object>;

public:
  Object();
  Object(const char* s);
  Object(std::string_view s);
  Object(std::string&& s);
  Object(const std::string& s);
  ~Object();

  Object(const Object& o);
  Object& operator =(const Object& o);

  Object(Object&& o);
  Object& operator =(Object&& o);

  operator std::string_view() const;

  friend std::ostream& operator <<(std::ostream& m, const Object& o);
  friend bool operator <(const Object& lhs, const Object& rhs);
  friend bool operator <=(const Object& lhs, const Object& rhs);
  friend bool operator >=(const Object& lhs, const Object& rhs);
  friend bool operator >(const Object& lhs, const Object& rhs);

  friend bool operator ==(const Object& lhs, const Object& rhs);
  friend bool operator !=(const Object& lhs, const Object& rhs);

private:

  static constexpr std::size_t impl_size = 4 * 8;
  std::aligned_storage_t<impl_size> impl;
};

// Not the same as the librados::IoCtx, but it does gather together
// some of the same metadata. Since we're likely to do multiple
// operations in the same pool or namespace, it doesn't make sense to
// redo a bunch of lookups and string copies.

class IOContext final {
  friend RADOS;
  friend std::hash<IOContext>;

public:

  IOContext();
  explicit IOContext(std::int64_t pool);
  IOContext(std::int64_t pool, std::string_view ns, std::string_view key = {});
  ~IOContext();

  IOContext(const IOContext& rhs);
  IOContext& operator =(const IOContext& rhs);

  IOContext(IOContext&& rhs);
  IOContext& operator =(IOContext&& rhs);

  std::int64_t get_pool() const;
  explicit operator std::int64_t() const;
  IOContext& set_pool(std::int64_t pool) &;
  IOContext&& set_pool(std::int64_t pool) &&;

  std::string_view get_ns() const;
  IOContext& set_ns(std::string_view ns) &;
  IOContext&& set_ns(std::string_view ns) &&;

  std::string_view get_key() const;
  IOContext& set_key(std::string_view key) &;
  IOContext&& set_key(std::string_view key) &&;

  std::int64_t get_hash() const;
  IOContext& set_hash(std::int64_t _hash) &;
  IOContext&& set_hash(std::int64_t _hash) &&;

  std::uint64_t get_read_snap() const;
  IOContext& set_read_snap(std::uint64_t _snapid) &;
  IOContext&& set_read_snap(std::uint64_t _snapid) &&;

  // Keep this optional since it provides the easiest way of testing
  // whether it's set or not.
  std::optional<
    std::pair<std::uint64_t,
	      std::vector<std::uint64_t>>> get_write_snap_context() const;
  IOContext& set_write_snap_context(std::optional<
				      std::pair<std::uint64_t,
				      std::vector<std::uint64_t>>> snapc) &;
  IOContext&& set_write_snap_context(std::optional<
				       std::pair<std::uint64_t,
				       std::vector<std::uint64_t>>> snapc) &&;

  bool get_full_try() const;
  IOContext& set_full_try(bool _full_try) &;
  IOContext&& set_full_try(bool _full_try) &&;

  friend std::ostream& operator <<(std::ostream& m, const IOContext& o);
  friend bool operator <(const IOContext& lhs, const IOContext& rhs);
  friend bool operator <=(const IOContext& lhs, const IOContext& rhs);
  friend bool operator >=(const IOContext& lhs, const IOContext& rhs);
  friend bool operator >(const IOContext& lhs, const IOContext& rhs);

  friend bool operator ==(const IOContext& lhs, const IOContext& rhs);
  friend bool operator !=(const IOContext& lhs, const IOContext& rhs);

private:

  static constexpr std::size_t impl_size = 16 * 8;
  std::aligned_storage_t<impl_size> impl;
};

inline constexpr std::string_view all_nspaces("\001");

enum class cmpxattr_op : std::uint8_t {
  eq  = 1,
  ne  = 2,
  gt  = 3,
  gte = 4,
  lt  = 5,
  lte = 6
};

namespace alloc_hint {
enum alloc_hint_t {
  sequential_write = 1,
  random_write = 2,
  sequential_read = 4,
  random_read = 8,
  append_only = 16,
  immutable = 32,
  shortlived = 64,
  longlived = 128,
  compressible = 256,
  incompressible = 512
};
}

class Op {
  friend RADOS;

public:

  Op(const Op&) = delete;
  Op& operator =(const Op&) = delete;
  Op(Op&&);
  Op& operator =(Op&&);
  ~Op();

  void set_excl();
  void set_failok();
  void set_fadvise_random();
  void set_fadvise_sequential();
  void set_fadvise_willneed();
  void set_fadvise_dontneed();
  void set_fadvise_nocache();

  void cmpext(uint64_t off, ceph::buffer::list&& cmp_bl, std::size_t* s);
  void cmpxattr(std::string_view name, cmpxattr_op op,
		const ceph::buffer::list& val);
  void cmpxattr(std::string_view name, cmpxattr_op op, std::uint64_t val);
  void assert_version(uint64_t ver);
  void assert_exists();
  void cmp_omap(const boost::container::flat_map<
		  std::string,
		  std::pair<ceph::buffer::list, int>>& assertions);

  void exec(std::string_view cls, std::string_view method,
	    const ceph::buffer::list& inbl,
	    ceph::buffer::list* out,
	    boost::system::error_code* ec = nullptr);
  void exec(std::string_view cls, std::string_view method,
	    const ceph::buffer::list& inbl,
	    fu2::unique_function<void(boost::system::error_code,
				      const ceph::buffer::list&) &&> f);
  void exec(std::string_view cls, std::string_view method,
	    const ceph::buffer::list& inbl,
	    fu2::unique_function<void(boost::system::error_code, int,
				      const ceph::buffer::list&) &&> f);
  void exec(std::string_view cls, std::string_view method,
	    const ceph::buffer::list& inbl,
	    boost::system::error_code* ec = nullptr);


  // Flags that apply to all ops in the operation vector
  void balance_reads();
  void localize_reads();
  void order_reads_writes();
  void ignore_cache();
  void skiprwlocks();
  void ignore_overlay();
  void full_try();
  void full_force();
  void ignore_redirect();
  void ordersnap();
  void returnvec();

  std::size_t size() const;
  using Signature = void(boost::system::error_code);
  using Completion = ceph::async::Completion<Signature>;

  friend std::ostream& operator <<(std::ostream& m, const Op& o);
protected:
  Op();
  static constexpr std::size_t impl_size = 85 * 8;
  std::aligned_storage_t<impl_size> impl;
};

// This class is /not/ thread-safe. If you want you can wrap it in
// something that locks it.

class ReadOp final : public Op {
  friend RADOS;

public:

  ReadOp() = default;
  ReadOp(const ReadOp&) = delete;
  ReadOp(ReadOp&&) = default;

  ReadOp& operator =(const ReadOp&) = delete;
  ReadOp& operator =(ReadOp&&) = default;

  void read(size_t off, uint64_t len, ceph::buffer::list* out,
	    boost::system::error_code* ec = nullptr);
  void get_xattr(std::string_view name, ceph::buffer::list* out,
		 boost::system::error_code* ec = nullptr);
  void get_omap_header(ceph::buffer::list*,
		       boost::system::error_code* ec = nullptr);

  void sparse_read(uint64_t off, uint64_t len,
		   ceph::buffer::list* out,
		   std::vector<std::pair<std::uint64_t, std::uint64_t>>* extents,
		   boost::system::error_code* ec = nullptr);

  void stat(std::uint64_t* size, ceph::real_time* mtime,
	    boost::system::error_code* ec = nullptr);

  void get_omap_keys(std::optional<std::string_view> start_after,
		     std::uint64_t max_return,
		     boost::container::flat_set<std::string>* keys,
		     bool* truncated,
		     boost::system::error_code* ec = nullptr);


  void get_xattrs(boost::container::flat_map<std::string,
		                             ceph::buffer::list>* kv,
		     boost::system::error_code* ec = nullptr);

  void get_omap_vals(std::optional<std::string_view> start_after,
		     std::optional<std::string_view> filter_prefix,
		     uint64_t max_return,
		     boost::container::flat_map<std::string,
		                                ceph::buffer::list>* kv,
		     bool* truncated,
		     boost::system::error_code* ec = nullptr);


  void get_omap_vals_by_keys(const boost::container::flat_set<std::string>& keys,
			     boost::container::flat_map<std::string,
			                                ceph::buffer::list>* kv,
			     boost::system::error_code* ec = nullptr);

  void list_watchers(std::vector<struct ObjWatcher>* watchers,
		     boost::system::error_code* ec = nullptr);

  void list_snaps(struct SnapSet* snaps,
		  boost::system::error_code* ec = nullptr);
};

class WriteOp final : public Op {
  friend RADOS;
public:

  WriteOp() = default;
  WriteOp(const WriteOp&) = delete;
  WriteOp(WriteOp&&) = default;

  WriteOp& operator =(const WriteOp&) = delete;
  WriteOp& operator =(WriteOp&&) = default;

  void set_mtime(ceph::real_time t);
  void create(bool exclusive);
  void write(uint64_t off, ceph::buffer::list&& bl);
  void write_full(ceph::buffer::list&& bl);
  void writesame(std::uint64_t off, std::uint64_t write_len,
		 ceph::buffer::list&& bl);
  void append(ceph::buffer::list&& bl);
  void remove();
  void truncate(uint64_t off);
  void zero(uint64_t off, uint64_t len);
  void rmxattr(std::string_view name);
  void setxattr(std::string_view name,
		ceph::buffer::list&& bl);
  void rollback(uint64_t snapid);
  void set_omap(const boost::container::flat_map<std::string,
		                                 ceph::buffer::list>& map);
  void set_omap_header(ceph::buffer::list&& bl);
  void clear_omap();
  void rm_omap_keys(const boost::container::flat_set<std::string>& to_rm);
  void set_alloc_hint(uint64_t expected_object_size,
		      uint64_t expected_write_size,
		      alloc_hint::alloc_hint_t flags);
};


struct FSStats {
  uint64_t kb;
  uint64_t kb_used;
  uint64_t kb_avail;
  uint64_t num_objects;
};

// From librados.h, maybe move into a common file. But I want to see
// if we need/want to amend/add/remove anything first.
struct PoolStats {
  /// space used in bytes
  uint64_t num_bytes;
  /// space used in KB
  uint64_t num_kb;
  /// number of objects in the pool
  uint64_t num_objects;
  /// number of clones of objects
  uint64_t num_object_clones;
  /// num_objects * num_replicas
  uint64_t num_object_copies;
  /// number of objects missing on primary
  uint64_t num_objects_missing_on_primary;
  /// number of objects found on no OSDs
  uint64_t num_objects_unfound;
  /// number of objects replicated fewer times than they should be
  /// (but found on at least one OSD)
  uint64_t num_objects_degraded;
  /// number of objects read
  uint64_t num_rd;
  /// objects read in KB
  uint64_t num_rd_kb;
  /// number of objects written
  uint64_t num_wr;
  /// objects written in KB
  uint64_t num_wr_kb;
  /// bytes originally provided by user
  uint64_t num_user_bytes;
  /// bytes passed compression
  uint64_t compressed_bytes_orig;
  /// bytes resulted after compression
  uint64_t compressed_bytes;
  /// bytes allocated at storage
  uint64_t compressed_bytes_alloc;
};

// Placement group, for PG commands
struct PG {
  uint64_t pool;
  uint32_t seed;
};

class Cursor final {
public:
  static Cursor begin();
  static Cursor end();

  Cursor();
  Cursor(const Cursor&);
  Cursor& operator =(const Cursor&);
  Cursor(Cursor&&);
  Cursor& operator =(Cursor&&);
  ~Cursor();

  friend bool operator ==(const Cursor& lhs,
			  const Cursor& rhs);
  friend bool operator !=(const Cursor& lhs,
			  const Cursor& rhs);
  friend bool operator <(const Cursor& lhs,
			 const Cursor& rhs);
  friend bool operator <=(const Cursor& lhs,
			  const Cursor& rhs);
  friend bool operator >=(const Cursor& lhs,
			  const Cursor& rhs);
  friend bool operator >(const Cursor& lhs,
			 const Cursor& rhs);

  std::string to_str() const;
  static std::optional<Cursor> from_str(const std::string& s);

private:
  struct end_magic_t {};
  Cursor(end_magic_t);
  Cursor(void*);
  friend RADOS;
  static constexpr std::size_t impl_size = 16 * 8;
  std::aligned_storage_t<impl_size> impl;
};

// Clang reports a spurious warning that a captured `this` is unused
// in the public 'wrapper' functions that construct the completion
// handler and pass it to the actual worker member functions. The `this` is
// used to call the member functions, and even doing so explicitly
// (e.g. `this->execute`) doesn't silence it.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-lambda-capture"
class RADOS final
{
public:
  static constexpr std::tuple<uint32_t, uint32_t, uint32_t> version() {
    return {0, 0, 1};
  }

  using BuildSig = void(boost::system::error_code, RADOS);
  using BuildComp = ceph::async::Completion<BuildSig>;
  class Builder {
    std::optional<std::string> conf_files;
    std::optional<std::string> cluster;
    std::optional<std::string> name;
    std::vector<std::pair<std::string, std::string>> configs;
    bool no_default_conf = false;
    bool no_mon_conf = false;

  public:
    Builder() = default;
    Builder& add_conf_file(std::string_view v);
    Builder& set_cluster(std::string_view c) {
      cluster = std::string(c);
      return *this;
    }
    Builder& set_name(std::string_view n) {
      name = std::string(n);
      return *this;
    }
    Builder& set_no_default_conf() {
      no_default_conf = true;
      return *this;
    }
    Builder& set_no_mon_conf() {
      no_mon_conf = true;
      return *this;
    }
    Builder& set_conf_option(std::string_view opt, std::string_view val) {
      configs.emplace_back(std::string(opt), std::string(val));
      return *this;
    }

    template<typename CompletionToken>
    auto build(boost::asio::io_context& ioctx, CompletionToken&& token) {
      return boost::asio::async_initiate<CompletionToken, BuildSig>(
	[&, this](auto&& handler) {
	  build(ioctx, BuildComp::create(ioctx.get_executor(),
					 std::move(handler)));
	}, token);
    }

  private:
    void build(boost::asio::io_context& ioctx,
	       std::unique_ptr<BuildComp> c);
  };


  template<typename CompletionToken>
  static auto make_with_cct(CephContext* cct,
			    boost::asio::io_context& ioctx,
			    CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, BuildSig>(
      [&](auto&& handler) {
	make_with_cct(cct, ioctx,
		      BuildComp::create(ioctx.get_executor(),
					std::move(handler)));
      }, token);
  }

  static RADOS make_with_librados(librados::Rados& rados);

  RADOS(const RADOS&) = delete;
  RADOS& operator =(const RADOS&) = delete;

  RADOS(RADOS&&);
  RADOS& operator =(RADOS&&);

  ~RADOS();

  CephContext* cct();

  using executor_type = boost::asio::io_context::executor_type;
  executor_type get_executor() const;
  boost::asio::io_context& get_io_context();

  template<typename CompletionToken>
  auto execute(const Object& o, const IOContext& ioc, ReadOp&& op,
	       ceph::buffer::list* bl,
	       CompletionToken&& token, uint64_t* objver = nullptr,
	       const blkin_trace_info* trace_info = nullptr) {
    return boost::asio::async_initiate<CompletionToken, Op::Signature>(
      [&, this](auto&& handler) {
	execute(o, ioc, std::move(op), bl,
		ReadOp::Completion::create(get_executor(),
					   std::move(handler)),
		objver, trace_info);
      }, token);
  }

  template<typename CompletionToken>
  auto execute(const Object& o, const IOContext& ioc, WriteOp&& op,
	       CompletionToken&& token, uint64_t* objver = nullptr,
	       const blkin_trace_info* trace_info = nullptr) {
    return boost::asio::async_initiate<CompletionToken, Op::Signature>(
      [&, this](auto&& handler) {
	execute(o, ioc, std::move(op),
		WriteOp::Completion::create(get_executor(),
					    std::move(handler)),
		objver, trace_info);
      }, token);
  }

  boost::uuids::uuid get_fsid() const noexcept;

  using LookupPoolSig = void(boost::system::error_code,
			     std::int64_t);
  using LookupPoolComp = ceph::async::Completion<LookupPoolSig>;
  template<typename CompletionToken>
  auto lookup_pool(std::string_view name,
		   CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, LookupPoolSig>(
      [&, this](auto&& handler) {
	lookup_pool(name, LookupPoolComp::create(get_executor(),
						 std::move(handler)));
      }, token);
  }

  std::optional<uint64_t> get_pool_alignment(int64_t pool_id);

  using LSPoolsSig = void(std::vector<std::pair<std::int64_t, std::string>>);
  using LSPoolsComp = ceph::async::Completion<LSPoolsSig>;
  template<typename CompletionToken>
  auto list_pools(CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, LSPoolsSig>(
      [this](auto&& handler) {
	list_pools(LSPoolsComp::create(get_executor(),
				       std::move(handler)));
      }, token);
  }

  using SimpleOpSig = void(boost::system::error_code);
  using SimpleOpComp = ceph::async::Completion<SimpleOpSig>;
  template<typename CompletionToken>
  auto create_pool_snap(int64_t pool, std::string_view snapName,
			CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	create_pool_snap(pool, snapName,
			 SimpleOpComp::create(get_executor(),
					      std::move(handler)));
      }, token);
  }

  using SMSnapSig = void(boost::system::error_code, std::uint64_t);
  using SMSnapComp = ceph::async::Completion<SMSnapSig>;
  template<typename CompletionToken>
  auto allocate_selfmanaged_snap(int64_t pool,
				 CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SMSnapSig>(
      [&, this](auto&& handler) {
	allocage_selfmanaged_snap(pool,
				  SMSnapComp::create(get_executor(),
						     std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto delete_pool_snap(int64_t pool, std::string_view snapName,
			CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	delete_pool_snap(pool, snapName,
			 SimpleOpComp::create(get_executor(),
					      std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto delete_selfmanaged_snap(int64_t pool, std::string_view snapName,
			       CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	delete_selfmanaged_snap(pool, snapName,
				SimpleOpComp::create(get_executor(),
						     std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto create_pool(std::string_view name, std::optional<int> crush_rule,
		   CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	create_pool(name, crush_rule,
		    SimpleOpComp::create(get_executor(),
					 std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto delete_pool(std::string_view name,
		   CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	delete_pool(name,
		    SimpleOpComp::create(get_executor(),
					 std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto delete_pool(int64_t pool,
		   CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	delete_pool(pool,
		    SimpleOpComp::create(get_executor(),
					 std::move(handler)));
      }, token);
  }

  using PoolStatSig = void(boost::system::error_code,
			   boost::container::flat_map<std::string,
			                              PoolStats>, bool);
  using PoolStatComp = ceph::async::Completion<PoolStatSig>;
  template<typename CompletionToken>
  auto stat_pools(const std::vector<std::string>& pools,
		  CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, PoolStatSig>(
      [&, this](auto&& handler) {
	stat_pools(pools,
		   PoolStatComp::create(get_executor(),
					std::move(handler)));
      }, token);
  }

  using StatFSSig = void(boost::system::error_code,
			 FSStats);
  using StatFSComp = ceph::async::Completion<StatFSSig>;
  template<typename CompletionToken>
  auto statfs(std::optional<int64_t> pool,
	      CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, StatFSSig>(
      [&, this](auto&& handler) {
	statfs(pool, StatFSComp::create(get_executor(),
					std::move(handler)));
      }, token);
  }

  using WatchCB = fu2::unique_function<void(boost::system::error_code,
					    uint64_t notify_id,
					    uint64_t cookie,
					    uint64_t notifier_id,
					    ceph::buffer::list&& bl)>;

  using WatchSig = void(boost::system::error_code ec,
			uint64_t cookie);
  using WatchComp = ceph::async::Completion<WatchSig>;
  template<typename CompletionToken>
  auto watch(const Object& o, const IOContext& ioc,
	     std::optional<std::chrono::seconds> timeout,
	     WatchCB&& cb, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, WatchSig>(
      [&, this](auto&& handler) {
	watch(o, ioc, timeout, cb, WatchComp::create(get_executor(),
						     std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto notify_ack(const Object& o,
		  const IOContext& ioc,
		  uint64_t notify_id,
		  uint64_t cookie,
		  ceph::buffer::list&& bl,
		  CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	notify_ack(o, ioc, notify_id, cookie, std::move(bl),
		   SimpleOpComp::create(get_executor(),
					std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto unwatch(std::uint64_t cookie, const IOContext& ioc,
	       CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	unwatch(cookie, ioc, SimpleOpComp::create(get_executor(),
						  std::move(handler)));
      }, token);
  }

  // This is one of those places where having to force everything into
  // a .cc file is really infuriating. If we had modules, that would
  // let us separate out the implementation details without
  // sacrificing all the benefits of templates.
  using VoidOpSig = void();
  using VoidOpComp = ceph::async::Completion<VoidOpSig>;
  template<typename CompletionToken>
  auto flush_watch(CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, VoidOpSig>(
      [this](auto&& handler) {
	flush_watch(VoidOpComp::create(get_executor(),
				       std::move(handler)));
      }, token);
  }

  using NotifySig = void(boost::system::error_code, ceph::buffer::list);
  using NotifyComp = ceph::async::Completion<NotifySig>;
  template<typename CompletionToken>
  auto notify(const Object& oid, const IOContext& ioc, ceph::buffer::list&& bl,
	      std::optional<std::chrono::milliseconds> timeout,
	      CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, NotifySig>(
      [&, this](auto&& handler) {
	notify(oid, ioc, std::move(bl), timeout,
	       NotifyComp::create(get_executor(),
				  std::move(handler)));
      }, token);
  }

  // The versions with pointers are fine for coroutines, but
  // extraordinarily unappealing for callback-oriented programming.
  using EnumerateSig = void(boost::system::error_code,
			    std::vector<Entry>,
			    Cursor);
  using EnumerateComp = ceph::async::Completion<EnumerateSig>;
  template<typename CompletionToken>
  auto enumerate_objects(const IOContext& ioc, const Cursor& begin,
			 const Cursor& end, const std::uint32_t max,
			 const ceph::buffer::list& filter,
			 CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, EnumerateSig>(
      [&, this](auto&& handler) {
	enumerate_objects(ioc, begin, end, max, filter,
			  EnumerateComp::create(get_executor(),
					     std::move(handler)));
      }, token);
  }

  using CommandSig = void(boost::system::error_code,
			  std::string, ceph::buffer::list);
  using CommandComp = ceph::async::Completion<CommandSig>;
  template<typename CompletionToken>
  auto osd_command(int osd, std::vector<std::string>&& cmd,
		   ceph::buffer::list&& in, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, CommandSig>(
      [&, this](auto&& handler) {
	osd_command(osd, std::move(cmd), std::move(in),
		    CommandComp::create(get_executor(),
					std::move(handler)));
      }, token);
  }
  template<typename CompletionToken>
  auto pg_command(PG pg, std::vector<std::string>&& cmd,
		  ceph::buffer::list&& in, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, CommandSig>(
      [&, this](auto&& handler) {
	pg_command(pg, std::move(cmd), std::move(in),
		   CommandComp::create(get_executor(),
				       std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto mon_command(const std::vector<std::string>& command,
		   const ceph::buffer::list& bl,
		   std::string* outs, ceph::buffer::list* outbl,
		   CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	mon_command(command, bl, outs, outbl,
		    SimpleOpComp::create(get_executor(),
					 std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto enable_application(std::string_view pool, std::string_view app_name,
			  bool force, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	enable_application(pool, app_name, force,
			   SimpleOpComp::create(get_executor(),
						std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto blocklist_add(std::string_view client_address,
                     std::optional<std::chrono::seconds> expire,
                     CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [&, this](auto&& handler) {
	blocklist_add(client_address, expire,
		      SimpleOpComp::create(get_executor(),
					   std::move(handler)));
      }, token);
  }

  template<typename CompletionToken>
  auto wait_for_latest_osd_map(CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, SimpleOpSig>(
      [this](auto&& handler) {
	wait_for_latest_osd_map(SimpleOpComp::create(get_executor(),
						     std::move(handler)));
      }, token);
  }

  uint64_t instance_id() const;

private:

  RADOS();

  friend Builder;

  RADOS(std::unique_ptr<detail::Client> impl);
  static void make_with_cct(CephContext* cct,
			    boost::asio::io_context& ioctx,
		    std::unique_ptr<BuildComp> c);

  void execute(const Object& o, const IOContext& ioc, ReadOp&& op,
	       ceph::buffer::list* bl, std::unique_ptr<Op::Completion> c,
	       uint64_t* objver, const blkin_trace_info* trace_info);

  void execute(const Object& o, const IOContext& ioc, WriteOp&& op,
	       std::unique_ptr<Op::Completion> c, uint64_t* objver,
	       const blkin_trace_info* trace_info);


  void lookup_pool(std::string_view name, std::unique_ptr<LookupPoolComp> c);
  void list_pools(std::unique_ptr<LSPoolsComp> c);
  void create_pool_snap(int64_t pool, std::string_view snapName,
			std::unique_ptr<SimpleOpComp> c);
  void allocate_selfmanaged_snap(int64_t pool, std::unique_ptr<SMSnapComp> c);
  void delete_pool_snap(int64_t pool, std::string_view snapName,
			std::unique_ptr<SimpleOpComp> c);
  void delete_selfmanaged_snap(int64_t pool, std::uint64_t snap,
			       std::unique_ptr<SimpleOpComp> c);
  void create_pool(std::string_view name, std::optional<int> crush_rule,
		   std::unique_ptr<SimpleOpComp> c);
  void delete_pool(std::string_view name,
		   std::unique_ptr<SimpleOpComp> c);
  void delete_pool(int64_t pool,
		   std::unique_ptr<SimpleOpComp> c);
  void stat_pools(const std::vector<std::string>& pools,
		  std::unique_ptr<PoolStatComp> c);
  void stat_fs(std::optional<std::int64_t> pool,
	       std::unique_ptr<StatFSComp> c);

  void watch(const Object& o, const IOContext& ioc,
	     std::optional<std::chrono::seconds> timeout,
	     WatchCB&& cb, std::unique_ptr<WatchComp> c);
  tl::expected<ceph::timespan, boost::system::error_code>
  watch_check(uint64_t cookie);
  void notify_ack(const Object& o,
		  const IOContext& _ioc,
		  uint64_t notify_id,
		  uint64_t cookie,
		  ceph::buffer::list&& bl,
		  std::unique_ptr<SimpleOpComp>);
  void unwatch(uint64_t cookie, const IOContext& ioc,
	       std::unique_ptr<SimpleOpComp>);
  void notify(const Object& oid, const IOContext& ioctx,
	      ceph::buffer::list&& bl,
	      std::optional<std::chrono::milliseconds> timeout,
	      std::unique_ptr<NotifyComp> c);
  void flush_watch(std::unique_ptr<VoidOpComp>);

  void enumerate_objects(const IOContext& ioc, const Cursor& begin,
			 const Cursor& end, const std::uint32_t max,
			 const ceph::buffer::list& filter,
			 std::vector<Entry>* ls,
			 Cursor* cursor,
			 std::unique_ptr<SimpleOpComp> c);
  void enumerate_objects(const IOContext& ioc, const Cursor& begin,
			 const Cursor& end, const std::uint32_t max,
			 const ceph::buffer::list& filter,
			 std::unique_ptr<EnumerateComp> c);
  void osd_command(int osd, std::vector<std::string>&& cmd,
		   ceph::buffer::list&& in, std::unique_ptr<CommandComp> c);
  void pg_command(PG pg, std::vector<std::string>&& cmd,
		  ceph::buffer::list&& in, std::unique_ptr<CommandComp> c);

  void mon_command(std::vector<std::string> command,
		   const ceph::buffer::list& bl,
		   std::string* outs, ceph::buffer::list* outbl,
		   std::unique_ptr<SimpleOpComp> c);

  void enable_application(std::string_view pool, std::string_view app_name,
			  bool force, std::unique_ptr<SimpleOpComp> c);

  void blocklist_add(std::string_view client_address,
                     std::optional<std::chrono::seconds> expire,
                     std::unique_ptr<SimpleOpComp> c);

  void wait_for_latest_osd_map(std::unique_ptr<SimpleOpComp> c);

  // Proxy object to provide access to low-level RADOS messaging clients
  std::unique_ptr<detail::Client> impl;
};
#pragma clang diagnostic pop

enum class errc {
  pool_dne = 1,
  invalid_snapcontext
};

const boost::system::error_category& error_category() noexcept;
}

namespace boost::system {
template<>
struct is_error_code_enum<::neorados::errc> {
  static const bool value = true;
};

template<>
struct is_error_condition_enum<::neorados::errc> {
  static const bool value = false;
};
}

namespace neorados {
//  explicit conversion:
inline boost::system::error_code make_error_code(errc e) noexcept {
  return { static_cast<int>(e), error_category() };
}

// implicit conversion:
inline boost::system::error_condition make_error_condition(errc e) noexcept {
  return { static_cast<int>(e), error_category() };
}
}

namespace std {
template<>
struct hash<neorados::Object> {
  size_t operator ()(const neorados::Object& r) const;
};
template<>
struct hash<neorados::IOContext> {
  size_t operator ()(const neorados::IOContext& r) const;
};
} // namespace std

#if FMT_VERSION >= 90000
namespace fmt {
template <>
struct formatter<neorados::Object> : ostream_formatter {};

template <>
struct formatter<neorados::IOContext> : ostream_formatter {};

template <>
struct formatter<neorados::Op> : ostream_formatter {};
} // namespace fmt
#endif // FMT_VERSION >= 90000
#endif // NEORADOS_RADOS_HPP
