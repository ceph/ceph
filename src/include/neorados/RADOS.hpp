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

#include <concepts>
#include <cstddef>
#include <memory>
#include <optional>
#include <tuple>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/uuid/uuid.hpp>

#include <boost/system/error_code.hpp>

// Will be in C++20!

#include "include/expected.hpp"

// Had better be in C++20. Why is this not in Boost?

#include "include/function2.hpp"

// Things broken out so we can decode them in Objecter.

#include "include/neorados/RADOS_Decodable.hpp"

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

inline constexpr std::uint64_t snap_dir = -1;
inline constexpr std::uint64_t snap_head = -2;

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
  IOContext(std::int64_t pool, std::string ns, std::string key = {});
  ~IOContext();

  IOContext(const IOContext& rhs);
  IOContext& operator =(const IOContext& rhs);

  IOContext(IOContext&& rhs);
  IOContext& operator =(IOContext&& rhs);

  std::int64_t get_pool() const;
  void set_pool(std::int64_t _pool) &;
  IOContext&& set_pool(std::int64_t _pool) &&;

  std::string_view get_ns() const;
  void set_ns(std::string ns) &;
  IOContext&& set_ns(std::string ns) &&;

  std::string_view get_key() const;
  void set_key(std::string key) &;
  IOContext&& set_key(std::string key) &&;

  std::int64_t get_hash() const;
  void set_hash(std::int64_t hash) &;
  IOContext&& set_hash(std::int64_t hash) &&;

  std::uint64_t get_read_snap() const;
  void set_read_snap(std::uint64_t snapid) &;
  IOContext&& set_read_snap(std::uint64_t snapid) &&;

  // I can't actually move-construct here since snapid_t is its own
  // separate class type, not an alias.
  std::optional<
    std::pair<std::uint64_t,
	      std::vector<std::uint64_t>>> get_write_snap_context() const;
  void set_write_snap_context(
    std::optional<std::pair<std::uint64_t,
                            std::vector<std::uint64_t>>> snapc) &;
  IOContext&& set_write_snap_context(
    std::optional<std::pair<std::uint64_t,
                            std::vector<std::uint64_t>>> snapc) &&;

  bool get_full_try() const;
  void set_full_try(bool full_try) &;
  IOContext&& set_full_try(bool full_try) &&;

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

inline const std::string all_nspaces("\001");

enum class cmp_op : std::uint8_t {
  eq  = 1,
  ne  = 2,
  gt  = 3,
  gte = 4,
  lt  = 5,
  lte = 6
};

struct cmp_assertion {
  std::string attr;
  cmp_op op;
  ceph::buffer::list bl;
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

namespace hash_alg {
struct xxhash32_t {
  using init_value = std::uint32_t;
  using hash_value = std::uint32_t;
};
struct xxhash64_t {
  using init_value = std::uint64_t;
  using hash_value = std::uint64_t;
};
struct crc32c_t {
  using init_value = std::uint32_t;
  using hash_value = std::uint32_t;
};

inline constexpr xxhash32_t xxhash32;
inline constexpr xxhash64_t xxhash64;
inline constexpr crc32c_t crc32c;
};

template<typename T>
concept HashAlg = requires {
  // Just enumerate, what's supported is what's on the OSD.
  (std::is_same_v<hash_alg::xxhash32_t, T> ||
   std::is_same_v<hash_alg::xxhash64_t, T> ||
   std::is_same_v<hash_alg::crc32c_t, T>);
};

class Op;
class ReadOp;
class WriteOp;

template<std::invocable<Op&> F>
class ClsOp {
  F f;
public:
  ClsOp(F&& f) : f(std::move(f)) {}

  ReadOp& operator()(ReadOp& op) {
    std::move(f)(op);
    return op;
  }

  ReadOp&& operator()(ReadOp&& op) {
    std::move(f)(op);
    return std::move(op);
  }

  WriteOp& operator()(WriteOp& op) {
    std::move(f)(op);
    return op;
  }

  WriteOp&& operator()(WriteOp&& op) {
    std::move(f)(op);
    return std::move(op);
  }
};

template<std::invocable<ReadOp&> F>
class ClsReadOp {
  F f;
public:
  ClsReadOp(F&& f) : f(std::move(f)) {}

  ReadOp& operator()(ReadOp& op) {
    std::move(f)(op);
    return op;
  }

  ReadOp&& operator()(ReadOp&& op) {
    std::move(f)(op);
    return std::move(op);
  }
};

template<std::invocable<WriteOp&> F>
class ClsWriteOp {
  F f;
public:
  ClsWriteOp(F&& f) : f(std::move(f)) {}

  WriteOp& operator()(WriteOp& op) {
    std::move(f)(op);
    return op;
  }

  WriteOp&& operator()(WriteOp&& op) {
    std::move(f)(op);
    return std::move(op);
  }
};

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

  void cmpext(uint64_t off, ceph::buffer::list cmp_bl,
	      uint64_t* unmatch = nullptr);
  void cmpxattr(std::string_view name, cmp_op op,
		const ceph::buffer::list& val);
  void cmpxattr(std::string_view name, cmp_op op, std::uint64_t val);
  void assert_version(uint64_t ver);
  void assert_exists();
  void cmp_omap(const std::vector<cmp_assertion>& assertions);

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
  using Completion = boost::asio::any_completion_handler<Signature>;

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

  ReadOp& read(size_t off, uint64_t len, ceph::buffer::list* out,
	       boost::system::error_code* ec = nullptr) &;
  ReadOp&& read(size_t off, uint64_t len, ceph::buffer::list* out,
		boost::system::error_code* ec = nullptr) && {
    return std::move(read(off, len, out, ec));
  }
  ReadOp& get_xattr(std::string_view name, ceph::buffer::list* out,
		    boost::system::error_code* ec = nullptr) &;
  ReadOp&& get_xattr(std::string_view name, ceph::buffer::list* out,
		     boost::system::error_code* ec = nullptr) && {
    return std::move(get_xattr(name, out, ec));
  }
  ReadOp& get_omap_header(ceph::buffer::list* bl,
			  boost::system::error_code* ec = nullptr) &;
  ReadOp&& get_omap_header(ceph::buffer::list* bl,
			   boost::system::error_code* ec = nullptr) && {
    return std::move(get_omap_header(bl, ec));
  }
  ReadOp& sparse_read(uint64_t off, uint64_t len,
		      ceph::buffer::list* out,
		      std::vector<std::pair<std::uint64_t,
		                            std::uint64_t>>* extents,
		      boost::system::error_code* ec = nullptr) &;
  ReadOp&& sparse_read(uint64_t off, uint64_t len,
		       ceph::buffer::list* out,
		       std::vector<std::pair<std::uint64_t,
		                             std::uint64_t>>* extents,
		       boost::system::error_code* ec = nullptr) && {
    return std::move(sparse_read(off, len, out, extents, ec));
  }

  ReadOp& stat(std::uint64_t* size, ceph::real_time* mtime,
	       boost::system::error_code* ec = nullptr) &;
  ReadOp&& stat(std::uint64_t* size, ceph::real_time* mtime,
		boost::system::error_code* ec = nullptr) && {
    return std::move(stat(size, mtime, ec));
  }

  ReadOp& get_omap_keys(std::optional<std::string_view> start_after,
			std::uint64_t max_return,
			boost::container::flat_set<std::string>* keys,
			bool* truncated,
			boost::system::error_code* ec = nullptr) &;
  ReadOp&& get_omap_keys(std::optional<std::string_view> start_after,
			 std::uint64_t max_return,
			 boost::container::flat_set<std::string>* keys,
			 bool* truncated,
			 boost::system::error_code* ec = nullptr) && {
    return std::move(get_omap_keys(start_after, max_return, keys, truncated, ec));
  }


  ReadOp& get_xattrs(boost::container::flat_map<std::string,
		                               ceph::buffer::list>* kv,
		     boost::system::error_code* ec = nullptr) &;
  ReadOp&& get_xattrs(boost::container::flat_map<std::string,
		                                ceph::buffer::list>* kv,
		      boost::system::error_code* ec = nullptr) && {
    return std::move(get_xattrs(kv, ec));
  }

  ReadOp& get_omap_vals(std::optional<std::string_view> start_after,
			std::optional<std::string_view> filter_prefix,
			uint64_t max_return,
			boost::container::flat_map<std::string,
		                                   ceph::buffer::list>* kv,
			bool* truncated,
			boost::system::error_code* ec = nullptr) &;
  ReadOp&& get_omap_vals(std::optional<std::string_view> start_after,
			 std::optional<std::string_view> filter_prefix,
			 uint64_t max_return,
			 boost::container::flat_map<std::string,
		                                    ceph::buffer::list>* kv,
			 bool* truncated,
			 boost::system::error_code* ec = nullptr) && {
    return std::move(get_omap_vals(start_after, filter_prefix, max_return, kv,
				   truncated, ec));
  }

  ReadOp& get_omap_vals_by_keys(
    const boost::container::flat_set<std::string>& keys,
    boost::container::flat_map<std::string, ceph::buffer::list>* kv,
    boost::system::error_code* ec = nullptr) &;
  ReadOp&& get_omap_vals_by_keys(
    const boost::container::flat_set<std::string>& keys,
    boost::container::flat_map<std::string, ceph::buffer::list>* kv,
    boost::system::error_code* ec = nullptr) && {
    return std::move(get_omap_vals_by_keys(keys, kv, ec));
  }

  ReadOp& list_watchers(std::vector<ObjWatcher>* watchers,
			boost::system::error_code* ec = nullptr) &;
  ReadOp&& list_watchers(std::vector<ObjWatcher>* watchers,
			 boost::system::error_code* ec = nullptr) && {
    return std::move(list_watchers(watchers, ec));
  }

  ReadOp& list_snaps(struct SnapSet* snaps,
		     boost::system::error_code* ec = nullptr) &;
  ReadOp&& list_snaps(struct SnapSet* snaps,
		      boost::system::error_code* ec = nullptr) && {
    return std::move(list_snaps(snaps, ec));
  }

  template<HashAlg T>
  ReadOp& checksum(T, const typename T::init_value& iv,
		   std::uint64_t off, std::uint64_t len,
		   std::uint64_t chunk_size,
		   std::vector<typename T::hash_value>* out,
		   boost::system::error_code* ec = nullptr) &;
  template<HashAlg T>
  ReadOp&& checksum(T t, const typename T::init_value& iv,
		    std::uint64_t off, std::uint64_t len,
		    std::uint64_t chunk_size,
		    std::vector<typename T::hash_value>* out,
		    boost::system::error_code* ec = nullptr) && {
    return std::move(checksum(t, iv, off, len, chunk_size, out, ec));
  }

  // Chaining versions of functions from Op
  ReadOp& set_excl() & {
    Op::set_excl();
    return *this;
  }
  ReadOp&& set_excl() && {
    Op::set_excl();
    return std::move(*this);
  }

  ReadOp& set_failok() & {
    Op::set_failok();
    return *this;
  }
  ReadOp&& set_failok() && {
    Op::set_failok();
    return std::move(*this);
  }

  ReadOp& set_fadvise_random() & {
    Op::set_fadvise_random();
    return *this;
  }
  ReadOp&& set_fadvise_random() && {
    Op::set_fadvise_random();
    return std::move(*this);
  }

  ReadOp& set_fadvise_sequential() & {
    Op::set_fadvise_sequential();
    return *this;
  }
  ReadOp&& set_fadvise_sequential() && {
    Op::set_fadvise_sequential();
    return std::move(*this);
  }

  ReadOp& set_fadvise_willneed() & {
    Op::set_fadvise_willneed();
    return *this;
  }
  ReadOp&& set_fadvise_willneed() && {
    Op::set_fadvise_willneed();
    return std::move(*this);
  }

  ReadOp& set_fadvise_dontneed() & {
    Op::set_fadvise_dontneed();
    return *this;
  }
  ReadOp&& set_fadvise_dontneed() && {
    Op::set_fadvise_dontneed();
    return std::move(*this);
  }

  ReadOp& set_fadvise_nocache() & {
    Op::set_fadvise_nocache();
    return *this;
  }
  ReadOp&& set_fadvise_nocache() && {
    Op::set_fadvise_nocache();
    return std::move(*this);
  }

  ReadOp& cmpext(uint64_t off, ceph::buffer::list cmp_bl,
		 uint64_t* unmatch = nullptr) & {
    Op::cmpext(off, std::move(cmp_bl), unmatch);
    return *this;
  }
  ReadOp&& cmpext(uint64_t off, ceph::buffer::list cmp_bl,
		  uint64_t* unmatch = nullptr) && {
    Op::cmpext(off, std::move(cmp_bl), unmatch);
    return std::move(*this);
  }

  ReadOp& cmpxattr(std::string_view name, cmp_op op,
		   const ceph::buffer::list& val) & {
    Op::cmpxattr(name, op, val);
    return *this;
  }
  ReadOp&& cmpxattr(std::string_view name, cmp_op op,
		    const ceph::buffer::list& val) && {
    Op::cmpxattr(name, op, val);
    return std::move(*this);
  }

  ReadOp& cmpxattr(std::string_view name, cmp_op op, std::uint64_t val) & {
    Op::cmpxattr(name, op, val);
    return *this;
  }
  ReadOp&& cmpxattr(std::string_view name, cmp_op op, std::uint64_t val) && {
    Op::cmpxattr(name, op, val);
    return std::move(*this);
  }

  ReadOp& assert_version(uint64_t ver) & {
    Op::assert_version(ver);
    return *this;
  }
  ReadOp&& assert_version(uint64_t ver) && {
    Op::assert_version(ver);
    return std::move(*this);
  }

  ReadOp& assert_exists() & {
    Op::assert_exists();
    return *this;
  }
  ReadOp&& assert_exists() && {
    Op::assert_exists();
    return std::move(*this);
  }

  ReadOp& cmp_omap(const std::vector<cmp_assertion>& assertions) & {
    Op::cmp_omap(assertions);
    return *this;
  }
  ReadOp&& cmp_omap(const std::vector<cmp_assertion>& assertions) && {
    Op::cmp_omap(assertions);
    return std::move(*this);
  }

  ReadOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       ceph::buffer::list* out,
	       boost::system::error_code* ec = nullptr) & {
    Op::exec(cls, method, inbl, out, ec);
    return *this;
  }
  ReadOp&& exec(std::string_view cls, std::string_view method,
		const ceph::buffer::list& inbl,
		ceph::buffer::list* out,
		boost::system::error_code* ec = nullptr) && {
    Op::exec(cls, method, inbl, out, ec);
    return std::move(*this);
  }

  ReadOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       fu2::unique_function<void(boost::system::error_code,
	                            const ceph::buffer::list&) &&> f) & {
    Op::exec(cls, method, inbl, std::move(f));
    return *this;
  }
  ReadOp&& exec(std::string_view cls, std::string_view method,
		const ceph::buffer::list& inbl,
	        fu2::unique_function<void(boost::system::error_code,
	                             const ceph::buffer::list&) &&> f) && {
    Op::exec(cls, method, inbl, std::move(f));
    return std::move(*this);
  }

  ReadOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       fu2::unique_function<void(boost::system::error_code, int,
	                                 const ceph::buffer::list&) &&> f) & {
    Op::exec(cls, method, inbl, std::move(f));
    return *this;
  }
  ReadOp&& exec(std::string_view cls, std::string_view method,
	        const ceph::buffer::list& inbl,
	        fu2::unique_function<void(boost::system::error_code, int,
	                                  const ceph::buffer::list&) &&> f) && {
    Op::exec(cls, method, inbl, std::move(f));
    return std::move(*this);
  }

  ReadOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       boost::system::error_code* ec = nullptr) & {
    Op::exec(cls, method, inbl, ec);
    return *this;
  }
  ReadOp&& exec(std::string_view cls, std::string_view method,
		const ceph::buffer::list& inbl,
		boost::system::error_code* ec = nullptr) && {
    Op::exec(cls, method, inbl, ec);
    return std::move(*this);
  }

  template<typename F>
  ReadOp& exec(ClsOp<F>&& clsop) & {
    return clsop(*this);
  }
  template<typename F>
  ReadOp&& exec(ClsOp<F>&& clsop) && {
    return std::move(clsop(*this));
  }
  template<typename F>
  ReadOp& exec(ClsReadOp<F>&& clsop) & {
    return clsop(*this);
  }
  template<typename F>
  ReadOp&& exec(ClsReadOp<F>&& clsop) && {
    return std::move(clsop(*this));
  }

  // Flags that apply to all ops in the operation vector
  ReadOp& balance_reads() & {
    Op::balance_reads();
    return *this;
  }
  ReadOp&& balance_reads() && {
    Op::balance_reads();
    return std::move(*this);
  }
  ReadOp& localize_reads() & {
    Op::localize_reads();
    return *this;
  }
  ReadOp&& localize_reads() && {
    Op::localize_reads();
    return std::move(*this);
  }
  ReadOp& order_reads_writes() & {
    Op::order_reads_writes();
    return *this;
  }
  ReadOp&& order_reads_writes() && {
    Op::order_reads_writes();
    return std::move(*this);
  }
  ReadOp& ignore_cache() & {
    Op::ignore_cache();
    return *this;
  }
  ReadOp&& ignore_cache() && {
    Op::ignore_cache();
    return std::move(*this);
  }
  ReadOp& skiprwlocks() & {
    Op::skiprwlocks();
    return *this;
  }
  ReadOp&& skiprwlocks() && {
    Op::skiprwlocks();
    return std::move(*this);
  }
  ReadOp& ignore_overlay() & {
    Op::ignore_overlay();
    return *this;
  }
  ReadOp&& ignore_overlay() && {
    Op::ignore_overlay();
    return std::move(*this);
  }
  ReadOp& full_try() & {
    Op::full_try();
    return *this;
  }
  ReadOp&& full_try() && {
    Op::full_try();
    return std::move(*this);
  }
  ReadOp& full_force() & {
    Op::full_force();
    return *this;
  }
  ReadOp&& full_force() && {
    Op::full_force();
    return std::move(*this);
  }
  ReadOp& ignore_redirect() & {
    Op::ignore_redirect();
    return *this;
  }
  ReadOp&& ignore_redirect() && {
    Op::ignore_redirect();
    return std::move(*this);
  }
  ReadOp& ordersnap() & {
    Op::ordersnap();
    return *this;
  }
  ReadOp&& ordersnap() && {
    Op::ordersnap();
    return std::move(*this);
  }
  ReadOp& returnvec() & {
    Op::returnvec();
    return *this;
  }
  ReadOp&& returnvec() && {
    Op::returnvec();
    return std::move(*this);
  }
};

class WriteOp final : public Op {
  friend RADOS;
public:

  WriteOp() = default;
  WriteOp(const WriteOp&) = delete;
  WriteOp(WriteOp&&) = default;

  WriteOp& operator =(const WriteOp&) = delete;
  WriteOp& operator =(WriteOp&&) = default;

  WriteOp& set_mtime(ceph::real_time t) &;
  WriteOp&& set_mtime(ceph::real_time t) && {
    return std::move(set_mtime(t));
  }
  WriteOp& create(bool exclusive) &;
  WriteOp&& create(bool exclusive) && {
    return std::move(create(exclusive));
  }
  WriteOp& write(uint64_t off, ceph::buffer::list bl) &;
  WriteOp&& write(uint64_t off, ceph::buffer::list bl) && {
    return std::move(write(off, std::move(bl)));
  }
  WriteOp& write_full(ceph::buffer::list bl) &;
  WriteOp&& write_full(ceph::buffer::list bl) && {
    return std::move(write_full(std::move(bl)));
  }
  WriteOp& writesame(std::uint64_t off, std::uint64_t write_len,
		     ceph::buffer::list bl) &;
  WriteOp&& writesame(std::uint64_t off, std::uint64_t write_len,
		      ceph::buffer::list bl) && {
    return std::move(writesame(off, write_len, std::move(bl)));
  }
  WriteOp& append(ceph::buffer::list bl) &;
  WriteOp&& append(ceph::buffer::list bl) && {
    return std::move(append(std::move(bl)));
  }
  WriteOp& remove() &;
  WriteOp&& remove() && {
    return std::move(remove());
  }
  WriteOp& truncate(uint64_t off) &;
  WriteOp&& truncate(uint64_t off) && {
    return std::move(truncate(off));
  }
  WriteOp& zero(uint64_t off, uint64_t len) &;
  WriteOp&& zero(uint64_t off, uint64_t len) && {
    return std::move(zero(off, len));
  }
  WriteOp& rmxattr(std::string_view name) &;
  WriteOp&& rmxattr(std::string_view name) && {
    return std::move(rmxattr(name));
  }
  WriteOp& setxattr(std::string_view name,
		    ceph::buffer::list bl) &;
  WriteOp&& setxattr(std::string_view name,
		     ceph::buffer::list bl) && {
    return std::move(setxattr(name, std::move(bl)));
  }
  WriteOp& rollback(uint64_t snapid) &;
  WriteOp&& rollback(uint64_t snapid) && {
    return std::move(rollback(snapid));
  }
  WriteOp& set_omap(
    const boost::container::flat_map<std::string, ceph::buffer::list>& map) &;
  WriteOp&& set_omap(
    const boost::container::flat_map<std::string, ceph::buffer::list>& map) && {
    return std::move(set_omap(map));
  }
  WriteOp& set_omap_header(ceph::buffer::list bl) &;
  WriteOp&& set_omap_header(ceph::buffer::list bl) && {
    return std::move(set_omap_header(std::move(bl)));
  }
  WriteOp& clear_omap() &;
  WriteOp&& clear_omap() && {
    return std::move(clear_omap());
  }
  WriteOp& rm_omap_keys(const boost::container::flat_set<std::string>& to_rm) &;
  WriteOp&& rm_omap_keys(const boost::container::flat_set<std::string>& to_rm) && {
    return std::move(rm_omap_keys(to_rm));
  }
  WriteOp& set_alloc_hint(uint64_t expected_object_size,
			  uint64_t expected_write_size,
			  alloc_hint::alloc_hint_t flags) &;
  WriteOp&& set_alloc_hint(uint64_t expected_object_size,
			   uint64_t expected_write_size,
			   alloc_hint::alloc_hint_t flags) && {
    return std::move(set_alloc_hint(expected_object_size,
				    expected_write_size,
				    flags));
  }

  // Chaining versions of functions from Op
  WriteOp& set_excl() & {
    Op::set_excl();
    return *this;
  }
  WriteOp&& set_excl() && {
    Op::set_excl();
    return std::move(*this);
  }

  WriteOp& set_failok() & {
    Op::set_failok();
    return *this;
  }
  WriteOp&& set_failok() && {
    Op::set_failok();
    return std::move(*this);
  }

  WriteOp& set_fadvise_random() & {
    Op::set_fadvise_random();
    return *this;
  }
  WriteOp&& set_fadvise_random() && {
    Op::set_fadvise_random();
    return std::move(*this);
  }

  WriteOp& set_fadvise_sequential() & {
    Op::set_fadvise_sequential();
    return *this;
  }
  WriteOp&& set_fadvise_sequential() && {
    Op::set_fadvise_sequential();
    return std::move(*this);
  }

  WriteOp& set_fadvise_willneed() & {
    Op::set_fadvise_willneed();
    return *this;
  }
  WriteOp&& set_fadvise_willneed() && {
    Op::set_fadvise_willneed();
    return std::move(*this);
  }

  WriteOp& set_fadvise_dontneed() & {
    Op::set_fadvise_dontneed();
    return *this;
  }
  WriteOp&& set_fadvise_dontneed() && {
    Op::set_fadvise_dontneed();
    return std::move(*this);
  }

  WriteOp& set_fadvise_nocache() & {
    Op::set_fadvise_nocache();
    return *this;
  }
  WriteOp&& set_fadvise_nocache() && {
    Op::set_fadvise_nocache();
    return std::move(*this);
  }

  WriteOp& cmpext(uint64_t off, ceph::buffer::list cmp_bl,
		  uint64_t* unmatch = nullptr) & {
    Op::cmpext(off, std::move(cmp_bl), unmatch);
    return *this;
  }
  WriteOp&& cmpext(uint64_t off, ceph::buffer::list cmp_bl,
		   uint64_t* unmatch = nullptr) && {
    Op::cmpext(off, std::move(cmp_bl), unmatch);
    return std::move(*this);
  }

  WriteOp& cmpxattr(std::string_view name, cmp_op op,
		   const ceph::buffer::list& val) & {
    Op::cmpxattr(name, op, val);
    return *this;
  }
  WriteOp&& cmpxattr(std::string_view name, cmp_op op,
		    const ceph::buffer::list& val) && {
    Op::cmpxattr(name, op, val);
    return std::move(*this);
  }

  WriteOp& cmpxattr(std::string_view name, cmp_op op, std::uint64_t val) & {
    Op::cmpxattr(name, op, val);
    return *this;
  }
  WriteOp&& cmpxattr(std::string_view name, cmp_op op, std::uint64_t val) && {
    Op::cmpxattr(name, op, val);
    return std::move(*this);
  }

  WriteOp& assert_version(uint64_t ver) & {
    Op::assert_version(ver);
    return *this;
  }
  WriteOp&& assert_version(uint64_t ver) && {
    Op::assert_version(ver);
    return std::move(*this);
  }

  WriteOp& assert_exists() & {
    Op::assert_exists();
    return *this;
  }
  WriteOp&& assert_exists() && {
    Op::assert_exists();
    return std::move(*this);
  }

  WriteOp& cmp_omap(const std::vector<cmp_assertion>& assertions) & {
    Op::cmp_omap(assertions);
    return *this;
  }
  WriteOp&& cmp_omap(const std::vector<cmp_assertion>& assertions) && {
    Op::cmp_omap(assertions);
    return std::move(*this);
  }

  WriteOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       ceph::buffer::list* out,
	       boost::system::error_code* ec = nullptr) & {
    Op::exec(cls, method, inbl, out, ec);
    return *this;
  }
  WriteOp&& exec(std::string_view cls, std::string_view method,
		const ceph::buffer::list& inbl,
		ceph::buffer::list* out,
		boost::system::error_code* ec = nullptr) && {
    Op::exec(cls, method, inbl, out, ec);
    return std::move(*this);
  }

  WriteOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       fu2::unique_function<void(boost::system::error_code,
	                            const ceph::buffer::list&) &&> f) & {
    Op::exec(cls, method, inbl, std::move(f));
    return *this;
  }
  WriteOp&& exec(std::string_view cls, std::string_view method,
		const ceph::buffer::list& inbl,
	        fu2::unique_function<void(boost::system::error_code,
	                             const ceph::buffer::list&) &&> f) && {
    Op::exec(cls, method, inbl, std::move(f));
    return std::move(*this);
  }

  WriteOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       fu2::unique_function<void(boost::system::error_code, int,
	                                 const ceph::buffer::list&) &&> f) & {
    Op::exec(cls, method, inbl, std::move(f));
    return *this;
  }
  WriteOp&& exec(std::string_view cls, std::string_view method,
	        const ceph::buffer::list& inbl,
	        fu2::unique_function<void(boost::system::error_code, int,
	                                  const ceph::buffer::list&) &&> f) && {
    Op::exec(cls, method, inbl, std::move(f));
    return std::move(*this);
  }

  WriteOp& exec(std::string_view cls, std::string_view method,
	       const ceph::buffer::list& inbl,
	       boost::system::error_code* ec = nullptr) & {
    Op::exec(cls, method, inbl, ec);
    return *this;
  }
  WriteOp&& exec(std::string_view cls, std::string_view method,
		const ceph::buffer::list& inbl,
		boost::system::error_code* ec = nullptr) && {
    Op::exec(cls, method, inbl, ec);
    return std::move(*this);
  }

  template<typename F>
  WriteOp& exec(ClsOp<F>&& clsop) & {
    return clsop(*this);
  }
  template<typename F>
  WriteOp&& exec(ClsOp<F>&& clsop) && {
    return std::move(clsop(*this));
  }
  template<typename F>
  WriteOp& exec(ClsWriteOp<F>&& clsop) & {
    return clsop(*this);
  }
  template<typename F>
  WriteOp&& exec(ClsWriteOp<F>&& clsop) && {
    return std::move(clsop(*this));
  }


  // Flags that apply to all ops in the operation vector
  WriteOp& balance_reads() & {
    Op::balance_reads();
    return *this;
  }
  WriteOp&& balance_reads() && {
    Op::balance_reads();
    return std::move(*this);
  }
  WriteOp& localize_reads() & {
    Op::localize_reads();
    return *this;
  }
  WriteOp&& localize_reads() && {
    Op::localize_reads();
    return std::move(*this);
  }
  WriteOp& order_reads_writes() & {
    Op::order_reads_writes();
    return *this;
  }
  WriteOp&& order_reads_writes() && {
    Op::order_reads_writes();
    return std::move(*this);
  }
  WriteOp& ignore_cache() & {
    Op::ignore_cache();
    return *this;
  }
  WriteOp&& ignore_cache() && {
    Op::ignore_cache();
    return std::move(*this);
  }
  WriteOp& skiprwlocks() & {
    Op::skiprwlocks();
    return *this;
  }
  WriteOp&& skiprwlocks() && {
    Op::skiprwlocks();
    return std::move(*this);
  }
  WriteOp& ignore_overlay() & {
    Op::ignore_overlay();
    return *this;
  }
  WriteOp&& ignore_overlay() && {
    Op::ignore_overlay();
    return std::move(*this);
  }
  WriteOp& full_try() & {
    Op::full_try();
    return *this;
  }
  WriteOp&& full_try() && {
    Op::full_try();
    return std::move(*this);
  }
  WriteOp& full_force() & {
    Op::full_force();
    return *this;
  }
  WriteOp&& full_force() && {
    Op::full_force();
    return std::move(*this);
  }
  WriteOp& ignore_redirect() & {
    Op::ignore_redirect();
    return *this;
  }
  WriteOp&& ignore_redirect() && {
    Op::ignore_redirect();
    return std::move(*this);
  }
  WriteOp& ordersnap() & {
    Op::ordersnap();
    return *this;
  }
  WriteOp&& ordersnap() && {
    Op::ordersnap();
    return std::move(*this);
  }
  WriteOp& returnvec() & {
    Op::returnvec();
    return *this;
  }
  WriteOp&& returnvec() && {
    Op::returnvec();
    return std::move(*this);
  }
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
  using BuildComp = boost::asio::any_completion_handler<BuildSig>;
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

    template<boost::asio::completion_token_for<BuildSig> CompletionToken>
    auto build(boost::asio::io_context& ioctx, CompletionToken&& token) {
      auto consigned = boost::asio::consign(
	std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	  boost::asio::get_associated_executor(token, ioctx.get_executor())));
      return boost::asio::async_initiate<decltype(consigned), BuildSig>(
	[&ioctx, this](auto handler) {
	  build_(ioctx, std::move(handler));
	}, consigned);
    }

  private:
    void build_(boost::asio::io_context& ioctx,
		BuildComp c);
  };


  template<boost::asio::completion_token_for<BuildSig> CompletionToken>
  static auto make_with_cct(CephContext* cct,
			    boost::asio::io_context& ioctx,
			    CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, ioctx.get_executor())));
    return boost::asio::async_initiate<decltype(consigned), BuildSig>(
      [cct, &ioctx](auto&& handler) {
	make_with_cct_(cct, ioctx, std::move(handler));
      }, consigned);
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

  template<boost::asio::completion_token_for<Op::Signature> CompletionToken>
  auto execute(Object o, IOContext ioc, ReadOp op,
	       ceph::buffer::list* bl,
	       CompletionToken&& token, uint64_t* objver = nullptr,
	       const blkin_trace_info* trace_info = nullptr) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), Op::Signature>(
      [o = std::move(o), ioc = std::move(ioc), op = std::move(op),
       bl, objver, trace_info, this](auto&& handler) mutable {
	execute_(std::move(o), std::move(ioc), std::move(op), bl,
		 std::move(handler), objver, trace_info);
      }, consigned);
  }

  template<boost::asio::completion_token_for<Op::Signature> CompletionToken>
  auto execute(Object o, IOContext ioc, WriteOp op,
	       CompletionToken&& token, uint64_t* objver = nullptr,
	       const blkin_trace_info* trace_info = nullptr) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), Op::Signature>(
      [o = std::move(o), ioc = std::move(ioc), op = std::move(op),
       objver, trace_info, this](auto&& handler) mutable {
	execute_(std::move(o), std::move(ioc), std::move(op),
		 std::move(handler), objver, trace_info);
      }, consigned);
  }

  boost::uuids::uuid get_fsid() const noexcept;

  using LookupPoolSig = void(boost::system::error_code,
			     std::int64_t);
  using LookupPoolComp = boost::asio::any_completion_handler<LookupPoolSig>;
  template<boost::asio::completion_token_for<LookupPoolSig> CompletionToken>
  auto lookup_pool(std::string name, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), LookupPoolSig>(
      [name = std::move(name), this](auto&& handler) mutable {
	lookup_pool_(std::move(name), std::move(handler));
      }, consigned);
  }

  std::optional<uint64_t> get_pool_alignment(int64_t pool_id);

  using LSPoolsSig = void(std::vector<std::pair<std::int64_t, std::string>>);
  using LSPoolsComp = boost::asio::any_completion_handler<LSPoolsSig>;
  template<boost::asio::completion_token_for<LSPoolsSig> CompletionToken>
  auto list_pools(CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), LSPoolsSig>(
      [this](auto&& handler) {
	list_pools_(std::move(handler));
      }, consigned);
  }

  using SimpleOpSig = void(boost::system::error_code);
  using SimpleOpComp = boost::asio::any_completion_handler<SimpleOpSig>;
  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto create_pool_snap(int64_t pool, std::string snap_name,
			CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [snap_name = std::move(snap_name), pool, this](auto&& handler) mutable {
	create_pool_snap_(pool, std::move(snap_name),
			  std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto create_pool_snap(const IOContext& pool, std::string snap_name,
			CompletionToken&& token) {
    return create_pool_snap(pool.get_pool(), std::move(snap_name),
			    std::forward<CompletionToken>(token));
  }

  using SMSnapSig = void(boost::system::error_code, std::uint64_t);
  using SMSnapComp = boost::asio::any_completion_handler<SMSnapSig>;
  template<boost::asio::completion_token_for<SMSnapSig> CompletionToken>
  auto allocate_selfmanaged_snap(int64_t pool, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SMSnapSig>(
      [pool, this](auto&& handler) mutable {
	allocate_selfmanaged_snap_(pool, std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto delete_pool_snap(int64_t pool, std::string snap_name,
			CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [snap_name = std::move(snap_name), pool, this](auto&& handler) mutable {
	delete_pool_snap_(pool, std::move(snap_name),
			  std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto delete_selfmanaged_snap(int64_t pool, std::uint64_t snap,
			       CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [pool, snap, this](auto&& handler) mutable {
	delete_selfmanaged_snap_(pool, snap, std::move(handler));
      }, consigned);
  }

  bool get_self_managed_snaps_mode(std::int64_t pool) const;
  bool get_self_managed_snaps_mode(std::string_view pool) const;
  bool get_self_managed_snaps_mode(const IOContext& pool) const {
    return get_self_managed_snaps_mode(pool.get_pool());
  }

  std::vector<std::uint64_t> list_snaps(std::int64_t pool) const;
  std::vector<std::uint64_t> list_snaps(std::string_view pool) const;
  std::vector<std::uint64_t> list_snaps(const IOContext& pool) const {
    return list_snaps(pool.get_pool());
  }

  std::uint64_t lookup_snap(std::int64_t pool, std::string_view snap) const;
  std::uint64_t lookup_snap(std::string_view pool, std::string_view snap) const;
  std::uint64_t lookup_snap(const IOContext& pool, std::string_view snap) const {
    return lookup_snap(pool.get_pool(), snap);
  }

  std::string get_snap_name(std::int64_t pool, std::uint64_t snap) const;
  std::string get_snap_name(std::string_view pool, std::uint64_t snap) const;
  std::string get_snap_name(const IOContext& pool, std::uint64_t snap) const {
    return get_snap_name(pool.get_pool(), snap);
  }

  ceph::real_time get_snap_timestamp(std::int64_t pool,
				     std::uint64_t snap) const;
  ceph::real_time get_snap_timestamp(std::string_view pool,
				     std::uint64_t snap) const;
  ceph::real_time get_snap_timestamp(const IOContext& pool,
				     std::uint64_t snap) const {
    return get_snap_timestamp(pool.get_pool(), snap);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto create_pool(std::string name, std::optional<int> crush_rule,
		   CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [name = std::move(name), crush_rule, this](auto&& handler) mutable {
	create_pool_(std::move(name), crush_rule,
		     std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto delete_pool(std::string name, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [name = std::move(name), this](auto&& handler) mutable {
	delete_pool_(std::move(name), std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto delete_pool(int64_t pool, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [pool, this](auto&& handler) mutable {
	delete_pool_(pool, std::move(handler));
      }, consigned);
  }

  using PoolStatSig = void(boost::system::error_code,
			   boost::container::flat_map<std::string,
						      PoolStats>, bool);
  using PoolStatComp = boost::asio::any_completion_handler<PoolStatSig>;
  template<boost::asio::completion_token_for<PoolStatSig> CompletionToken>
  auto stat_pools(std::vector<std::string> pools, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), PoolStatSig>(
      [pools = std::move(pools), this](auto&& handler) mutable {
	stat_pools_(std::move(pools), std::move(handler));
      }, consigned);
  }

  using StatFSSig = void(boost::system::error_code,
			 FSStats);
  using StatFSComp = boost::asio::any_completion_handler<StatFSSig>;
  template<boost::asio::completion_token_for<StatFSSig> CompletionToken>
  auto statfs(std::optional<int64_t> pool, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), StatFSSig>(
      [pool, this](auto&& handler) mutable {
	statfs_(pool, std::move(handler));
      }, consigned);
  }

  using WatchCB = fu2::unique_function<void(boost::system::error_code,
					    uint64_t notify_id,
					    uint64_t cookie,
					    uint64_t notifier_id,
					    ceph::buffer::list&& bl)>;

  using WatchSig = void(boost::system::error_code ec,
			uint64_t cookie);
  using WatchComp = boost::asio::any_completion_handler<WatchSig>;
  template<boost::asio::completion_token_for<WatchSig> CompletionToken>
  auto watch(Object o, IOContext ioc,
	     std::optional<std::chrono::seconds> timeout,
	     WatchCB cb, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), WatchSig>(
      [o = std::move(o), ioc = std::move(ioc), timeout,
       cb = std::move(cb), this](auto&& handler) mutable {
	watch_(std::move(o), std::move(ioc), timeout, std::move(cb),
	       std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto notify_ack(Object o, IOContext ioc,
		  uint64_t notify_id, uint64_t cookie,
		  ceph::buffer::list bl,
		  CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [o = std::move(o), ioc = std::move(ioc), notify_id,
       cookie, bl = std::move(bl), this](auto&& handler) mutable {
	notify_ack_(std::move(o), std::move(ioc), std::move(notify_id),
		    std::move(cookie), std::move(bl), std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto unwatch(std::uint64_t cookie, IOContext ioc,
	       CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [cookie, ioc = std::move(ioc), this](auto&& handler) mutable {
	unwatch_(cookie, std::move(ioc), std::move(handler));
      }, consigned);
  }

  // This is one of those places where having to force everything into
  // a .cc file is really infuriating. If we had modules, that would
  // let us separate out the implementation details without
  // sacrificing all the benefits of templates.
  using VoidOpSig = void();
  using VoidOpComp = boost::asio::any_completion_handler<VoidOpSig>;
  template<boost::asio::completion_token_for<VoidOpSig> CompletionToken>
  auto flush_watch(CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), VoidOpSig>(
      [this](auto&& handler) {
	flush_watch_(std::move(handler));
      }, consigned);
  }

  tl::expected<ceph::timespan, boost::system::error_code>
  check_watch(uint64_t cookie);

  using NotifySig = void(boost::system::error_code, ceph::buffer::list);
  using NotifyComp = boost::asio::any_completion_handler<NotifySig>;
  template<boost::asio::completion_token_for<NotifySig> CompletionToken>
  auto notify(Object o, IOContext ioc, ceph::buffer::list bl,
	      std::optional<std::chrono::seconds> timeout,
	      CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), NotifySig>(
      [o = std::move(o), ioc = std::move(ioc), bl = std::move(bl), timeout,
       this](auto&& handler) mutable {
	notify_(std::move(o), std::move(ioc), std::move(bl), timeout,
		std::move(handler));
      }, consigned);
  }

  // The versions with pointers are fine for coroutines, but
  // extraordinarily unappealing for callback-oriented programming.
  using EnumerateSig = void(boost::system::error_code,
			    std::vector<Entry>,
			    Cursor);
  using EnumerateComp = boost::asio::any_completion_handler<EnumerateSig>;
  template<boost::asio::completion_token_for<EnumerateSig> CompletionToken>
  auto enumerate_objects(IOContext ioc, Cursor begin,
			 Cursor end, std::uint32_t max,
			 ceph::buffer::list filter,
			 CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), EnumerateSig>(
      [ioc = std::move(ioc), begin = std::move(begin), end = std::move(end),
       max, filter = std::move(filter), this](auto&& handler) mutable {
	enumerate_objects_(std::move(ioc), std::move(begin), std::move(end),
			   std::move(max), std::move(filter),
			   std::move(handler));
      }, consigned);
  }

  using CommandSig = void(boost::system::error_code,
			  std::string, ceph::buffer::list);
  using CommandComp = boost::asio::any_completion_handler<CommandSig>;
  template<boost::asio::completion_token_for<CommandSig> CompletionToken>
  auto osd_command(int osd, std::vector<std::string> cmd,
		   ceph::buffer::list in, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), CommandSig>(
      [osd, cmd = std::move(cmd), in = std::move(in),
       this](auto&& handler) mutable {
	osd_command_(osd, std::move(cmd), std::move(in),
		     std::move(handler));
      }, consigned);
  }
  template<boost::asio::completion_token_for<CommandSig> CompletionToken>
  auto pg_command(PG pg, std::vector<std::string> cmd,
		  ceph::buffer::list in, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), CommandSig>(
      [pg = std::move(pg), cmd = std::move(cmd), in = std::move(in),
       this](auto&& handler) mutable {
	pg_command_(std::move(pg), std::move(cmd), std::move(in),
		    std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto mon_command(std::vector<std::string> command,
		   ceph::buffer::list bl,
		   std::string* outs, ceph::buffer::list* outbl,
		   CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [command = std::move(command), bl = std::move(bl), outs, outbl,
       this](auto&& handler) mutable {
	mon_command_(std::move(command), std::move(bl), outs, outbl,
		     std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto enable_application(std::string pool, std::string app_name,
			  bool force, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [pool = std::move(pool), app_name = std::move(app_name),
       force, this](auto&& handler) mutable {
	enable_application_(std::move(pool), std::move(app_name), force,
			    std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto blocklist_add(std::string client_address,
                     std::optional<std::chrono::seconds> expire,
                     CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [client_address = std::move(client_address), expire,
       this](auto&& handler) mutable {
	blocklist_add_(std::move(client_address), expire,
		       std::move(handler));
      }, consigned);
  }

  template<boost::asio::completion_token_for<SimpleOpSig> CompletionToken>
  auto wait_for_latest_osd_map(CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	boost::asio::get_associated_executor(token, get_executor())));
    return boost::asio::async_initiate<decltype(consigned), SimpleOpSig>(
      [this](auto&& handler) {
	wait_for_latest_osd_map_(std::move(handler));
      }, consigned);
  }

  uint64_t instance_id() const;

private:

  RADOS();

  friend Builder;

  RADOS(std::unique_ptr<detail::Client> impl);
  static void make_with_cct_(CephContext* cct,
			     boost::asio::io_context& ioctx,
			     BuildComp c);

  void execute_(Object o, IOContext ioc, ReadOp op,
		ceph::buffer::list* bl, Op::Completion c,
		uint64_t* objver, const blkin_trace_info* trace_info);

  void execute_(Object o, IOContext ioc, WriteOp op,
		Op::Completion c, uint64_t* objver,
		const blkin_trace_info* trace_info);

  void lookup_pool_(std::string name, LookupPoolComp c);
  void list_pools_(LSPoolsComp c);
  void create_pool_snap_(int64_t pool, std::string snap_name,
			 SimpleOpComp c);
  void allocate_selfmanaged_snap_(int64_t pool, SMSnapComp c);
  void delete_pool_snap_(int64_t pool, std::string snap_name,
			 SimpleOpComp c);
  void delete_selfmanaged_snap_(int64_t pool, std::uint64_t snap,
				SimpleOpComp c);
  void create_pool_(std::string name, std::optional<int> crush_rule,
		    SimpleOpComp c);
  void delete_pool_(std::string name,
		    SimpleOpComp c);
  void delete_pool_(int64_t pool,
		    SimpleOpComp c);
  void stat_pools_(std::vector<std::string> pools,
		   PoolStatComp c);
  void stat_fs_(std::optional<std::int64_t> pool,
		StatFSComp c);
  void watch_(Object o, IOContext ioc,
	      std::optional<std::chrono::seconds> timeout,
	      WatchCB cb, WatchComp c);
  void notify_ack_(Object o, IOContext _ioc,
		   uint64_t notify_id,
		   uint64_t cookie,
		   ceph::buffer::list bl,
		   SimpleOpComp);
  void unwatch_(uint64_t cookie, IOContext ioc,
		SimpleOpComp);
  void notify_(Object oid, IOContext ioctx,
	       ceph::buffer::list bl,
	       std::optional<std::chrono::seconds> timeout,
	       NotifyComp c);
  void flush_watch_(VoidOpComp);

  void enumerate_objects_(IOContext ioc, Cursor begin,
			  Cursor end, std::uint32_t max,
			  ceph::buffer::list filter,
			  std::vector<Entry>* ls,
			  Cursor* cursor,
			  SimpleOpComp c);
  void enumerate_objects_(IOContext ioc, Cursor begin,
			  Cursor end, std::uint32_t max,
			  ceph::buffer::list filter,
			  EnumerateComp c);
  void osd_command_(int osd, std::vector<std::string> cmd,
		    ceph::buffer::list in, CommandComp c);
  void pg_command_(PG pg, std::vector<std::string> cmd,
		   ceph::buffer::list in, CommandComp c);

  void mon_command_(std::vector<std::string> command,
		    ceph::buffer::list bl,
		    std::string* outs, ceph::buffer::list* outbl,
		    SimpleOpComp c);

  void enable_application_(std::string pool, std::string app_name,
			   bool force, SimpleOpComp c);

  void blocklist_add_(std::string client_address,
		      std::optional<std::chrono::seconds> expire,
		      SimpleOpComp c);

  void wait_for_latest_osd_map_(SimpleOpComp c);

  // Proxy object to provide access to low-level RADOS messaging clients
  std::unique_ptr<detail::Client> impl;
};
#pragma clang diagnostic pop

enum class errc {
  pool_dne = 1,
  snap_dne,
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
template<> struct fmt::formatter<neorados::Object> : fmt::ostream_formatter {};
template<> struct fmt::formatter<neorados::IOContext>
  : fmt::ostream_formatter {};
#endif // FMT_VERSION

#endif // NEORADOS_RADOS_HPP
