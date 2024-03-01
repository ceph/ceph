// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#define BOOST_BIND_NO_PLACEHOLDERS

#include <optional>
#include <string_view>

#include <boost/intrusive_ptr.hpp>

#include <fmt/format.h>

#include "include/ceph_fs.h"

#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/hobject.h"
#include "common/EventTrace.h"

#include "global/global_init.h"

#include "osd/osd_types.h"
#include "osdc/error_code.h"

#include "neorados/RADOSImpl.h"
#include "include/neorados/RADOS.hpp"

using namespace std::literals;

namespace asio = boost::asio;
namespace bc = boost::container;
namespace bs = boost::system;
namespace cb = ceph::buffer;

namespace neorados {
// Object

Object::Object() {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t();
}

Object::Object(const char* s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(s);
}

Object::Object(std::string_view s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(s);
}

Object::Object(std::string&& s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(std::move(s));
}

Object::Object(const std::string& s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(s);
}

Object::~Object() {
  reinterpret_cast<object_t*>(&impl)->~object_t();
}

Object::Object(const Object& o) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(*reinterpret_cast<const object_t*>(&o.impl));
}
Object& Object::operator =(const Object& o) {
  *reinterpret_cast<object_t*>(&impl) =
    *reinterpret_cast<const object_t*>(&o.impl);
  return *this;
}
Object::Object(Object&& o) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(std::move(*reinterpret_cast<object_t*>(&o.impl)));
}
Object& Object::operator =(Object&& o) {
  *reinterpret_cast<object_t*>(&impl) =
    std::move(*reinterpret_cast<object_t*>(&o.impl));
  return *this;
}

Object::operator std::string_view() const {
  return std::string_view(reinterpret_cast<const object_t*>(&impl)->name);
}

bool operator <(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) <
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator <=(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) <=
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator >=(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) >=
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator >(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) >
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}

bool operator ==(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) ==
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator !=(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) !=
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}

std::ostream& operator <<(std::ostream& m, const Object& o) {
  return (m << *reinterpret_cast<const object_t*>(&o.impl));
}

// IOContext

struct IOContextImpl {
  object_locator_t oloc;
  snapid_t snap_seq = CEPH_NOSNAP;
  SnapContext snapc;
  int extra_op_flags = 0;
};

IOContext::IOContext() {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl();
}

IOContext::IOContext(std::int64_t pool) : IOContext() {
  set_pool(pool);
}

IOContext::IOContext(std::int64_t pool, std::string ns, std::string key)
  : IOContext() {
  set_pool(pool);
  set_ns(std::move(ns));
  set_key(std::move(key));
}

IOContext::~IOContext() {
  reinterpret_cast<IOContextImpl*>(&impl)->~IOContextImpl();
}

IOContext::IOContext(const IOContext& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(*reinterpret_cast<const IOContextImpl*>(&rhs.impl));
}

IOContext& IOContext::operator =(const IOContext& rhs) {
  *reinterpret_cast<IOContextImpl*>(&impl) =
    *reinterpret_cast<const IOContextImpl*>(&rhs.impl);
  return *this;
}

IOContext::IOContext(IOContext&& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(
    std::move(*reinterpret_cast<IOContextImpl*>(&rhs.impl)));
}

IOContext& IOContext::operator =(IOContext&& rhs) {
  *reinterpret_cast<IOContextImpl*>(&impl) =
    std::move(*reinterpret_cast<IOContextImpl*>(&rhs.impl));
  return *this;
}

std::int64_t IOContext::get_pool() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.pool;
}

void IOContext::set_pool(std::int64_t pool) & {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.pool = pool;
}

IOContext&& IOContext::set_pool(std::int64_t pool) && {
  set_pool(pool);
  return std::move(*this);
}

std::string_view IOContext::get_ns() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.nspace;
}

void IOContext::set_ns(std::string ns) & {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.nspace = std::move(ns);
}

IOContext&& IOContext::set_ns(std::string ns) && {
  set_ns(std::move(ns));
  return std::move(*this);
}

std::string_view IOContext::get_key() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.key;
}

void IOContext::set_key(std::string key) & {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key = std::move(key);
}

IOContext&& IOContext::set_key(std::string key) && {
  set_key(std::move(key));
  return std::move(*this);
}

std::int64_t IOContext::get_hash() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.hash;
}

void IOContext::set_hash(std::int64_t hash) & {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = hash;
  oloc.key.clear();
}

IOContext&& IOContext::set_hash(std::int64_t hash) && {
  set_hash(hash);
  return std::move(*this);
}

std::uint64_t IOContext::get_read_snap() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->snap_seq;
}

void IOContext::set_read_snap(std::uint64_t snapid) & {
  reinterpret_cast<IOContextImpl*>(&impl)->snap_seq = snapid;
}

IOContext&& IOContext::set_read_snap(std::uint64_t snapid) && {
  set_read_snap(snapid);
  return std::move(*this);
}

std::optional<std::pair<std::uint64_t, std::vector<std::uint64_t>>>
IOContext::get_write_snap_context() const {
  auto& snapc = reinterpret_cast<const IOContextImpl*>(&impl)->snapc;
  if (snapc.empty()) {
    return std::nullopt;
  } else {
    std::vector<uint64_t> v(snapc.snaps.begin(), snapc.snaps.end());
    return std::make_optional(std::make_pair(uint64_t(snapc.seq), v));
  }
}

void IOContext::set_write_snap_context(
  std::optional<std::pair<std::uint64_t,
                          std::vector<std::uint64_t>>> _snapc) & {
  auto& snapc = reinterpret_cast<IOContextImpl*>(&impl)->snapc;
  if (!_snapc) {
    snapc.clear();
  } else {
    SnapContext n(_snapc->first, { _snapc->second.begin(), _snapc->second.end()});
    if (!n.is_valid()) {
      throw bs::system_error(EINVAL,
			     bs::system_category(),
			     "Invalid snap context.");

    } else {
      snapc = n;
    }
  }
}

IOContext&& IOContext::set_write_snap_context(
  std::optional<std::pair<std::uint64_t,
                          std::vector<std::uint64_t>>> snapc) && {
  set_write_snap_context(std::move(snapc));
  return std::move(*this);
}

bool IOContext::get_full_try() const {
  const auto ioc = reinterpret_cast<const IOContextImpl*>(&impl);
  return (ioc->extra_op_flags & CEPH_OSD_FLAG_FULL_TRY) != 0;
}

void IOContext::set_full_try(bool full_try) & {
  auto ioc = reinterpret_cast<IOContextImpl*>(&impl);
  if (full_try) {
    ioc->extra_op_flags |= CEPH_OSD_FLAG_FULL_TRY;
  } else {
    ioc->extra_op_flags &= ~CEPH_OSD_FLAG_FULL_TRY;
  }
}

IOContext&& IOContext::set_full_try(bool full_try) && {
  set_full_try(full_try);
  return std::move(*this);
}

bool operator <(const IOContext& lhs, const IOContext& rhs) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  const auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);

  return (std::tie(l->oloc.pool, l->oloc.nspace, l->oloc.key) <
	  std::tie(r->oloc.pool, r->oloc.nspace, r->oloc.key));
}

bool operator <=(const IOContext& lhs, const IOContext& rhs) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  const auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);

  return (std::tie(l->oloc.pool, l->oloc.nspace, l->oloc.key) <=
	  std::tie(r->oloc.pool, r->oloc.nspace, r->oloc.key));
}

bool operator >=(const IOContext& lhs, const IOContext& rhs) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  const auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);

  return (std::tie(l->oloc.pool, l->oloc.nspace, l->oloc.key) >=
	  std::tie(r->oloc.pool, r->oloc.nspace, r->oloc.key));
}

bool operator >(const IOContext& lhs, const IOContext& rhs) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  const auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);

  return (std::tie(l->oloc.pool, l->oloc.nspace, l->oloc.key) >
	  std::tie(r->oloc.pool, r->oloc.nspace, r->oloc.key));
}

bool operator ==(const IOContext& lhs, const IOContext& rhs) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  const auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);

  return (std::tie(l->oloc.pool, l->oloc.nspace, l->oloc.key) ==
	  std::tie(r->oloc.pool, r->oloc.nspace, r->oloc.key));
}

bool operator !=(const IOContext& lhs, const IOContext& rhs) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  const auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);

  return (std::tie(l->oloc.pool, l->oloc.nspace, l->oloc.key) !=
	  std::tie(r->oloc.pool, r->oloc.nspace, r->oloc.key));
}

std::ostream& operator <<(std::ostream& m, const IOContext& o) {
  const auto l = reinterpret_cast<const IOContextImpl*>(&o.impl);
  return (m << l->oloc.pool << ":" << l->oloc.nspace << ":" << l->oloc.key);
}


// Op

struct OpImpl {
  ObjectOperation op;
  std::optional<ceph::real_time> mtime;

  OpImpl() = default;

  OpImpl(const OpImpl& rhs) = delete;
  OpImpl(OpImpl&& rhs)
    : op(std::move(rhs.op)), mtime(std::move(rhs.mtime)) {
    rhs.op = ObjectOperation{};
    rhs.mtime.reset();
  }

  OpImpl& operator =(const OpImpl& rhs) = delete;
  OpImpl& operator =(OpImpl&& rhs) {
    op = std::move(rhs.op);
    mtime = std::move(rhs.mtime);

    rhs.op = ObjectOperation{};
    rhs.mtime.reset();

    return *this;
  }
};

Op::Op() {
  static_assert(Op::impl_size >= sizeof(OpImpl));
  new (&impl) OpImpl;
}

Op::Op(Op&& rhs) {
  new (&impl) OpImpl(std::move(*reinterpret_cast<OpImpl*>(&rhs.impl)));
}
Op& Op::operator =(Op&& rhs) {
  reinterpret_cast<OpImpl*>(&impl)->~OpImpl();
  new (&impl) OpImpl(std::move(*reinterpret_cast<OpImpl*>(&rhs.impl)));
  return *this;
}
Op::~Op() {
  reinterpret_cast<OpImpl*>(&impl)->~OpImpl();
}

void Op::set_excl() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(CEPH_OSD_OP_FLAG_EXCL);
}
void Op::set_failok() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FAILOK);
}
void Op::set_fadvise_random() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_RANDOM);
}
void Op::set_fadvise_sequential() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
}
void Op::set_fadvise_willneed() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
}
void Op::set_fadvise_dontneed() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
}
void Op::set_fadvise_nocache() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
}

void Op::cmpext(uint64_t off, bufferlist cmp_bl, uint64_t* unmatch) {
  reinterpret_cast<OpImpl*>(&impl)->op.cmpext(off, std::move(cmp_bl), nullptr,
					      unmatch);
}
void Op::cmpxattr(std::string_view name, cmp_op op, const bufferlist& val) {
  reinterpret_cast<OpImpl*>(&impl)->
    op.cmpxattr(name, std::uint8_t(op), CEPH_OSD_CMPXATTR_MODE_STRING, val);
}
void Op::cmpxattr(std::string_view name, cmp_op op, std::uint64_t val) {
  bufferlist bl;
  encode(val, bl);
  reinterpret_cast<OpImpl*>(&impl)->
    op.cmpxattr(name, std::uint8_t(op), CEPH_OSD_CMPXATTR_MODE_U64, bl);
}

void Op::assert_version(uint64_t ver) {
  reinterpret_cast<OpImpl*>(&impl)->op.assert_version(ver);
}
void Op::assert_exists() {
  reinterpret_cast<OpImpl*>(&impl)->op.stat(
    nullptr,
    static_cast<ceph::real_time*>(nullptr),
    static_cast<bs::error_code*>(nullptr));
}
void Op::cmp_omap(const std::vector<cmp_assertion>& assertions) {
  buffer::list bl;
  encode(uint32_t(assertions.size()), bl);
  for (const auto& [key, op, value] : assertions) {
    encode(key, bl);
    encode(value, bl);
    encode(int(op), bl);
  }
  reinterpret_cast<OpImpl*>(&impl)->op.omap_cmp(std::move(bl), nullptr);
}

void Op::exec(std::string_view cls, std::string_view method,
	      const bufferlist& inbl,
	      cb::list* out,
	      bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.call(cls, method, inbl, ec, out);
}

void Op::exec(std::string_view cls, std::string_view method,
	      const bufferlist& inbl,
	      fu2::unique_function<void(bs::error_code,
					const cb::list&) &&> f) {
  reinterpret_cast<OpImpl*>(&impl)->op.call(cls, method, inbl, std::move(f));
}

void Op::exec(std::string_view cls, std::string_view method,
	      const bufferlist& inbl,
	      fu2::unique_function<void(bs::error_code, int,
					const cb::list&) &&> f) {
  reinterpret_cast<OpImpl*>(&impl)->op.call(cls, method, inbl, std::move(f));
}

void Op::exec(std::string_view cls, std::string_view method,
	      const bufferlist& inbl, bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.call(cls, method, inbl, ec);
}

void Op::balance_reads() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_BALANCE_READS;
}
void Op::localize_reads() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
}
void Op::order_reads_writes() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_RWORDERED;
}
void Op::ignore_cache() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
}
void Op::skiprwlocks() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_SKIPRWLOCKS;
}
void Op::ignore_overlay() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
}
void Op::full_try() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_FULL_TRY;
}
void Op::full_force() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_FULL_FORCE;
}
void Op::ignore_redirect() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_IGNORE_REDIRECT;
}
void Op::ordersnap() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_ORDERSNAP;
}
void Op::returnvec() {
  reinterpret_cast<OpImpl*>(&impl)->op.flags |= CEPH_OSD_FLAG_RETURNVEC;
}

std::size_t Op::size() const {
  return reinterpret_cast<const OpImpl*>(&impl)->op.size();
}

std::ostream& operator <<(std::ostream& m, const Op& o) {
  return m << reinterpret_cast<const OpImpl*>(&o.impl)->op;
}


// ---

// ReadOp / WriteOp

ReadOp& ReadOp::read(size_t off, uint64_t len, cb::list* out,
		      bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.read(off, len, ec, out);
  return *this;
}

ReadOp& ReadOp::get_xattr(std::string_view name, cb::list* out,
			  bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.getxattr(name, ec, out);
  return *this;
}

ReadOp& ReadOp::get_omap_header(cb::list* out,
				bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_header(ec, out);
  return *this;
}

ReadOp& ReadOp::sparse_read(uint64_t off, uint64_t len, cb::list* out,
			    std::vector<std::pair<std::uint64_t,
			    std::uint64_t>>* extents,
			    bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.sparse_read(off, len, ec, extents, out);
  return *this;
}

ReadOp& ReadOp::stat(std::uint64_t* size, ceph::real_time* mtime,
		     bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.stat(size, mtime, ec);
  return *this;
}

ReadOp& ReadOp::get_omap_keys(std::optional<std::string_view> start_after,
			      std::uint64_t max_return,
			      bc::flat_set<std::string>* keys,
			      bool* done,
			      bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_keys(start_after, max_return,
						     ec, keys, done);
  return *this;
}

ReadOp& ReadOp::get_xattrs(bc::flat_map<std::string, cb::list>* kv,
			   bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.getxattrs(ec, kv);
  return *this;
}

ReadOp& ReadOp::get_omap_vals(std::optional<std::string_view> start_after,
			      std::optional<std::string_view> filter_prefix,
			      uint64_t max_return,
			      bc::flat_map<std::string, cb::list>* kv,
			      bool* done,
			      bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_vals(start_after, filter_prefix,
						     max_return, ec, kv, done);
  return *this;
}

ReadOp& ReadOp::get_omap_vals_by_keys(
  const bc::flat_set<std::string>& keys,
  bc::flat_map<std::string, cb::list>* kv,
  bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_vals_by_keys(keys, ec, kv);
  return *this;
}

ReadOp& ReadOp::list_watchers(std::vector<ObjWatcher>* watchers,
			      bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)-> op.list_watchers(watchers, ec);
  return *this;
}

ReadOp& ReadOp::list_snaps(SnapSet* snaps,
			   bs::error_code* ec) & {
  reinterpret_cast<OpImpl*>(&impl)->op.list_snaps(snaps, nullptr, ec);
  return *this;
}

inline uint8_t checksum_op_type(hash_alg::xxhash32_t) {
  return CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32;
}
inline uint8_t checksum_op_type(hash_alg::xxhash64_t) {
  return CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64;
}
inline uint8_t checksum_op_type(hash_alg::crc32c_t) {
    return CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C;
}

template<HashAlg T>
ReadOp& ReadOp::checksum(T t, const typename T::init_value& iv,
			 std::uint64_t off, std::uint64_t len,
			 std::uint64_t chunk_size,
			 std::vector<typename T::hash_value>* out,
			 boost::system::error_code* ec) & {
  using ceph::encode;
  buffer::list init_bl;
  encode(iv, init_bl);
  // If this isn't the case we have a programming error
  assert(init_bl.length() == sizeof(typename T::init_value));
  reinterpret_cast<OpImpl*>(&impl)->op.
    checksum(checksum_op_type(t), std::move(init_bl),
	     off, len, chunk_size,
	     [out](bs::error_code ec, int, const buffer::list& bl) {
	       if (!ec) {
		 std::vector<typename T::hash_value> v;
		 auto bi = bl.begin();
		 decode(v, bi);
		 if (out) {
		   *out = std::move(v);
		 };
	       }
	     }, ec);
  return *this;
}

template
ReadOp& ReadOp::checksum<hash_alg::xxhash32_t>(
  hash_alg::xxhash32_t, const typename hash_alg::xxhash32_t::init_value&,
  std::uint64_t off, std::uint64_t len, std::uint64_t chunk_size,
  std::vector<typename hash_alg::xxhash32_t::hash_value>* out,
  boost::system::error_code* ec) &;
template
ReadOp& ReadOp::checksum<hash_alg::xxhash64_t>(
  hash_alg::xxhash64_t, const typename hash_alg::xxhash64_t::init_value&,
  std::uint64_t off, std::uint64_t len, std::uint64_t chunk_size,
  std::vector<typename hash_alg::xxhash64_t::hash_value>* out,
  boost::system::error_code* ec) &;
template
ReadOp& ReadOp::checksum<hash_alg::crc32c_t>(
  hash_alg::crc32c_t, const typename hash_alg::crc32c_t::init_value&,
  std::uint64_t off, std::uint64_t len, std::uint64_t chunk_size,
  std::vector<typename hash_alg::crc32c_t::hash_value>* out,
  boost::system::error_code* ec) &;

// WriteOp

WriteOp& WriteOp::set_mtime(ceph::real_time t) & {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->mtime = t;
  return *this;
}

WriteOp& WriteOp::create(bool exclusive) & {
  reinterpret_cast<OpImpl*>(&impl)->op.create(exclusive);
  return *this;
}

WriteOp& WriteOp::write(uint64_t off, bufferlist bl) & {
  reinterpret_cast<OpImpl*>(&impl)->op.write(off, std::move(bl));
  return *this;
}

WriteOp& WriteOp::write_full(bufferlist bl) & {
  reinterpret_cast<OpImpl*>(&impl)->op.write_full(std::move(bl));
  return *this;
}

WriteOp& WriteOp::writesame(uint64_t off, uint64_t write_len, bufferlist bl) & {
  reinterpret_cast<OpImpl*>(&impl)->op.writesame(off, write_len, std::move(bl));
  return *this;
}

WriteOp& WriteOp::append(bufferlist bl) & {
  reinterpret_cast<OpImpl*>(&impl)->op.append(std::move(bl));
  return *this;
}

WriteOp& WriteOp::remove() & {
  reinterpret_cast<OpImpl*>(&impl)->op.remove();
  return *this;
}

WriteOp& WriteOp::truncate(uint64_t off) & {
  reinterpret_cast<OpImpl*>(&impl)->op.truncate(off);
  return *this;
}

WriteOp& WriteOp::zero(uint64_t off, uint64_t len) & {
  reinterpret_cast<OpImpl*>(&impl)->op.zero(off, len);
  return *this;
}

WriteOp& WriteOp::rmxattr(std::string_view name) & {
  reinterpret_cast<OpImpl*>(&impl)->op.rmxattr(name);
  return *this;
}

WriteOp& WriteOp::setxattr(std::string_view name,
			   bufferlist bl) & {
  reinterpret_cast<OpImpl*>(&impl)->op.setxattr(name, std::move(bl));
  return *this;
}

WriteOp& WriteOp::rollback(uint64_t snapid) & {
  reinterpret_cast<OpImpl*>(&impl)->op.rollback(snapid);
  return *this;
}

WriteOp& WriteOp::set_omap(
  const bc::flat_map<std::string, cb::list>& map) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_set(map);
  return *this;
}

WriteOp& WriteOp::set_omap_header(bufferlist bl) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_set_header(std::move(bl));
  return *this;
}

WriteOp& WriteOp::clear_omap() & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_clear();
  return *this;
}

WriteOp& WriteOp::rm_omap_keys(const bc::flat_set<std::string>& to_rm) & {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_rm_keys(to_rm);
  return *this;
}

WriteOp& WriteOp::set_alloc_hint(uint64_t expected_object_size,
				 uint64_t expected_write_size,
				 alloc_hint::alloc_hint_t flags) & {
  using namespace alloc_hint;
  static_assert(sequential_write ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE));
  static_assert(random_write ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE));
  static_assert(sequential_read ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ));
  static_assert(random_read ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ));
  static_assert(append_only ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY));
  static_assert(immutable ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_IMMUTABLE));
  static_assert(shortlived ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_SHORTLIVED));
  static_assert(longlived ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_LONGLIVED));
  static_assert(compressible ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE));
  static_assert(incompressible ==
		static_cast<int>(CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE));

  reinterpret_cast<OpImpl*>(&impl)->op.set_alloc_hint(expected_object_size,
						      expected_write_size,
						      flags);
  return *this;
}

// RADOS

RADOS::Builder& RADOS::Builder::add_conf_file(std::string_view f) {
  if (conf_files)
    *conf_files += (", " + std::string(f));
  else
    conf_files = std::string(f);
  return *this;
}

void RADOS::Builder::build_(asio::io_context& ioctx,
			    BuildComp c) {
  constexpr auto env = CODE_ENVIRONMENT_LIBRARY;
  CephInitParameters ci(env);
  if (name)
    ci.name.set(CEPH_ENTITY_TYPE_CLIENT, *name);
  else
    ci.name.set(CEPH_ENTITY_TYPE_CLIENT, "admin");
  uint32_t flags = 0;
  if (no_default_conf)
    flags |= CINIT_FLAG_NO_DEFAULT_CONFIG_FILE;
  if (no_mon_conf)
    flags |= CINIT_FLAG_NO_MON_CONFIG;

  CephContext *cct = common_preinit(ci, env, flags);
  if (cluster)
    cct->_conf->cluster = *cluster;

  if (no_mon_conf)
    cct->_conf->no_mon_config = true;

  // TODO: Come up with proper error codes here. Maybe augment the
  // functions with a default bs::error_code* parameter to
  // pass back.
  {
    std::ostringstream ss;
    auto r = cct->_conf.parse_config_files(conf_files ? conf_files->data() : nullptr,
					   &ss, flags);
    if (r < 0)
      asio::post(ioctx.get_executor(),
		 asio::append(std::move(c), ceph::to_error_code(r),
			      RADOS{nullptr}));
  }

  cct->_conf.parse_env(cct->get_module_type());

  for (const auto& [n, v] : configs) {
    std::stringstream ss;
    auto r = cct->_conf.set_val(n, v, &ss);
    if (r < 0)
      asio::post(ioctx.get_executor(),
		 asio::append(std::move(c), ceph::to_error_code(-EINVAL),
			      RADOS{nullptr}));
  }

  if (!no_mon_conf) {
    MonClient mc_bootstrap(cct, ioctx);
    // TODO This function should return an error code.
    auto err = mc_bootstrap.get_monmap_and_config();
    if (err < 0)
      asio::post(ioctx.get_executor(),
		 asio::append(std::move(c), ceph::to_error_code(err),
			      RADOS{nullptr}));
  }
  if (!cct->_log->is_started()) {
    cct->_log->start();
  }
  common_init_finish(cct);

  RADOS::make_with_cct(cct, ioctx, std::move(c));
}

void RADOS::make_with_cct_(CephContext* cct,
			   asio::io_context& ioctx,
			   BuildComp c) {
  try {
    auto r = new detail::NeoClient{std::make_unique<detail::RADOS>(ioctx, cct)};
    r->objecter->wait_for_osd_map(
      [c = std::move(c), r = std::unique_ptr<detail::Client>(r)]() mutable {
	asio::dispatch(asio::append(std::move(c), bs::error_code{},
				    RADOS{std::move(r)}));
      });
  } catch (const bs::system_error& err) {
    asio::post(ioctx.get_executor(),
	       asio::append(std::move(c), err.code(), RADOS{nullptr}));
  }
}

RADOS RADOS::make_with_librados(librados::Rados& rados) {
  return RADOS{std::make_unique<detail::RadosClient>(rados.client)};
}

RADOS::RADOS() = default;

RADOS::RADOS(std::unique_ptr<detail::Client> impl)
  : impl(std::move(impl)) {}

RADOS::RADOS(RADOS&&) = default;
RADOS& RADOS::operator =(RADOS&&) = default;

RADOS::~RADOS() = default;

RADOS::executor_type RADOS::get_executor() const {
  return impl->ioctx.get_executor();
}

asio::io_context& RADOS::get_io_context() {
  return impl->ioctx;
}

void RADOS::execute_(Object o, IOContext _ioc, ReadOp _op,
		     cb::list* bl,
		     ReadOp::Completion c, version_t* objver,
		     const blkin_trace_info *trace_info) {
  if (_op.size() == 0) {
    asio::dispatch(asio::append(std::move(c), bs::error_code{}));
    return;
  }
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = op->op.flags | ioc->extra_op_flags;

  ZTracer::Trace trace;
  if (trace_info) {
    ZTracer::Trace parent_trace("", nullptr, trace_info);
    trace.init("rados execute", &impl->objecter->trace_endpoint, &parent_trace);
  }

  trace.event("init");
  impl->objecter->read(
    *oid, ioc->oloc, std::move(op->op), ioc->snap_seq, bl, flags,
    std::move(c), objver, nullptr /* data_offset */, 0 /* features */, &trace);

  trace.event("submitted");
}

void RADOS::execute_(Object o, IOContext _ioc, WriteOp _op,
		     WriteOp::Completion c, version_t* objver,
		     const blkin_trace_info *trace_info) {
  if (_op.size() == 0) {
    asio::dispatch(asio::append(std::move(c), bs::error_code{}));
    return;
  }
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = op->op.flags | ioc->extra_op_flags;
  ceph::real_time mtime;
  if (op->mtime)
    mtime = *op->mtime;
  else
    mtime = ceph::real_clock::now();

  ZTracer::Trace trace;
  if (trace_info) {
    ZTracer::Trace parent_trace("", nullptr, trace_info);
    trace.init("rados execute", &impl->objecter->trace_endpoint, &parent_trace);
  }

  trace.event("init");
  impl->objecter->mutate(
    *oid, ioc->oloc, std::move(op->op), ioc->snapc,
    mtime, flags,
    std::move(c), objver, osd_reqid_t{}, &trace);
  trace.event("submitted");
}

boost::uuids::uuid RADOS::get_fsid() const noexcept {
  return impl->monclient.get_fsid().uuid;
}

void RADOS::lookup_pool_(std::string name, LookupPoolComp c)
{
  // I kind of want to make lookup_pg_pool return
  // std::optional<int64_t> since it can only return one error code.
  int64_t ret = impl->objecter->with_osdmap(
    std::mem_fn(&OSDMap::lookup_pg_pool_name),
    name);
  if (ret < 0) {
    impl->objecter->wait_for_latest_osdmap(
      [name = std::move(name), c = std::move(c),
       objecter = impl->objecter](bs::error_code ec) mutable {
	int64_t ret =
	  objecter->with_osdmap([&](const OSDMap &osdmap) {
	    return osdmap.lookup_pg_pool_name(name);
	  });
	if (ret < 0)
	  asio::dispatch(asio::append(std::move(c), osdc_errc::pool_dne,
				      std::int64_t(0)));
	else
	  asio::dispatch(asio::append(std::move(c), bs::error_code{}, ret));
      });
  } else {
    asio::post(get_executor(),
	       asio::append(std::move(c), bs::error_code{}, ret));
  }
}


std::optional<uint64_t> RADOS::get_pool_alignment(int64_t pool_id)
{
  return impl->objecter->with_osdmap(
    [pool_id](const OSDMap &o) -> std::optional<uint64_t> {
      if (!o.have_pg_pool(pool_id)) {
	throw bs::system_error(
	  ENOENT, bs::system_category(),
	  "Cannot find pool in OSDMap.");
      } else if (o.get_pg_pool(pool_id)->requires_aligned_append()) {
	return o.get_pg_pool(pool_id)->required_alignment();
      } else {
	return std::nullopt;
      }
    });
}

void RADOS::list_pools_(LSPoolsComp c) {
  asio::dispatch(asio::append(std::move(c),
			      impl->objecter->with_osdmap(
				[&](OSDMap& o) {
				  std::vector<std::pair<std::int64_t, std::string>> v;
				  for (auto p : o.get_pools())
				    v.push_back(std::make_pair(p.first,
							       o.get_pool_name(p.first)));
				  return v;
				})));
}

void RADOS::create_pool_snap_(std::int64_t pool,
			      std::string snap_name,
			      SimpleOpComp c)
{
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->create_pool_snap(
    pool, snap_name,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      }));
}

void RADOS::allocate_selfmanaged_snap_(int64_t pool,
				       SMSnapComp c) {
  auto e = asio::prefer(
    get_executor(),
    asio::execution::outstanding_work.tracked);

  impl->objecter->allocate_selfmanaged_snap(
    pool,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, snapid_t snap) mutable {
	asio::dispatch(asio::append(std::move(c), e, snap));
      }));
}

void RADOS::delete_pool_snap_(std::int64_t pool,
			      std::string snap_name,
			      SimpleOpComp c)
{
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->delete_pool_snap(
    pool, snap_name,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      }));
}

void RADOS::delete_selfmanaged_snap_(std::int64_t pool,
				     std::uint64_t snap,
				     SimpleOpComp c)
{
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->delete_selfmanaged_snap(
    pool, snap,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      }));
}

bool RADOS::get_self_managed_snaps_mode(std::int64_t pool) const {
  return impl->objecter->with_osdmap([pool](const OSDMap& osdmap) {
    const auto pgpool = osdmap.get_pg_pool(pool);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    return pgpool->is_unmanaged_snaps_mode();
  });
}

bool RADOS::get_self_managed_snaps_mode(std::string_view pool) const {
  return impl->objecter->with_osdmap([pool](const OSDMap& osdmap) {
    int64_t poolid = osdmap.lookup_pg_pool_name(pool);
    if (poolid < 0) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    const auto pgpool = osdmap.get_pg_pool(poolid);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    return pgpool->is_unmanaged_snaps_mode();
  });
}

std::vector<std::uint64_t> RADOS::list_snaps(std::int64_t pool) const {
  return impl->objecter->with_osdmap([pool](const OSDMap& osdmap) {
    const auto pgpool = osdmap.get_pg_pool(pool);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    std::vector<std::uint64_t> snaps;
    for (const auto& [snapid, snapinfo] : pgpool->snaps) {
      snaps.push_back(snapid);
    }
    return snaps;
  });
}

std::vector<std::uint64_t> RADOS::list_snaps(std::string_view pool) const {
  return impl->objecter->with_osdmap([pool](const OSDMap& osdmap) {
    int64_t poolid = osdmap.lookup_pg_pool_name(pool);
    if (poolid < 0) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    const auto pgpool = osdmap.get_pg_pool(poolid);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    std::vector<std::uint64_t> snaps;
    for (const auto& [snapid, snapinfo] : pgpool->snaps) {
      snaps.push_back(snapid);
    }
    return snaps;
  });
}

std::uint64_t RADOS::lookup_snap(std::int64_t pool, std::string_view snap) const {
  return impl->objecter->with_osdmap([pool, snap](const OSDMap& osdmap) {
    const auto pgpool = osdmap.get_pg_pool(pool);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    for (const auto& [id, snapinfo] : pgpool->snaps) {
      if (snapinfo.name == snap) return id;
    }
    throw bs::system_error(bs::error_code(errc::snap_dne));
  });
}

std::uint64_t RADOS::lookup_snap(std::string_view pool, std::string_view snap) const {
  return impl->objecter->with_osdmap([pool, snap](const OSDMap& osdmap) {
    int64_t poolid = osdmap.lookup_pg_pool_name(pool);
    if (poolid < 0) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    const auto pgpool = osdmap.get_pg_pool(poolid);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    for (const auto& [id, snapinfo] : pgpool->snaps) {
      if (snapinfo.name == snap) return id;
    }
    throw bs::system_error(bs::error_code(errc::snap_dne));
  });
}

std::string RADOS::get_snap_name(std::int64_t pool, std::uint64_t snap) const {
  return impl->objecter->with_osdmap([pool, snap](const OSDMap& osdmap) {
    const auto pgpool = osdmap.get_pg_pool(pool);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    if (auto i = pgpool->snaps.find(snap); i == pgpool->snaps.cend()) {
      throw bs::system_error(bs::error_code(errc::snap_dne));
    } else {
      return i->second.name;
    }
  });
}
std::string RADOS::get_snap_name(std::string_view pool,
				 std::uint64_t snap) const {
  return impl->objecter->with_osdmap([pool, snap](const OSDMap& osdmap) {
    int64_t poolid = osdmap.lookup_pg_pool_name(pool);
    if (poolid < 0) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    const auto pgpool = osdmap.get_pg_pool(poolid);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    if (auto i = pgpool->snaps.find(snap); i == pgpool->snaps.cend()) {
      throw bs::system_error(bs::error_code(errc::snap_dne));
    } else {
      return i->second.name;
    }
  });
}

ceph::real_time RADOS::get_snap_timestamp(std::int64_t pool,
					  std::uint64_t snap) const {
  return impl->objecter->with_osdmap([pool, snap](const OSDMap& osdmap) {
    const auto pgpool = osdmap.get_pg_pool(pool);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    if (auto i = pgpool->snaps.find(snap); i == pgpool->snaps.cend()) {
      throw bs::system_error(bs::error_code(errc::snap_dne));
    } else {
      return i->second.stamp.to_real_time();
    }
  });
}
ceph::real_time RADOS::get_snap_timestamp(std::string_view pool,
					  std::uint64_t snap) const {
  return impl->objecter->with_osdmap([pool, snap](const OSDMap& osdmap) {
    int64_t poolid = osdmap.lookup_pg_pool_name(pool);
    if (poolid < 0) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    const auto pgpool = osdmap.get_pg_pool(poolid);
    if (!pgpool) {
      throw bs::system_error(bs::error_code(errc::pool_dne));
    }
    if (auto i = pgpool->snaps.find(snap); i == pgpool->snaps.cend()) {
      throw bs::system_error(bs::error_code(errc::snap_dne));
    } else {
      return i->second.stamp.to_real_time();
    }
  });
}

void RADOS::create_pool_(std::string name,
			 std::optional<int> crush_rule,
			 SimpleOpComp c)
{
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);

  impl->objecter->create_pool(
    name,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      }),
      crush_rule.value_or(-1));
}

void RADOS::delete_pool_(std::string name, SimpleOpComp c)
{
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->delete_pool(
    name,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      }));
}

void RADOS::delete_pool_(std::int64_t pool,
			 SimpleOpComp c)
{
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->delete_pool(
    pool,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      }));
}

void RADOS::stat_pools_(std::vector<std::string> pools,
			PoolStatComp c) {
  impl->objecter->get_pool_stats(
    pools,
    [c = std::move(c)]
    (bs::error_code ec,
     bc::flat_map<std::string, pool_stat_t> rawresult,
     bool per_pool) mutable {
      bc::flat_map<std::string, PoolStats> result;
      for (auto p = rawresult.begin(); p != rawresult.end(); ++p) {
	auto& pv = result[p->first];
	auto& pstat = p->second;
	store_statfs_t &statfs = pstat.store_stats;
	uint64_t allocated_bytes = pstat.get_allocated_data_bytes(per_pool) +
	  pstat.get_allocated_omap_bytes(per_pool);
	// FIXME: raw_used_rate is unknown hence use 1.0 here
	// meaning we keep net amount aggregated over all replicas
	// Not a big deal so far since this field isn't exposed
	uint64_t user_bytes = pstat.get_user_data_bytes(1.0, per_pool) +
	  pstat.get_user_omap_bytes(1.0, per_pool);

	object_stat_sum_t *sum = &p->second.stats.sum;
	pv.num_kb = shift_round_up(allocated_bytes, 10);
	pv.num_bytes = allocated_bytes;
	pv.num_objects = sum->num_objects;
	pv.num_object_clones = sum->num_object_clones;
	pv.num_object_copies = sum->num_object_copies;
	pv.num_objects_missing_on_primary = sum->num_objects_missing_on_primary;
	pv.num_objects_unfound = sum->num_objects_unfound;
	pv.num_objects_degraded = sum->num_objects_degraded;
	pv.num_rd = sum->num_rd;
	pv.num_rd_kb = sum->num_rd_kb;
	pv.num_wr = sum->num_wr;
	pv.num_wr_kb = sum->num_wr_kb;
	pv.num_user_bytes = user_bytes;
	pv.compressed_bytes_orig = statfs.data_compressed_original;
	pv.compressed_bytes = statfs.data_compressed;
	pv.compressed_bytes_alloc = statfs.data_compressed_allocated;
      }

      asio::dispatch(asio::append(std::move(c), ec, std::move(result),
				  per_pool));
    });
}

void RADOS::stat_fs_(std::optional<std::int64_t> _pool,
		     StatFSComp c) {
  std::optional<int64_t> pool;
  if (_pool)
    pool = *pool;
  impl->objecter->get_fs_stats(
    pool,
    [c = std::move(c)](bs::error_code ec, const struct ceph_statfs s) mutable {
      FSStats fso{s.kb, s.kb_used, s.kb_avail, s.num_objects};
      asio::dispatch(asio::append(std::move(c), ec, std::move(fso)));
    });
}

// --- Watch/Notify

void RADOS::watch_(Object o, IOContext _ioc,
		   std::optional<std::chrono::seconds> timeout, WatchCB cb,
		   WatchComp c) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  ObjectOperation op;

  auto linger_op = impl->objecter->linger_register(*oid, ioc->oloc,
                                                   ioc->extra_op_flags);
  uint64_t cookie = linger_op->get_cookie();
  linger_op->handle = std::move(cb);
  op.watch(cookie, CEPH_OSD_WATCH_OP_WATCH, timeout.value_or(0s).count());
  bufferlist bl;
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->linger_watch(
    linger_op, op, ioc->snapc, ceph::real_clock::now(), bl,
    asio::bind_executor(
      std::move(e),
      [c = std::move(c), cookie](bs::error_code e, cb::list) mutable {
	asio::dispatch(asio::append(std::move(c), e, cookie));
      }), nullptr);
}

void RADOS::notify_ack_(Object o, IOContext _ioc,
			uint64_t notify_id,
			uint64_t cookie,
			bufferlist bl,
			SimpleOpComp c)
{
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  ObjectOperation op;
  op.notify_ack(notify_id, cookie, bl);

  impl->objecter->read(*oid, ioc->oloc, std::move(op), ioc->snap_seq,
		       nullptr, ioc->extra_op_flags, std::move(c));
}

tl::expected<ceph::timespan, bs::error_code> RADOS::check_watch(uint64_t cookie)
{
  auto linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  if (impl->objecter->is_valid_watch(linger_op)) {
    return impl->objecter->linger_check(linger_op);
  } else {
    return tl::unexpected(bs::error_code(ENOTCONN, bs::generic_category()));
  }
}

void RADOS::unwatch_(uint64_t cookie, IOContext _ioc,
		     SimpleOpComp c)
{
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);

  ObjectOperation op;
  op.watch(cookie, CEPH_OSD_WATCH_OP_UNWATCH);
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  impl->objecter->mutate(linger_op->target.base_oid, ioc->oloc, std::move(op),
			 ioc->snapc, ceph::real_clock::now(), ioc->extra_op_flags,
			 asio::bind_executor(
			   std::move(e),
			   [objecter = impl->objecter,
			    linger_op, c = std::move(c)]
			   (bs::error_code ec) mutable {
			     objecter->linger_cancel(linger_op);
			     asio::dispatch(asio::append(std::move(c), ec));
			   }));
}

void RADOS::flush_watch_(VoidOpComp c)
{
  impl->objecter->linger_callback_flush([c = std::move(c)]() mutable {
					  asio::dispatch(std::move(c));
					});
}

struct NotifyHandler : std::enable_shared_from_this<NotifyHandler> {
  asio::io_context& ioc;
  asio::strand<asio::io_context::executor_type> strand;
  Objecter* objecter;
  Objecter::LingerOp* op;
  RADOS::NotifyComp c;

  bool acked = false;
  bool finished = false;
  bs::error_code res;
  bufferlist rbl;

  NotifyHandler(asio::io_context& ioc,
		Objecter* objecter,
		Objecter::LingerOp* op,
		RADOS::NotifyComp c)
    : ioc(ioc), strand(asio::make_strand(ioc)),
      objecter(objecter), op(op), c(std::move(c)) {}

  // Use bind or a lambda to pass this in.
  void handle_ack(bs::error_code ec,
		  bufferlist&&) {
    asio::post(
      strand,
      [this, ec, p = shared_from_this()]() mutable {
	acked = true;
	maybe_cleanup(ec);
      });
  }

  // Notify finish callback. It can actually own the object's storage.

  void operator()(bs::error_code ec,
		  bufferlist&& bl) {
    asio::post(
      strand,
      [this, ec, bl = std::move(bl), p = shared_from_this()]() mutable {
	finished = true;
	rbl = std::move(bl);
	maybe_cleanup(ec);
      });
  }

  // Should be called from strand.
  void maybe_cleanup(bs::error_code ec) {
    if (!res && ec)
      res = ec;
    if ((acked && finished) || res) {
      objecter->linger_cancel(op);
      ceph_assert(c);
      asio::dispatch(asio::append(std::move(c), res, std::move(rbl)));
    }
  }
};

void RADOS::notify_(Object o, IOContext _ioc, bufferlist bl,
		    std::optional<std::chrono::seconds> timeout,
		    NotifyComp c)
{
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto linger_op = impl->objecter->linger_register(*oid, ioc->oloc,
                                                   ioc->extra_op_flags);

  auto cb = std::make_shared<NotifyHandler>(impl->ioctx, impl->objecter,
                                            linger_op, std::move(c));
  auto e = asio::prefer(get_executor(),
			asio::execution::outstanding_work.tracked);
  linger_op->on_notify_finish =
    asio::bind_executor(
      e,
      [cb](bs::error_code ec, ceph::bufferlist bl) mutable {
	(*cb)(ec, std::move(bl));
      });
  ObjectOperation rd;
  bufferlist inbl;
  // 30s is the default in librados. Use that rather than borrowing from CephFS.
  // TODO add a config option later.
  rd.notify(
    linger_op->get_cookie(), 1,
    timeout.value_or(30s).count(),
    bl, &inbl);

  impl->objecter->linger_notify(
    linger_op, rd, ioc->snap_seq, inbl,
    asio::bind_executor(
      e,
      [cb](bs::error_code ec, ceph::bufferlist bl) mutable {
	cb->handle_ack(ec, std::move(bl));
      }), nullptr);
}

// Enumeration

Cursor::Cursor() {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t();
};

Cursor::Cursor(end_magic_t) {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t(hobject_t::get_max());
}

Cursor::Cursor(void* p) {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t(std::move(*reinterpret_cast<hobject_t*>(p)));
}

Cursor Cursor::begin() {
  Cursor e;
  return e;
}

Cursor Cursor::end() {
  Cursor e(end_magic_t{});
  return e;
}

Cursor::Cursor(const Cursor& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t(*reinterpret_cast<const hobject_t*>(&rhs.impl));
}

Cursor& Cursor::operator =(const Cursor& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  reinterpret_cast<hobject_t*>(&impl)->~hobject_t();
  new (&impl) hobject_t(*reinterpret_cast<const hobject_t*>(&rhs.impl));
  return *this;
}

Cursor::Cursor(Cursor&& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t(std::move(*reinterpret_cast<hobject_t*>(&rhs.impl)));
}

Cursor& Cursor::operator =(Cursor&& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  reinterpret_cast<hobject_t*>(&impl)->~hobject_t();
  new (&impl) hobject_t(std::move(*reinterpret_cast<hobject_t*>(&rhs.impl)));
  return *this;
}
Cursor::~Cursor() {
  reinterpret_cast<hobject_t*>(&impl)->~hobject_t();
}

bool operator ==(const Cursor& lhs, const Cursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) ==
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator !=(const Cursor& lhs, const Cursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) !=
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator <(const Cursor& lhs, const Cursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) <
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator <=(const Cursor& lhs, const Cursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) <=
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator >=(const Cursor& lhs, const Cursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) >=
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator >(const Cursor& lhs, const Cursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) >
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

std::string Cursor::to_str() const {
  using namespace std::literals;
  auto& h = *reinterpret_cast<const hobject_t*>(&impl);

  return h.is_max() ? "MAX"s : h.to_str();
}

std::optional<Cursor>
Cursor::from_str(const std::string& s) {
  Cursor e;
  auto& h = *reinterpret_cast<hobject_t*>(&e.impl);
  if (!h.parse(s))
    return std::nullopt;

  return e;
}

void RADOS::enumerate_objects_(IOContext _ioc, Cursor begin, Cursor end,
			       const std::uint32_t max,
			       bufferlist filter,
			       EnumerateComp c) {
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  impl->objecter->enumerate_objects<Entry>(
    ioc->oloc.pool,
    ioc->oloc.nspace,
    *reinterpret_cast<const hobject_t*>(&begin.impl),
    *reinterpret_cast<const hobject_t*>(&end.impl),
    max,
    filter,
    [c = std::move(c)]
    (bs::error_code ec, std::vector<Entry>&& v,
     hobject_t&& n) mutable {
      asio::dispatch(asio::append(std::move(c), ec, std::move(v),
				  Cursor(static_cast<void*>(&n))));
    });
}

void RADOS::osd_command_(int osd, std::vector<std::string> cmd,
			 ceph::bufferlist in, CommandComp c) {
  impl->objecter->osd_command(
    osd, std::move(cmd), std::move(in), nullptr,
    [c = std::move(c)]
    (bs::error_code ec, std::string&& s, ceph::bufferlist&& b) mutable {
      asio::dispatch(asio::append(std::move(c), ec, std::move(s),
				  std::move(b)));
    });
}

void RADOS::pg_command_(PG pg, std::vector<std::string> cmd,
			ceph::bufferlist in, CommandComp c) {
  impl->objecter->pg_command(
    pg_t{pg.seed, pg.pool}, std::move(cmd), std::move(in), nullptr,
    [c = std::move(c)]
    (bs::error_code ec, std::string&& s,
     ceph::bufferlist&& b) mutable {
      asio::dispatch(asio::append(std::move(c), ec, std::move(s),
				  std::move(b)));
    });
}

void RADOS::enable_application_(std::string pool, std::string app_name,
				bool force, SimpleOpComp c) {
  // pre-Luminous clusters will return -EINVAL and application won't be
  // preserved until Luminous is configured as minimum version.
  if (!impl->get_required_monitor_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    asio::post(get_executor(),
	       asio::append(std::move(c), ceph::to_error_code(-EOPNOTSUPP)));
  } else {
    impl->monclient.start_mon_command(
      { fmt::format("{{ \"prefix\": \"osd pool application enable\","
		    "\"pool\": \"{}\", \"app\": \"{}\"{}}}",
		    pool, app_name,
		    force ? " ,\"yes_i_really_mean_it\": true" : "")},
      {}, [c = std::move(c)](bs::error_code e,
			     std::string, cb::list) mutable {
	asio::dispatch(asio::append(std::move(c), e));
      });
  }
}

void RADOS::blocklist_add_(std::string client_address,
			   std::optional<std::chrono::seconds> expire,
			   SimpleOpComp c) {
  auto expire_arg = (expire ?
    fmt::format(", \"expire\": \"{}.0\"", expire->count()) : std::string{});
  impl->monclient.start_mon_command(
    { fmt::format("{{"
                  "\"prefix\": \"osd blocklist\", "
                  "\"blocklistop\": \"add\", "
                  "\"addr\": \"{}\"{}}}",
                  client_address, expire_arg) },
    {},
    [this, client_address = std::string(client_address), expire_arg,
     c = std::move(c)](bs::error_code ec, std::string, cb::list) mutable {
      if (ec != bs::errc::invalid_argument) {
        asio::post(get_executor(),
		   asio::append(std::move(c), ec));
        return;
      }

      // retry using the legacy command
      impl->monclient.start_mon_command(
        { fmt::format("{{"
                      "\"prefix\": \"osd blacklist\", "
                      "\"blacklistop\": \"add\", "
                      "\"addr\": \"{}\"{}}}",
                      client_address, expire_arg) },
        {},
        [c = std::move(c)](bs::error_code ec, std::string, cb::list) mutable {
          asio::dispatch(asio::append(std::move(c), ec));
        });
    });
}

void RADOS::wait_for_latest_osd_map_(SimpleOpComp c) {
  impl->objecter->wait_for_latest_osdmap(std::move(c));
}

void RADOS::mon_command_(std::vector<std::string> command,
			 cb::list bl, std::string* outs, cb::list* outbl,
			 SimpleOpComp c) {

  impl->monclient.start_mon_command(
    command, bl,
    [c = std::move(c), outs, outbl](bs::error_code e,
				    std::string s, cb::list bl) mutable {
      if (outs)
	*outs = std::move(s);
      if (outbl)
	*outbl = std::move(bl);
      asio::dispatch(asio::append(std::move(c), e));
    });
}

uint64_t RADOS::instance_id() const {
  return impl->get_instance_id();
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"
class category : public ceph::converting_category {
public:
  category() {}
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

const char* category::name() const noexcept {
  return "RADOS";
}

const char* category::message(int ev, char*,
			      std::size_t) const noexcept {
  if (ev == 0)
    return "No error";

  switch (static_cast<errc>(ev)) {
  case errc::pool_dne:
    return "Pool does not exist";
  case errc::snap_dne:
    return "Snapshot does not exist";
  case errc::invalid_snapcontext:
    return "Invalid snapcontext";
  }

  return "Unknown error";
}

std::string category::message(int ev) const {
  return message(ev, nullptr, 0);
}

bs::error_condition category::default_error_condition(int ev) const noexcept {
  switch (static_cast<errc>(ev)) {
  case errc::pool_dne:
    return ceph::errc::does_not_exist;
  case errc::snap_dne:
    return ceph::errc::does_not_exist;
  case errc::invalid_snapcontext:
    return bs::errc::invalid_argument;
  }

  return { ev, *this };
}

bool category::equivalent(int ev, const bs::error_condition& c) const noexcept {
  if (static_cast<errc>(ev) == errc::pool_dne) {
    if (c == bs::errc::no_such_file_or_directory) {
      return true;
    }
  }
  if (static_cast<errc>(ev) == errc::snap_dne) {
    if (c == bs::errc::no_such_file_or_directory) {
      return true;
    }
  }

  return default_error_condition(ev) == c;
}

int category::from_code(int ev) const noexcept {
  switch (static_cast<errc>(ev)) {
  case errc::pool_dne:
    return -ENOENT;
  case errc::snap_dne:
    return -ENOENT;
  case errc::invalid_snapcontext:
    return -EINVAL;
  }
  return -EDOM;
}

const bs::error_category& error_category() noexcept {
  static const class category c;
  return c;
}

CephContext* RADOS::cct() {
  return impl->cct.get();
}
}

namespace std {
size_t hash<neorados::Object>::operator ()(
  const neorados::Object& r) const {
  static constexpr const hash<object_t> H;
  return H(*reinterpret_cast<const object_t*>(&r.impl));
}

size_t hash<neorados::IOContext>::operator ()(
  const neorados::IOContext& r) const {
  static constexpr const hash<int64_t> H;
  static constexpr const hash<std::string> G;
  const auto l = reinterpret_cast<const neorados::IOContextImpl*>(&r.impl);
  return H(l->oloc.pool) ^ (G(l->oloc.nspace) << 1) ^ (G(l->oloc.key) << 2);
}
}
