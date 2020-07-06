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

#include "global/global_init.h"

#include "osd/osd_types.h"
#include "osdc/error_code.h"

#include "neorados/RADOSImpl.h"
#include "include/neorados/RADOS.hpp"

namespace bc = boost::container;
namespace bs = boost::system;
namespace ca = ceph::async;
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
};

IOContext::IOContext() {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl();
}

IOContext::IOContext(std::int64_t _pool) : IOContext() {
  pool(_pool);
}

IOContext::IOContext(std::int64_t _pool, std::string_view _ns)
  : IOContext() {
  pool(_pool);
  ns(_ns);
}

IOContext::IOContext(std::int64_t _pool, std::string&& _ns)
  : IOContext() {
  pool(_pool);
  ns(std::move(_ns));
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

std::int64_t IOContext::pool() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.pool;
}

void IOContext::pool(std::int64_t _pool) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.pool = _pool;
}

std::string_view IOContext::ns() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.nspace;
}

void IOContext::ns(std::string_view _ns) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.nspace = _ns;
}

void IOContext::ns(std::string&& _ns) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.nspace = std::move(_ns);
}

std::optional<std::string_view> IOContext::key() const {
  auto& oloc = reinterpret_cast<const IOContextImpl*>(&impl)->oloc;
  if (oloc.key.empty())
    return std::nullopt;
  else
    return std::string_view(oloc.key);
}

void IOContext::key(std::string_view _key) {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key = _key;
}

void IOContext::key(std::string&&_key) {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key = std::move(_key);
}

void IOContext::clear_key() {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key.clear();
}

std::optional<std::int64_t> IOContext::hash() const {
  auto& oloc = reinterpret_cast<const IOContextImpl*>(&impl)->oloc;
  if (oloc.hash < 0)
    return std::nullopt;
  else
    return oloc.hash;
}

void IOContext::hash(std::int64_t _hash) {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = _hash;
  oloc.key.clear();
}

void IOContext::clear_hash() {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key.clear();
}


std::optional<std::uint64_t> IOContext::read_snap() const {
  auto& snap_seq = reinterpret_cast<const IOContextImpl*>(&impl)->snap_seq;
  if (snap_seq == CEPH_NOSNAP)
    return std::nullopt;
  else
    return snap_seq;
}
void IOContext::read_snap(std::optional<std::uint64_t> _snapid) {
  auto& snap_seq = reinterpret_cast<IOContextImpl*>(&impl)->snap_seq;
  snap_seq = _snapid.value_or(CEPH_NOSNAP);
}

std::optional<
  std::pair<std::uint64_t,
	    std::vector<std::uint64_t>>> IOContext::write_snap_context() const {
  auto& snapc = reinterpret_cast<const IOContextImpl*>(&impl)->snapc;
  if (snapc.empty()) {
    return std::nullopt;
  } else {
    std::vector<uint64_t> v(snapc.snaps.begin(), snapc.snaps.end());
    return std::make_optional(std::make_pair(uint64_t(snapc.seq), v));
  }
}

void IOContext::write_snap_context(
  std::optional<std::pair<std::uint64_t, std::vector<std::uint64_t>>> _snapc) {
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
  OpImpl(OpImpl&& rhs) = default;

  OpImpl& operator =(const OpImpl& rhs) = delete;
  OpImpl& operator =(OpImpl&& rhs) = default;
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

void Op::cmpext(uint64_t off, bufferlist&& cmp_bl, std::size_t* s) {
  reinterpret_cast<OpImpl*>(&impl)->op.cmpext(off, std::move(cmp_bl), nullptr,
					      s);
}
void Op::cmpxattr(std::string_view name, cmpxattr_op op, const bufferlist& val) {
  reinterpret_cast<OpImpl*>(&impl)->
    op.cmpxattr(name, std::uint8_t(op), CEPH_OSD_CMPXATTR_MODE_STRING, val);
}
void Op::cmpxattr(std::string_view name, cmpxattr_op op, std::uint64_t val) {
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
void Op::cmp_omap(const bc::flat_map<
		  std::string, std::pair<cb::list,
		  int>>& assertions) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_cmp(assertions, nullptr);
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

void ReadOp::read(size_t off, uint64_t len, cb::list* out,
		  bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.read(off, len, ec, out);
}

void ReadOp::get_xattr(std::string_view name, cb::list* out,
		       bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.getxattr(name, ec, out);
}

void ReadOp::get_omap_header(cb::list* out,
			     bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_header(ec, out);
}

void ReadOp::sparse_read(uint64_t off, uint64_t len, cb::list* out,
			 std::vector<std::pair<std::uint64_t,
			 std::uint64_t>>* extents,
			 bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.sparse_read(off, len, ec, extents, out);
}

void ReadOp::stat(std::uint64_t* size, ceph::real_time* mtime,
		  bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.stat(size, mtime, ec);
}

void ReadOp::get_omap_keys(std::optional<std::string_view> start_after,
			   std::uint64_t max_return,
			   bc::flat_set<std::string>* keys,
			   bool* done,
			   bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_keys(start_after, max_return,
						     ec, keys, done);
}

void ReadOp::get_xattrs(bc::flat_map<std::string,
			cb::list>* kv,
			bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.getxattrs(ec, kv);
}

void ReadOp::get_omap_vals(std::optional<std::string_view> start_after,
			   std::optional<std::string_view> filter_prefix,
			   uint64_t max_return,
			   bc::flat_map<std::string,
			   cb::list>* kv,
			   bool* done,
			   bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_vals(start_after, filter_prefix,
						     max_return, ec, kv, done);
}

void ReadOp::get_omap_vals_by_keys(
  const bc::flat_set<std::string>& keys,
  bc::flat_map<std::string, cb::list>* kv,
  bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_get_vals_by_keys(keys, ec, kv);
}

void ReadOp::list_watchers(std::vector<ObjWatcher>* watchers,
			   bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)-> op.list_watchers(watchers, ec);
}

void ReadOp::list_snaps(SnapSet* snaps,
			bs::error_code* ec) {
  reinterpret_cast<OpImpl*>(&impl)->op.list_snaps(snaps, nullptr, ec);
}

// WriteOp

void WriteOp::set_mtime(ceph::real_time t) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->mtime = t;
}

void WriteOp::create(bool exclusive) {
  reinterpret_cast<OpImpl*>(&impl)->op.create(exclusive);
}

void WriteOp::write(uint64_t off, bufferlist&& bl) {
  reinterpret_cast<OpImpl*>(&impl)->op.write(off, bl);
}

void WriteOp::write_full(bufferlist&& bl) {
  reinterpret_cast<OpImpl*>(&impl)->op.write_full(bl);
}

void WriteOp::writesame(uint64_t off, uint64_t write_len, bufferlist&& bl) {
  reinterpret_cast<OpImpl*>(&impl)->op.writesame(off, write_len, bl);
}

void WriteOp::append(bufferlist&& bl) {
  reinterpret_cast<OpImpl*>(&impl)->op.append(bl);
}

void WriteOp::remove() {
  reinterpret_cast<OpImpl*>(&impl)->op.remove();
}

void WriteOp::truncate(uint64_t off) {
  reinterpret_cast<OpImpl*>(&impl)->op.truncate(off);
}

void WriteOp::zero(uint64_t off, uint64_t len) {
  reinterpret_cast<OpImpl*>(&impl)->op.zero(off, len);
}

void WriteOp::rmxattr(std::string_view name) {
  reinterpret_cast<OpImpl*>(&impl)->op.rmxattr(name);
}

void WriteOp::setxattr(std::string_view name,
                       bufferlist&& bl) {
  reinterpret_cast<OpImpl*>(&impl)->op.setxattr(name, bl);
}

void WriteOp::rollback(uint64_t snapid) {
  reinterpret_cast<OpImpl*>(&impl)->op.rollback(snapid);
}

void WriteOp::set_omap(
  const bc::flat_map<std::string, cb::list>& map) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_set(map);
}

void WriteOp::set_omap_header(bufferlist&& bl) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_set_header(bl);
}

void WriteOp::clear_omap() {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_clear();
}

void WriteOp::rm_omap_keys(
  const bc::flat_set<std::string>& to_rm) {
  reinterpret_cast<OpImpl*>(&impl)->op.omap_rm_keys(to_rm);
}

void WriteOp::set_alloc_hint(uint64_t expected_object_size,
			     uint64_t expected_write_size,
			     alloc_hint::alloc_hint_t flags) {
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
}

// RADOS

RADOS::Builder& RADOS::Builder::add_conf_file(std::string_view f) {
  if (conf_files)
    *conf_files += (", " + std::string(f));
  else
    conf_files = std::string(f);
  return *this;
}

void RADOS::Builder::build(boost::asio::io_context& ioctx,
			   std::unique_ptr<BuildComp> c) {
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
      c->dispatch(std::move(c), ceph::to_error_code(r), RADOS{nullptr});
  }

  cct->_conf.parse_env(cct->get_module_type());

  for (const auto& [n, v] : configs) {
    std::stringstream ss;
    auto r = cct->_conf.set_val(n, v, &ss);
    if (r < 0)
      c->dispatch(std::move(c), ceph::to_error_code(-EINVAL), RADOS{nullptr});
  }

  if (!no_mon_conf) {
    MonClient mc_bootstrap(cct, ioctx);
    // TODO This function should return an error code.
    auto err = mc_bootstrap.get_monmap_and_config();
    if (err < 0)
      c->dispatch(std::move(c), ceph::to_error_code(err), RADOS{nullptr});
  }
  if (!cct->_log->is_started()) {
    cct->_log->start();
  }
  common_init_finish(cct);

  RADOS::make_with_cct(cct, ioctx, std::move(c));
}

void RADOS::make_with_cct(CephContext* cct,
			  boost::asio::io_context& ioctx,
			  std::unique_ptr<BuildComp> c) {
  try {
    auto r = new detail::NeoClient{std::make_unique<detail::RADOS>(ioctx, cct)};
    r->objecter->wait_for_osd_map(
      [c = std::move(c), r = std::unique_ptr<detail::Client>(r)]() mutable {
	c->dispatch(std::move(c), bs::error_code{},
		    RADOS{std::move(r)});
      });
  } catch (const bs::system_error& err) {
    c->dispatch(std::move(c), err.code(), RADOS{nullptr});
  }
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

boost::asio::io_context& RADOS::get_io_context() {
  return impl->ioctx;
}

void RADOS::execute(const Object& o, const IOContext& _ioc, ReadOp&& _op,
		    cb::list* bl,
		    std::unique_ptr<ReadOp::Completion> c, version_t* objver) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = 0; // Should be in Op.

  impl->objecter->read(
    *oid, ioc->oloc, std::move(op->op), ioc->snap_seq, bl, flags,
    std::move(c), objver);
}

void RADOS::execute(const Object& o, const IOContext& _ioc, WriteOp&& _op,
		    std::unique_ptr<WriteOp::Completion> c, version_t* objver) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = 0; // Should be in Op.
  ceph::real_time mtime;
  if (op->mtime)
    mtime = *op->mtime;
  else
    mtime = ceph::real_clock::now();

  impl->objecter->mutate(
    *oid, ioc->oloc, std::move(op->op), ioc->snapc,
    mtime, flags,
    std::move(c), objver);
}

void RADOS::execute(const Object& o, std::int64_t pool, ReadOp&& _op,
		    cb::list* bl,
		    std::unique_ptr<ReadOp::Completion> c,
		    std::optional<std::string_view> ns,
		    std::optional<std::string_view> key,
		    version_t* objver) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = 0; // Should be in Op.
  object_locator_t oloc;
  oloc.pool = pool;
  if (ns)
    oloc.nspace = *ns;
  if (key)
    oloc.key = *key;

  impl->objecter->read(
    *oid, oloc, std::move(op->op), CEPH_NOSNAP, bl, flags,
    std::move(c), objver);
}

void RADOS::execute(const Object& o, std::int64_t pool, WriteOp&& _op,
		    std::unique_ptr<WriteOp::Completion> c,
		    std::optional<std::string_view> ns,
		    std::optional<std::string_view> key,
		    version_t* objver) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = 0; // Should be in Op.
  object_locator_t oloc;
  oloc.pool = pool;
  if (ns)
    oloc.nspace = *ns;
  if (key)
    oloc.key = *key;

  ceph::real_time mtime;
  if (op->mtime)
    mtime = *op->mtime;
  else
    mtime = ceph::real_clock::now();

  impl->objecter->mutate(
    *oid, oloc, std::move(op->op), {},
    mtime, flags,
    std::move(c), objver);
}

boost::uuids::uuid RADOS::get_fsid() const noexcept {
  return impl->monclient.get_fsid().uuid;
}


void RADOS::lookup_pool(std::string_view name,
			std::unique_ptr<LookupPoolComp> c)
{
  // I kind of want to make lookup_pg_pool return
  // std::optional<int64_t> since it can only return one error code.
  int64_t ret = impl->objecter->with_osdmap(
    std::mem_fn(&OSDMap::lookup_pg_pool_name),
    name);
  if (ret < 0) {
    impl->objecter->wait_for_latest_osdmap(
      [name = std::string(name), c = std::move(c),
       objecter = impl->objecter]
      (bs::error_code ec) mutable {
	int64_t ret =
	  objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
				name);
	if (ret < 0)
	  ca::dispatch(std::move(c), osdc_errc::pool_dne,
		       std::int64_t(0));
	else
	  ca::dispatch(std::move(c), bs::error_code{}, ret);
      });
  } else if (ret < 0) {
    ca::dispatch(std::move(c), osdc_errc::pool_dne,
		 std::int64_t(0));
  } else {
    ca::dispatch(std::move(c), bs::error_code{}, ret);
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

void RADOS::list_pools(std::unique_ptr<LSPoolsComp> c) {
  impl->objecter->with_osdmap(
    [&](OSDMap& o) {
      std::vector<std::pair<std::int64_t, std::string>> v;
      for (auto p : o.get_pools())
	v.push_back(std::make_pair(p.first, o.get_pool_name(p.first)));
      ca::dispatch(std::move(c), std::move(v));
    });
}

void RADOS::create_pool_snap(std::int64_t pool,
			     std::string_view snapName,
			     std::unique_ptr<SimpleOpComp> c)
{
  impl->objecter->create_pool_snap(
    pool, snapName,
    Objecter::PoolOp::OpComp::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	ca::dispatch(std::move(c), e);
      }));
}

void RADOS::allocate_selfmanaged_snap(int64_t pool,
				      std::unique_ptr<SMSnapComp> c) {
  impl->objecter->allocate_selfmanaged_snap(
    pool,
    ca::Completion<void(bs::error_code, snapid_t)>::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, snapid_t snap) mutable {
	ca::dispatch(std::move(c), e, snap);
      }));
}

void RADOS::delete_pool_snap(std::int64_t pool,
			     std::string_view snapName,
			     std::unique_ptr<SimpleOpComp> c)
{
  impl->objecter->delete_pool_snap(
    pool, snapName,
    Objecter::PoolOp::OpComp::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	ca::dispatch(std::move(c), e);
      }));
}

void RADOS::delete_selfmanaged_snap(std::int64_t pool,
				    std::uint64_t snap,
				    std::unique_ptr<SimpleOpComp> c)
{
  impl->objecter->delete_selfmanaged_snap(
    pool, snap,
    Objecter::PoolOp::OpComp::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	ca::dispatch(std::move(c), e);
      }));
}

void RADOS::create_pool(std::string_view name,
			std::optional<int> crush_rule,
			std::unique_ptr<SimpleOpComp> c)
{
  impl->objecter->create_pool(
    name,
    Objecter::PoolOp::OpComp::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	ca::dispatch(std::move(c), e);
      }),
      crush_rule.value_or(-1));
}

void RADOS::delete_pool(std::string_view name,
			std::unique_ptr<SimpleOpComp> c)
{
  impl->objecter->delete_pool(
    name,
    Objecter::PoolOp::OpComp::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	ca::dispatch(std::move(c), e);
      }));
}

void RADOS::delete_pool(std::int64_t pool,
			std::unique_ptr<SimpleOpComp> c)
{
  impl->objecter->delete_pool(
    pool,
    Objecter::PoolOp::OpComp::create(
      get_executor(),
      [c = std::move(c)](bs::error_code e, const bufferlist&) mutable {
	ca::dispatch(std::move(c), e);
      }));
}

void RADOS::stat_pools(const std::vector<std::string>& pools,
		       std::unique_ptr<PoolStatComp> c) {
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

      ca::dispatch(std::move(c), ec, std::move(result), per_pool);
    });
}

void RADOS::stat_fs(std::optional<std::int64_t> _pool,
		    std::unique_ptr<StatFSComp> c) {
  boost::optional<int64_t> pool;
  if (_pool)
    pool = *pool;
  impl->objecter->get_fs_stats(
    pool,
    [c = std::move(c)](bs::error_code ec, const struct ceph_statfs s) mutable {
      FSStats fso{s.kb, s.kb_used, s.kb_avail, s.num_objects};
      c->dispatch(std::move(c), ec, std::move(fso));
    });
}

// --- Watch/Notify

void RADOS::watch(const Object& o, const IOContext& _ioc,
		  std::optional<std::chrono::seconds> timeout, WatchCB&& cb,
		  std::unique_ptr<WatchComp> c) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  ObjectOperation op;

  auto linger_op = impl->objecter->linger_register(*oid, ioc->oloc, 0);
  uint64_t cookie = linger_op->get_cookie();
  linger_op->handle = std::move(cb);
  op.watch(cookie, CEPH_OSD_WATCH_OP_WATCH, timeout.value_or(0s).count());
  bufferlist bl;
  impl->objecter->linger_watch(
    linger_op, op, ioc->snapc, ceph::real_clock::now(), bl,
    Objecter::LingerOp::OpComp::create(
      get_executor(),
      [c = std::move(c), cookie](bs::error_code e, cb::list) mutable {
	ca::dispatch(std::move(c), e, cookie);
      }), nullptr);
}

void RADOS::watch(const Object& o, std::int64_t pool,
		  std::optional<std::chrono::seconds> timeout, WatchCB&& cb,
		  std::unique_ptr<WatchComp> c,
		  std::optional<std::string_view> ns,
		  std::optional<std::string_view> key) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  object_locator_t oloc;
  oloc.pool = pool;
  if (ns)
    oloc.nspace = *ns;
  if (key)
    oloc.key = *key;

  ObjectOperation op;

  Objecter::LingerOp *linger_op = impl->objecter->linger_register(*oid, oloc, 0);
  uint64_t cookie = linger_op->get_cookie();
  linger_op->handle = std::move(cb);
  op.watch(cookie, CEPH_OSD_WATCH_OP_WATCH, timeout.value_or(0s).count());
  bufferlist bl;
  impl->objecter->linger_watch(
    linger_op, op, {}, ceph::real_clock::now(), bl,
    Objecter::LingerOp::OpComp::create(
      get_executor(),
      [c = std::move(c), cookie](bs::error_code e, bufferlist) mutable {
	ca::dispatch(std::move(c), e, cookie);
      }), nullptr);
}

void RADOS::notify_ack(const Object& o,
		       const IOContext& _ioc,
		       uint64_t notify_id,
		       uint64_t cookie,
		       bufferlist&& bl,
		       std::unique_ptr<SimpleOpComp> c)
{
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  ObjectOperation op;
  op.notify_ack(notify_id, cookie, bl);

  impl->objecter->read(*oid, ioc->oloc, std::move(op), ioc->snap_seq,
		       nullptr, 0, std::move(c));
}

void RADOS::notify_ack(const Object& o,
		       std::int64_t pool,
		       uint64_t notify_id,
		       uint64_t cookie,
		       bufferlist&& bl,
		       std::unique_ptr<SimpleOpComp> c,
		       std::optional<std::string_view> ns,
		       std::optional<std::string_view> key) {
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  object_locator_t oloc;
  oloc.pool = pool;
  if (ns)
    oloc.nspace = *ns;
  if (key)
    oloc.key = *key;

  ObjectOperation op;
  op.notify_ack(notify_id, cookie, bl);
  impl->objecter->read(*oid, oloc, std::move(op), CEPH_NOSNAP, nullptr, 0,
		       std::move(c));
}

tl::expected<ceph::timespan, bs::error_code> RADOS::watch_check(uint64_t cookie)
{
  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  return impl->objecter->linger_check(linger_op);
}

void RADOS::unwatch(uint64_t cookie, const IOContext& _ioc,
		    std::unique_ptr<SimpleOpComp> c)
{
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);

  ObjectOperation op;
  op.watch(cookie, CEPH_OSD_WATCH_OP_UNWATCH);
  impl->objecter->mutate(linger_op->target.base_oid, ioc->oloc, std::move(op),
			 ioc->snapc, ceph::real_clock::now(), 0,
			 Objecter::Op::OpComp::create(
			   get_executor(),
			   [objecter = impl->objecter,
			    linger_op, c = std::move(c)]
			   (bs::error_code ec) mutable {
			     objecter->linger_cancel(linger_op);
			     ca::dispatch(std::move(c), ec);
			   }));
}

void RADOS::unwatch(uint64_t cookie, std::int64_t pool,
		    std::unique_ptr<SimpleOpComp> c,
		    std::optional<std::string_view> ns,
		    std::optional<std::string_view> key)
{
  object_locator_t oloc;
  oloc.pool = pool;
  if (ns)
    oloc.nspace = *ns;
  if (key)
    oloc.key = *key;

  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);

  ObjectOperation op;
  op.watch(cookie, CEPH_OSD_WATCH_OP_UNWATCH);
  impl->objecter->mutate(linger_op->target.base_oid, oloc, std::move(op),
			 {}, ceph::real_clock::now(), 0,
			 Objecter::Op::OpComp::create(
			   get_executor(),
			   [objecter = impl->objecter,
			    linger_op, c = std::move(c)]
			   (bs::error_code ec) mutable {
			     objecter->linger_cancel(linger_op);
			     ca::dispatch(std::move(c), ec);
			   }));
}

void RADOS::flush_watch(std::unique_ptr<VoidOpComp> c)
{
  impl->objecter->linger_callback_flush([c = std::move(c)]() mutable {
					  ca::post(std::move(c));
					});
}

struct NotifyHandler : std::enable_shared_from_this<NotifyHandler> {
  boost::asio::io_context& ioc;
  boost::asio::io_context::strand strand;
  Objecter* objecter;
  Objecter::LingerOp* op;
  std::unique_ptr<RADOS::NotifyComp> c;

  bool acked = false;
  bool finished = false;
  bs::error_code res;
  bufferlist rbl;

  NotifyHandler(boost::asio::io_context& ioc,
		Objecter* objecter,
		Objecter::LingerOp* op,
		std::unique_ptr<RADOS::NotifyComp> c)
    : ioc(ioc), strand(ioc), objecter(objecter), op(op), c(std::move(c)) {}

  // Use bind or a lambda to pass this in.
  void handle_ack(bs::error_code ec,
		  bufferlist&&) {
    boost::asio::post(
      strand,
      [this, ec, p = shared_from_this()]() mutable {
	acked = true;
	maybe_cleanup(ec);
      });
  }

  // Notify finish callback. It can actually own the object's storage.

  void operator()(bs::error_code ec,
		  bufferlist&& bl) {
    boost::asio::post(
      strand,
      [this, ec, p = shared_from_this()]() mutable {
	finished = true;
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
      ca::dispatch(std::move(c), res, std::move(rbl));
    }
  }
};

void RADOS::notify(const Object& o, const IOContext& _ioc, bufferlist&& bl,
		   std::optional<std::chrono::milliseconds> timeout,
		   std::unique_ptr<NotifyComp> c)
{
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto linger_op = impl->objecter->linger_register(*oid, ioc->oloc, 0);

  auto cb = std::make_shared<NotifyHandler>(impl->ioctx, impl->objecter,
                                            linger_op, std::move(c));
  linger_op->on_notify_finish =
    Objecter::LingerOp::OpComp::create(
      get_executor(),
      [cb](bs::error_code ec, ceph::bufferlist bl) mutable {
	(*cb)(ec, std::move(bl));
      });
  ObjectOperation rd;
  bufferlist inbl;
  rd.notify(
    linger_op->get_cookie(), 1,
    timeout ? timeout->count() : impl->cct->_conf->client_notify_timeout,
    bl, &inbl);

  impl->objecter->linger_notify(
    linger_op, rd, ioc->snap_seq, inbl,
    Objecter::LingerOp::OpComp::create(
      get_executor(),
      [cb](bs::error_code ec, ceph::bufferlist bl) mutable {
	cb->handle_ack(ec, std::move(bl));
      }), nullptr);
}

void RADOS::notify(const Object& o, std::int64_t pool, bufferlist&& bl,
		   std::optional<std::chrono::milliseconds> timeout,
		   std::unique_ptr<NotifyComp> c,
		   std::optional<std::string_view> ns,
		   std::optional<std::string_view> key)
{
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  object_locator_t oloc;
  oloc.pool = pool;
  if (ns)
    oloc.nspace = *ns;
  if (key)
    oloc.key = *key;
  auto linger_op = impl->objecter->linger_register(*oid, oloc, 0);

  auto cb = std::make_shared<NotifyHandler>(impl->ioctx, impl->objecter,
                                            linger_op, std::move(c));
  linger_op->on_notify_finish =
    Objecter::LingerOp::OpComp::create(
      get_executor(),
      [cb](bs::error_code ec, ceph::bufferlist&& bl) mutable {
	(*cb)(ec, std::move(bl));
      });
  ObjectOperation rd;
  bufferlist inbl;
  rd.notify(
    linger_op->get_cookie(), 1,
    timeout ? timeout->count() : impl->cct->_conf->client_notify_timeout,
    bl, &inbl);

  impl->objecter->linger_notify(
    linger_op, rd, CEPH_NOSNAP, inbl,
    Objecter::LingerOp::OpComp::create(
      get_executor(),
      [cb](bs::error_code ec, bufferlist&& bl) mutable {
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

void RADOS::enumerate_objects(const IOContext& _ioc,
			      const Cursor& begin,
			      const Cursor& end,
			      const std::uint32_t max,
			      const bufferlist& filter,
			      std::unique_ptr<EnumerateComp> c) {
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
      ca::dispatch(std::move(c), ec, std::move(v),
		   Cursor(static_cast<void*>(&n)));
    });
}

void RADOS::enumerate_objects(std::int64_t pool,
			      const Cursor& begin,
			      const Cursor& end,
			      const std::uint32_t max,
			      const bufferlist& filter,
			      std::unique_ptr<EnumerateComp> c,
			      std::optional<std::string_view> ns,
			      std::optional<std::string_view> key) {
  impl->objecter->enumerate_objects<Entry>(
    pool,
    ns ? *ns : std::string_view{},
    *reinterpret_cast<const hobject_t*>(&begin.impl),
    *reinterpret_cast<const hobject_t*>(&end.impl),
    max,
    filter,
    [c = std::move(c)]
    (bs::error_code ec, std::vector<Entry>&& v,
     hobject_t&& n) mutable {
      ca::dispatch(std::move(c), ec, std::move(v),
		   Cursor(static_cast<void*>(&n)));
    });
}


void RADOS::osd_command(int osd, std::vector<std::string>&& cmd,
			ceph::bufferlist&& in, std::unique_ptr<CommandComp> c) {
  impl->objecter->osd_command(osd, std::move(cmd), std::move(in), nullptr,
			      [c = std::move(c)]
			      (bs::error_code ec,
			       std::string&& s,
			       ceph::bufferlist&& b) mutable {
				ca::dispatch(std::move(c), ec,
					     std::move(s),
					     std::move(b));
			      });
}
void RADOS::pg_command(PG pg, std::vector<std::string>&& cmd,
		       ceph::bufferlist&& in, std::unique_ptr<CommandComp> c) {
  impl->objecter->pg_command(pg_t{pg.seed, pg.pool}, std::move(cmd), std::move(in), nullptr,
			     [c = std::move(c)]
			     (bs::error_code ec,
			      std::string&& s,
			      ceph::bufferlist&& b) mutable {
			       ca::dispatch(std::move(c), ec,
					    std::move(s),
					    std::move(b));
			     });
}

void RADOS::enable_application(std::string_view pool, std::string_view app_name,
			       bool force, std::unique_ptr<SimpleOpComp> c) {
  // pre-Luminous clusters will return -EINVAL and application won't be
  // preserved until Luminous is configured as minimum version.
  if (!impl->get_required_monitor_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    ca::dispatch(std::move(c), ceph::to_error_code(-EOPNOTSUPP));
  } else {
    impl->monclient.start_mon_command(
      { fmt::format("{{ \"prefix\": \"osd pool application enable\","
		    "\"pool\": \"{}\", \"app\": \"{}\"{}}}",
		    pool, app_name,
		    force ? " ,\"yes_i_really_mean_it\": true" : "")},
      {}, [c = std::move(c)](bs::error_code e,
			     std::string, cb::list) mutable {
	    ca::post(std::move(c), e);
	  });
  }
}

void RADOS::mon_command(std::vector<std::string> command,
			const cb::list& bl,
			std::string* outs, cb::list* outbl,
			std::unique_ptr<SimpleOpComp> c) {

  impl->monclient.start_mon_command(
    command, bl,
    [c = std::move(c), outs, outbl](bs::error_code e,
				    std::string s, cb::list bl) mutable {
      if (outs)
	*outs = std::move(s);
      if (outbl)
	*outbl = std::move(bl);
      ca::post(std::move(c), e);
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

  return default_error_condition(ev) == c;
}

int category::from_code(int ev) const noexcept {
  switch (static_cast<errc>(ev)) {
  case errc::pool_dne:
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
