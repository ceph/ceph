// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/neorados/RADOS.hpp"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/hobject.h"
#include "librados/AioCompletionImpl.h"
#include "mon/error_code.h"
#include "osd/error_code.h"
#include "osd/osd_types.h"
#include "osdc/error_code.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "test/librados_test_stub/TestClassHandler.h"
#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "test/librados_test_stub/TestRadosClient.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <functional>

#include <boost/asio/append.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/system_error.hpp>

namespace bs = boost::system;
namespace asio = boost::asio;
using namespace std::literals;
using namespace std::placeholders;

namespace neorados {
namespace detail {

class Client {
public:
  ceph::mutex mutex = ceph::make_mutex("NeoradosTestStub::Client");

  librados::TestRadosClient* test_rados_client;
  boost::asio::io_context& io_context;

  std::map<std::pair<int64_t, std::string>, librados::TestIoCtxImpl*> io_ctxs;

  Client(librados::TestRadosClient* test_rados_client)
    : test_rados_client(test_rados_client),
      io_context(test_rados_client->get_io_context()) {
  }

  ~Client() {
    for (auto& io_ctx : io_ctxs) {
      io_ctx.second->put();
    }
  }

  librados::TestIoCtxImpl* get_io_ctx(const IOContext& ioc) {
    int64_t pool_id = ioc.get_pool();
    std::string ns = std::string{ioc.get_ns()};

    auto lock = std::scoped_lock{mutex};
    auto key = make_pair(pool_id, ns);
    auto it = io_ctxs.find(key);
    if (it != io_ctxs.end()) {
      return it->second;
    }

    std::list<std::pair<int64_t, std::string>> pools;
    int r = test_rados_client->pool_list(pools);
    if (r < 0) {
      return nullptr;
    }

    for (auto& pool : pools) {
      if (pool.first == pool_id) {
        auto io_ctx = test_rados_client->create_ioctx(pool_id, pool.second);
        io_ctx->set_namespace(ns);
        io_ctxs[key] = io_ctx;
        return io_ctx;
      }
    }
    return nullptr;
  }
};

} // namespace detail

namespace {

struct CompletionPayload {
  Op::Completion c;
};

void completion_callback_adapter(rados_completion_t c, void *arg) {
  auto impl = reinterpret_cast<librados::AioCompletionImpl *>(c);
  auto r = impl->get_return_value();
  impl->release();

  auto payload = reinterpret_cast<CompletionPayload*>(arg);
  asio::dispatch(asio::append(std::move(payload->c),
			      (r < 0) ? bs::error_code(-r, osd_category()) :
			      bs::error_code()));
  delete payload;
}

librados::AioCompletionImpl* create_aio_completion(
  Op::Completion&& c) {
  auto payload = new CompletionPayload{std::move(c)};

  auto impl = new librados::AioCompletionImpl();
  impl->set_complete_callback(payload, completion_callback_adapter);

  return impl;
}

int save_operation_size(int result, uint64_t* pval) {
  int our_r = result;
  if (result <= -MAX_ERRNO) {
    if (pval != NULL) {
      *pval = -MAX_ERRNO - result;
    }
    our_r = -MAX_ERRNO;
  } else {
    if (pval != NULL) {
      *pval = -1;
    }
  }
  return our_r;
}

int save_operation_ec(int result, boost::system::error_code* ec) {
  if (ec != NULL) {
    *ec = {std::abs(result), bs::system_category()};
  }
  return result;
}

} // anonymous namespace

Object::Object() {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t();
}

Object::Object(std::string&& s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(std::move(s));
}

Object::Object(const Object& rhs) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(*reinterpret_cast<const object_t*>(&rhs.impl));
}

Object::Object(Object&& rhs) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(*std::move(reinterpret_cast<object_t*>(&rhs.impl)));
}

Object::~Object() {
  reinterpret_cast<object_t*>(&impl)->~object_t();
}

Object::operator std::string_view() const {
  return std::string_view(reinterpret_cast<const object_t*>(&impl)->name);
}

struct IOContextImpl {
  object_locator_t oloc;
  snapid_t snap_seq = CEPH_NOSNAP;
  SnapContext snapc;
};

IOContext::IOContext() {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl();
}

IOContext::IOContext(const IOContext& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(*reinterpret_cast<const IOContextImpl*>(&rhs.impl));
}

IOContext::IOContext(IOContext&& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(std::move(*reinterpret_cast<const IOContextImpl*>(&rhs.impl)));
}

IOContext::IOContext(int64_t pool, std::string ns, std::string key)
  : IOContext() {
  set_pool(pool);
  set_ns(std::move(ns));
  set_key(std::move(key));
}

IOContext::~IOContext() {
  reinterpret_cast<IOContextImpl*>(&impl)->~IOContextImpl();
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
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.key = std::move(key);
}

IOContext&& IOContext::set_key(std::string key) && {
  set_key(std::move(key));
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


std::optional<
  std::pair<std::uint64_t,
            std::vector<std::uint64_t>>> IOContext::get_write_snap_context() const {
  auto& snapc = reinterpret_cast<const IOContextImpl*>(&impl)->snapc;
  if (snapc.empty()) {
    return std::nullopt;
  } else {
    std::vector<uint64_t> v(snapc.snaps.begin(), snapc.snaps.end());
    return std::make_optional(std::make_pair(uint64_t(snapc.seq), v));
  }
}

void IOContext::set_write_snap_context(
  std::optional<std::pair<std::uint64_t, std::vector<std::uint64_t>>> _snapc) & {
  auto& snapc = reinterpret_cast<IOContextImpl*>(&impl)->snapc;
  if (!_snapc) {
    snapc.clear();
  } else {
    SnapContext n(_snapc->first, { _snapc->second.begin(), _snapc->second.end()});
    if (!n.is_valid()) {
      throw bs::system_error(EINVAL,
                             bs::system_category(),
                             "Invalid snap context.");
    }

    snapc = n;
  }
}

void IOContext::set_full_try(bool _full_try) & {
  // no-op
}

bool operator ==(const IOContext& lhs, const IOContext& rhs) {
  auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);
  return (l->oloc == r->oloc &&
          l->snap_seq == r->snap_seq &&
          l->snapc.seq == r->snapc.seq &&
          l->snapc.snaps == r->snapc.snaps);
}

bool operator !=(const IOContext& lhs, const IOContext& rhs) {
  return !(lhs == rhs);
}

Op::Op() {
  static_assert(Op::impl_size >= sizeof(librados::TestObjectOperationImpl*));
  auto& o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o = new librados::TestObjectOperationImpl();
  o->get();
}

Op::Op(Op&& rhs) {
  static_assert(Op::impl_size >= sizeof(librados::TestObjectOperationImpl*));
  auto& o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  auto& p = *reinterpret_cast<librados::TestObjectOperationImpl**>(&rhs.impl);
  o = p;
  p = new librados::TestObjectOperationImpl();
  p->get();
}

Op::~Op() {
  auto& o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  if (o != nullptr) {
    o->put();
    o = nullptr;
  }
}

void Op::assert_exists() {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::assert_exists, _1, _2, _4));
}

void Op::assert_version(uint64_t ver) {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
          &librados::TestIoCtxImpl::assert_version, _1, _2, ver));
}

void Op::cmpext(uint64_t off, ceph::buffer::list cmp_bl, uint64_t* s) {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  librados::ObjectOperationTestImpl op = std::bind(
    &librados::TestIoCtxImpl::cmpext, _1, _2, off, cmp_bl, _4);
  if (s != nullptr) {
    op = std::bind(
      save_operation_size, std::bind(op, _1, _2, _3, _4, _5, _6), s);
  }
  o->ops.push_back(op);
}

std::size_t Op::size() const {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl* const *>(&impl);
  return o->ops.size();
}

void Op::set_fadvise_random() {
  // no-op
}

void Op::set_fadvise_sequential() {
  // no-op
}

void Op::set_fadvise_willneed() {
  // no-op
}

void Op::set_fadvise_dontneed() {
  // no-op
}

void Op::set_fadvise_nocache() {
  // no-op
}

void Op::balance_reads() {
  // no-op
}

void Op::localize_reads() {
  // no-op
}

void Op::exec(std::string_view cls, std::string_view method,
              const ceph::buffer::list& inbl,
              ceph::buffer::list* out,
              boost::system::error_code* ec) {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);

  auto cls_handler = librados_test_stub::get_class_handler();
  librados::ObjectOperationTestImpl op =
    [cls_handler, cls, method, inbl = const_cast<bufferlist&>(inbl), out]
    (librados::TestIoCtxImpl* io_ctx, const std::string& oid, bufferlist* outbl,
     uint64_t snap_id, const SnapContext& snapc, uint64_t*) mutable -> int {
      return io_ctx->exec(
        oid, cls_handler, std::string(cls).c_str(),
        std::string(method).c_str(), inbl,
        (out != nullptr ? out : outbl), snap_id, snapc);
    };
  if (ec != nullptr) {
    op = std::bind(
      save_operation_ec, std::bind(op, _1, _2, _3, _4, _5, _6), ec);
  }
  o->ops.push_back(op);
}

void Op::exec(std::string_view cls, std::string_view method,
              const ceph::buffer::list& inbl,
              boost::system::error_code* ec) {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);

  auto cls_handler = librados_test_stub::get_class_handler();
  librados::ObjectOperationTestImpl op =
    [cls_handler, cls, method, inbl = const_cast<bufferlist&>(inbl)]
    (librados::TestIoCtxImpl* io_ctx, const std::string& oid, bufferlist* outbl,
     uint64_t snap_id, const SnapContext& snapc, uint64_t*) mutable -> int {
      return io_ctx->exec(
        oid, cls_handler, std::string(cls).c_str(),
        std::string(method).c_str(), inbl, outbl, snap_id, snapc);
    };
  if (ec != NULL) {
    op = std::bind(
      save_operation_ec, std::bind(op, _1, _2, _3, _4, _5, _6), ec);
  }
  o->ops.push_back(op);
}

ReadOp& ReadOp::read(size_t off, uint64_t len, ceph::buffer::list* out,
		     boost::system::error_code* ec) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  librados::ObjectOperationTestImpl op;
  if (out != nullptr) {
    op = std::bind(
            &librados::TestIoCtxImpl::read, _1, _2, len, off, out, _4, _6);
  } else {
    op = std::bind(
            &librados::TestIoCtxImpl::read, _1, _2, len, off, _3, _4, _6);
  }

  if (ec != NULL) {
    op = std::bind(
      save_operation_ec, std::bind(op, _1, _2, _3, _4, _5, _6), ec);
  }
  o->ops.push_back(op);
  return *this;
}

ReadOp& ReadOp::sparse_read(uint64_t off, uint64_t len,
			    ceph::buffer::list* out,
			    std::vector<std::pair<std::uint64_t,
                                                  std::uint64_t>>* extents,
			    boost::system::error_code* ec) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  librados::ObjectOperationTestImpl op =
    [off, len, out, extents]
    (librados::TestIoCtxImpl* io_ctx, const std::string& oid, bufferlist* outbl,
     uint64_t snap_id, const SnapContext& snapc, uint64_t*) mutable -> int {
      std::map<uint64_t,uint64_t> m;
      int r = io_ctx->sparse_read(
        oid, off, len, &m, (out != nullptr ? out : outbl), snap_id);
      if (r >= 0 && extents != nullptr) {
        extents->clear();
        extents->insert(extents->end(), m.begin(), m.end());
      }
      return r;
    };
  if (ec != NULL) {
    op = std::bind(save_operation_ec,
                     std::bind(op, _1, _2, _3, _4, _5, _6), ec);
  }
  o->ops.push_back(op);
  return *this;
}

ReadOp& ReadOp::list_snaps(SnapSet* snaps, bs::error_code* ec) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  librados::ObjectOperationTestImpl op =
    [snaps]
    (librados::TestIoCtxImpl* io_ctx, const std::string& oid, bufferlist*,
     uint64_t, const SnapContext&, uint64_t*) mutable -> int {
      librados::snap_set_t snap_set;
      int r = io_ctx->list_snaps(oid, &snap_set);
      if (r >= 0 && snaps != nullptr) {
        *snaps = {};
        snaps->seq = snap_set.seq;
        snaps->clones.reserve(snap_set.clones.size());
        for (auto& clone : snap_set.clones) {
          neorados::CloneInfo clone_info;
          clone_info.cloneid = clone.cloneid;
          clone_info.snaps = clone.snaps;
          clone_info.overlap = clone.overlap;
          clone_info.size = clone.size;
          snaps->clones.push_back(clone_info);
        }
      }
      return r;
    };
  if (ec != NULL) {
    op = std::bind(save_operation_ec,
                   std::bind(op, _1, _2, _3, _4, _5, _6), ec);
  }
  o->ops.push_back(op);
  return *this;
}

WriteOp& WriteOp::create(bool exclusive) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::create, _1, _2, exclusive, _5));
  return *this;
}

WriteOp& WriteOp::write(uint64_t off, ceph::buffer::list bl) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::write, _1, _2, bl, bl.length(), off, _5));
  return *this;
}

WriteOp& WriteOp::write_full(ceph::buffer::list bl) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::write_full, _1, _2, bl, _5));
  return *this;
}

WriteOp& WriteOp::remove() & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::remove, _1, _2, _5));
  return *this;
}

WriteOp& WriteOp::truncate(uint64_t off) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::truncate, _1, _2, off, _5));
  return *this;
}

WriteOp& WriteOp::zero(uint64_t off, uint64_t len) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::zero, _1, _2, off, len, _5));
  return *this;
}

WriteOp& WriteOp::writesame(std::uint64_t off, std::uint64_t write_len,
			    ceph::buffer::list bl) & {
  auto o = *reinterpret_cast<librados::TestObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::TestIoCtxImpl::writesame, _1, _2, bl, write_len, off, _5));
  return *this;
}

WriteOp& WriteOp::set_alloc_hint(uint64_t expected_object_size,
				 uint64_t expected_write_size,
				 alloc_hint::alloc_hint_t flags) & {
  // no-op
  return *this;
}

RADOS::RADOS() = default;

RADOS::RADOS(RADOS&&) = default;

RADOS::RADOS(std::unique_ptr<detail::Client> impl)
  : impl(std::move(impl)) {
}

RADOS::~RADOS() = default;

RADOS RADOS::make_with_librados(librados::Rados& rados) {
  auto test_rados_client = reinterpret_cast<librados::TestRadosClient*>(
    rados.client);
  return RADOS{std::make_unique<detail::Client>(test_rados_client)};
}

CephContext* neorados::RADOS::cct() {
  return impl->test_rados_client->cct();
}

boost::asio::io_context& neorados::RADOS::get_io_context() {
  return impl->io_context;
}

boost::asio::io_context::executor_type neorados::RADOS::get_executor() const {
  return impl->io_context.get_executor();
}

void RADOS::execute_(Object o, IOContext ioc, ReadOp op,
		     ceph::buffer::list* bl, Op::Completion c,
		     uint64_t* objver, const blkin_trace_info* trace_info) {
  auto io_ctx = impl->get_io_ctx(ioc);
  if (io_ctx == nullptr) {
    asio::dispatch(asio::append(std::move(c), osdc_errc::pool_dne));
    return;
  }

  auto ops = *reinterpret_cast<librados::TestObjectOperationImpl**>(&op.impl);

  auto snap_id = ioc.get_read_snap();

  auto completion = create_aio_completion(std::move(c));
  auto r = io_ctx->aio_operate_read(std::string{o}, *ops, completion, 0U, bl,
                                    snap_id, objver);
  ceph_assert(r == 0);
}

void RADOS::execute_(Object o, IOContext ioc, WriteOp op,
		     Op::Completion c, uint64_t* objver,
		     const blkin_trace_info* trace_info) {
  auto io_ctx = impl->get_io_ctx(ioc);
  if (io_ctx == nullptr) {
    asio::dispatch(asio::append(std::move(c), osdc_errc::pool_dne));
    return;
  }

  auto ops = *reinterpret_cast<librados::TestObjectOperationImpl**>(&op.impl);

  SnapContext snapc;
  auto opt_snapc = ioc.get_write_snap_context();
  if (opt_snapc) {
    snapc.seq = opt_snapc->first;
    snapc.snaps.assign(opt_snapc->second.begin(), opt_snapc->second.end());
  }

  auto completion = create_aio_completion(std::move(c));
  auto r = io_ctx->aio_operate(std::string{o}, *ops, completion, &snapc, nullptr, 0U);
  ceph_assert(r == 0);
}

void RADOS::mon_command_(std::vector<std::string> command,
			 bufferlist bl,
			 std::string* outs, bufferlist* outbl,
			 Op::Completion c) {
  auto r = impl->test_rados_client->mon_command(command, bl, outbl, outs);
  asio::post(get_executor(),
	     asio::append(std::move(c),
			  (r < 0 ? bs::error_code(-r, osd_category()) :
			   bs::error_code())));
}

void RADOS::blocklist_add_(std::string client_address,
			   std::optional<std::chrono::seconds> expire,
			   SimpleOpComp c) {
  auto r = impl->test_rados_client->blocklist_add(
    std::string(client_address), expire.value_or(0s).count());
  asio::post(get_executor(),
	     asio::append(std::move(c),
			  (r < 0 ? bs::error_code(-r, mon_category()) :
			   bs::error_code())));
}

void RADOS::wait_for_latest_osd_map_(Op::Completion c) {
  auto r = impl->test_rados_client->wait_for_latest_osd_map();
  asio::dispatch(asio::append(std::move(c),
			      (r < 0 ? bs::error_code(-r, osd_category()) :
			       bs::error_code())));
}

} // namespace neorados

namespace librados {

MockTestMemIoCtxImpl& get_mock_io_ctx(neorados::RADOS& rados,
                                      neorados::IOContext& io_context) {
  auto& impl = *reinterpret_cast<std::unique_ptr<neorados::detail::Client>*>(
    &rados);
  auto io_ctx = impl->get_io_ctx(io_context);
  ceph_assert(io_ctx != nullptr);
  return *reinterpret_cast<MockTestMemIoCtxImpl*>(io_ctx);
}

MockTestMemRadosClient& get_mock_rados_client(neorados::RADOS& rados) {
  auto& impl = *reinterpret_cast<std::unique_ptr<neorados::detail::Client>*>(
    &rados);
  return *reinterpret_cast<MockTestMemRadosClient*>(impl->test_rados_client);
}

} // namespace librados
