// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_ioc_dispatch.h"
#include "rgw_common.h"
#include "rgw_aio.h"

#include "common/async/completion.h"
#include "common/async/yield_context.h"
#include "common/ceph_mutex.h"

#include <aio.h>
#include <errno.h>
#include "common/error_code.h"
#include "common/errno.h"

using namespace ceph::immutable_obj_cache;

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

#ifndef O_BINARY
#define O_BINARY (0)
#endif /* O_BINARY */

namespace ioc {

namespace detail {
/// unique_ptr with custom deleter for struct aiocb
struct AioCBDeleter {
  void operator()(struct aiocb* c) {
        if(c->aio_fildes > 0) ::close(c->aio_fildes);
        delete c;
  }
};

using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, AioCBDeleter>;

struct AsyncFileReadOp {
  bufferlist result;
  unique_aio_cb_ptr aio_cb;
  using Signature = void(boost::system::error_code, bufferlist);
  using Completion = ceph::async::Completion<Signature, AsyncFileReadOp>;
  int init(const std::string& file_path, off_t read_ofs, off_t read_len, void* arg) {
    aio_cb.reset(new struct aiocb);
    memset(aio_cb.get(), 0, sizeof(struct aiocb));
    aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
    if(aio_cb->aio_fildes < 0) {
      int err = errno;
      dout(2) << "IOC:AsyncFileReadOp can't open " << file_path << ": " << cpp_strerror(err) << dendl;
      return -err;
    }

    bufferptr bp(read_len);
    aio_cb->aio_buf = bp.c_str();
    result.append(std::move(bp));

    aio_cb->aio_nbytes = read_len;
    aio_cb->aio_offset = read_ofs;
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = aio_dispatch;
    aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

    return 0;
  }

  static void aio_dispatch(sigval_t sigval) {
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    const int ret = -aio_error(op.aio_cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
    }

    ceph::async::dispatch(std::move(p), ec, std::move(op.result));
  }

  template <typename Executor1, typename CompletionHandler>
  static auto create(const Executor1& ex1, CompletionHandler&& handler) {
    auto p = Completion::create(ex1, std::move(handler));
    return p;
  }
};
} // namespace detail

template <typename ExecutionContext, typename CompletionToken>
auto async_read(ExecutionContext& ctx, const std::string& file_path,
                off_t read_ofs, off_t read_len, CompletionToken&& token) {
  using Op = detail::AsyncFileReadOp;
  using Signature = Op::Signature;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = Op::create(ctx.get_executor(), init.completion_handler);
  auto& op = p->user_data;
  int ret = op.init(file_path, read_ofs, read_len, p.get());

  if(0 == ret) {
    ret = aio_read(op.aio_cb.get());
  }

  if(ret < 0) {
    auto ec = boost::system::error_code{-ret, boost::system::system_category()};
    ceph::async::post(std::move(p), ec, bufferlist{});
  } else {
    p.release();
  }
  return init.result.get();
}

namespace {
  struct Handler {
  rgw::Aio* throttle = nullptr;
  rgw::AioResult& r;
  // read callback
  void operator()(boost::system::error_code ec, bufferlist bl) const {
    r.result = -ec.value();
    r.data = std::move(bl);
    throttle->put(r);
  }
};

void file_aio_read_abstract(boost::asio::io_context& context, spawn::yield_context yield,
                            std::string& file_path, off_t read_ofs, off_t read_len,
                            rgw::Aio* aio, rgw::AioResult& r) {
  using namespace boost::asio;
  async_completion<spawn::yield_context, void()> init(yield);
  auto ex = get_associated_executor(init.completion_handler);
  async_read(context, file_path, read_ofs, read_len, bind_executor(ex, Handler{aio,r}));
}
} //anonymous namespace

void file_aio_read_abstract(optional_yield y, std::string& file_path,
                           off_t read_ofs, off_t read_len,
                           rgw::Aio* aio, rgw::AioResult& r) {
  file_aio_read_abstract(y.get_io_context(), y.get_yield_context(),
                         file_path, read_ofs, read_len, aio, r);
}

}//namespace ioc

void IOChook::init(CephContext* _cct, Context* on_finish) {
  m_cct = _cct;
  ceph_assert(m_cct != nullptr);
  std::unique_lock locker{m_lock};
  if(m_cache_client == nullptr) {
    auto controller_path = m_cct->_conf.template get_val<std::string>(
      "immutable_object_cache_sock");
    m_cache_client = new CacheClient(controller_path.c_str(), m_cct);
  }
  create_cache_session(on_finish, false);
}

void IOChook::create_cache_session(Context *on_finish, bool is_reconnect) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if(m_connecting) {
    return;
  }

  m_connecting = true;

  Context* register_ctx = new LambdaContext([this, on_finish](int ret) {
    if(ret < 0) {
      ldout(m_cct, 1) << "rgw_hook: IOC hook fail to register client." << dendl;
    }
    handle_register_client(ret < 0 ? false : true);

    ceph_assert(m_connecting);
    m_connecting = false;

    if(on_finish != nullptr) {
      on_finish->complete(0);
    }
  });

  Context* connect_ctx = new LambdaContext([this, register_ctx](int ret) {
    if(ret < 0) {
      ldout(m_cct, 1)  << "rgw_hook: IOC hook fail to connect to IOC daemon" << dendl;
      register_ctx->complete(0);
      return;
    }

    ldout(m_cct, 20) << "rgw_hook: IOC hook connected to IOC daemon." << dendl;

    m_cache_client->register_client(register_ctx);
  });

  if(m_cache_client != nullptr && is_reconnect) {
    delete m_cache_client;
    auto controller_path = m_cct->_conf.template get_val<std::string>(
                                        "immutable_object_cache_sock");
    m_cache_client = new CacheClient(controller_path.c_str(), m_cct);
  }

  ceph_assert(m_cache_client != nullptr);

  m_cache_client->run();
  m_cache_client->connect(connect_ctx);
}

int IOChook::handle_register_client(bool reg) {
  ldout(m_cct, 20) << "rgw_hook: registering client" << dendl;

  if(!reg) {
    ldout(m_cct, 1) << "rgw_hook: IOC hook register fails." << dendl;
  }
  return 0;
}

void IOChook::read(std::string oid, std::string pool_ns, int64_t pool_id,
                   off_t read_ofs, off_t read_len, optional_yield y,
                   rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r
                  ) {

  /* if IOC daemon still don't startup, or IOC daemon crash,
     or session occur any error, try to re-connect daemon. */
  std::unique_lock locker{m_lock};
  if(!m_cache_client->is_session_work()) {
    // go to read from rados first
    ldout(m_cct, 5) << "rgw_hook: session didn't work, go to rados" << oid << dendl;
    std::move(radosread)(aio,r);
    // then try to conect to IOC daemon.
    ldout(m_cct, 5) << "rgw_hook: IOC hook try to re-connect to IOC daemon." << dendl;
    create_cache_session(nullptr, true);
    return;
  }

  CacheGenContextURef gen_ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                            std::function<void(ObjectCacheRequest*)>>
            ([this, oid, read_ofs, read_len, y, radosread=std::move(radosread), aio, &r](ObjectCacheRequest* ack) mutable {
    handle_read_cache(ack, oid, read_ofs, read_len, y, std::move(radosread), aio, r);
  });

  ceph_assert(m_cache_client != nullptr);
  m_cache_client->lookup_object(pool_ns,
                                pool_id,
                                CEPH_NOSNAP,
                                read_len,
                                oid,
                                std::move(gen_ctx));

  return;
}

void IOChook::handle_read_cache(ObjectCacheRequest* ack, std::string oid,
                                off_t read_ofs, off_t read_len, optional_yield y,
                                rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r
                                ) {
  if(ack->type != RBDSC_READ_REPLY) {
    // go back to read rados
    ldout(m_cct, 5) << "rgw_hook: object is not in IOC cache, go to rados " << oid << dendl;
    std::move(radosread)(aio,r);
    return;
  }

  ceph_assert(ack->type == RBDSC_READ_REPLY);
  std::string file_path = ((ObjectCacheReadReplyData*)ack)->cache_path;
  if(file_path.empty()) {
    ldout(m_cct, 5) << "rgw_hook: file path is empty, go to rados" << oid << dendl;
    std::move(radosread)(aio,r);
    return;
  }

  ioc::file_aio_read_abstract(y, file_path, read_ofs, read_len, aio, r);

  return;
}
