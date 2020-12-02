// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_ioc_dispatch.h"
#include "rgw_common.h"
#include "rgw_aio.h"
#include "rgw_d3n_cacherequest.h"

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

void IOChook::read(const DoutPrefixProvider *dpp, std::string oid, std::string pool_ns, int64_t pool_id,
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
            ([this, dpp, oid, read_ofs, read_len, y, radosread=std::move(radosread), aio, &r](ObjectCacheRequest* ack) mutable {
    handle_read_cache(dpp, ack, oid, read_ofs, read_len, y, std::move(radosread), aio, r);
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

void IOChook::handle_read_cache(const DoutPrefixProvider *dpp, ObjectCacheRequest* ack, std::string oid,
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

  auto c = std::make_unique<D3nL1CacheRequest>();
  c->file_aio_read_abstract(dpp, y.get_io_context(), y.get_yield_context(), file_path, read_ofs, read_len, aio, r);

  return;
}
