// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CacheController.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheController: " << this << " " \
                           << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

CacheController::CacheController(CephContext *cct, const std::vector<const char*> &args):
  m_args(args), m_cct(cct) {
  ldout(m_cct, 20) << dendl;
}

CacheController::~CacheController() {
  delete m_cache_server;
  delete m_object_cache_store;
}

int CacheController::init() {
  ldout(m_cct, 20) << dendl;

  m_object_cache_store = new ObjectCacheStore(m_cct, pcache_op_work_queue);
  //TODO(): make this configurable
  int r = m_object_cache_store->init(true);
  if (r < 0) {
    lderr(m_cct) << "init error\n" << dendl;
  }
  return r;
}

int CacheController::shutdown() {
  ldout(m_cct, 20) << dendl;

  int r = m_object_cache_store->shutdown();
  return r;
}

void CacheController::handle_signal(int signum){}

void CacheController::run() {
  try {
    std::string controller_path = m_cct->_conf.get_val<std::string>("rbd_shared_cache_sock");
    std::remove(controller_path.c_str()); 
    
    m_cache_server = new CacheServer(m_cct, controller_path,
      ([&](uint64_t p, std::string s){handle_request(p, s);}));

    int ret = m_cache_server->run();
    if (ret != 0) {
      throw std::runtime_error("io serivce run error");
    }
  } catch (std::exception& e) {
    lderr(m_cct) << "Exception: " << e.what() << dendl;
  }
}

void CacheController::handle_request(uint64_t session_id, std::string msg){
  ldout(m_cct, 20) << dendl;

  rbdsc_req_type_t *io_ctx = (rbdsc_req_type_t*)(msg.c_str());

  switch (io_ctx->type) {
    case RBDSC_REGISTER: {
      // init cache layout for volume        
      m_object_cache_store->init_cache(io_ctx->pool_name, io_ctx->vol_name, io_ctx->vol_size);
      io_ctx->type = RBDSC_REGISTER_REPLY;
      m_cache_server->send(session_id, std::string((char*)io_ctx, msg.size()));

      break;
    }
    case RBDSC_READ: {
      // lookup object in local cache store
      int ret = m_object_cache_store->lookup_object(io_ctx->pool_name, io_ctx->vol_name, io_ctx->oid);
      if (ret < 0) {
        io_ctx->type = RBDSC_READ_RADOS;
      } else {
        io_ctx->type = RBDSC_READ_REPLY;
      }
      m_cache_server->send(session_id, std::string((char*)io_ctx, msg.size()));

      break;
    }
    ldout(m_cct, 5) << "can't recongize request" << dendl;
    assert(0); // TODO replace it.
  }
}

} // namespace immutable_obj_cache
} // namespace ceph


