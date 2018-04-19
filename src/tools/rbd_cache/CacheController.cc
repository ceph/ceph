// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CacheController.hpp"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_cache
#undef dout_prefix
#define dout_prefix *_dout << "rbd::cache::CacheController: " << this << " " \
                           << __func__ << ": "


class ThreadPoolSingleton : public ThreadPool {
public:
  ContextWQ *op_work_queue;

  explicit ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "librbd::cache::thread_pool", "tp_librbd_cache", 32,
                 "pcache_threads"),
      op_work_queue(new ContextWQ("librbd::pcache_op_work_queue",
                    cct->_conf->get_val<int64_t>("rbd_op_thread_timeout"),
                    this)) {
    start();
  }
  ~ThreadPoolSingleton() override {
    op_work_queue->drain();
    delete op_work_queue;

    stop();
  }
};


CacheController::CacheController(CephContext *cct, const std::vector<const char*> &args):
  m_args(args), m_cct(cct) {

}

CacheController::~CacheController() {

}

int CacheController::init() {
  ThreadPoolSingleton* thread_pool_singleton = &m_cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
    "rbd::cache::thread_pool", false, m_cct);
  pcache_op_work_queue = thread_pool_singleton->op_work_queue;

  m_object_cache_store = new ObjectCacheStore(m_cct, pcache_op_work_queue);
  int r = m_object_cache_store->init(false);
  if (r < 0) {
    //derr << "init error\n" << dendl;
  }
  return r;
}

int CacheController::shutdown() {
  int r = m_object_cache_store->shutdown();
  return r;
}

void CacheController::handle_signal(int signum){}

void CacheController::run() {
  try {
    //TODO(): use new socket path
    std::string controller_path = "/tmp/rbd_shared_readonly_cache_demo";
    std::remove(controller_path.c_str()); 
    
    m_cache_server = new CacheServer(io_service, controller_path,
      ([&](uint64_t p, std::string s){handle_request(p, s);}));
    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }
}

void CacheController::handle_request(uint64_t sesstion_id, std::string msg){
  rbdsc_req_type_t *io_ctx = (rbdsc_req_type_t*)(msg.c_str());

  int ret = 0;
  
  switch (io_ctx->type) {
    case RBDSC_REGISTER: {
      // init cache layout for volume        
      m_object_cache_store->init_cache(io_ctx->vol_name, io_ctx->vol_size);
      io_ctx->type = RBDSC_REGISTER_REPLY;
      m_cache_server->send(sesstion_id, std::string((char*)io_ctx, msg.size()));

      break;
    }
    case RBDSC_READ: {
      // lookup object in local cache store
      ret = m_object_cache_store->lookup_object(io_ctx->pool_name, io_ctx->vol_name);
      if (ret < 0) {
        io_ctx->type = RBDSC_READ_RADOS;
      } else {
        io_ctx->type = RBDSC_READ_REPLY;
      }
      m_cache_server->send(sesstion_id, std::string((char*)io_ctx, msg.size()));

      break;
    }
    
  }
}
