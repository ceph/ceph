#include <thread>
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"

#include "AdminSocket.hpp"
#include "RBDImageStore.hpp"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_cache
#undef dout_prefix
#define dout_prefix *_dout << "rbd::cache::CacheController: " << this << " " \
                           << __func__ << ": "


using boost::asio::local::stream_protocol;
using librados::Rados;
using librados::IoCtx;

typedef shared_ptr<librados::Rados> RadosRef;
typedef shared_ptr<librados::IoCtx> IoCtxRef;


class CacheController {
 public:
  CacheController(CephContext *cct, const std::vector<const char*> &args):
  m_args(args),
  m_cct(cct){}
  //m_lock("rbd::cache::CacheController"),
  ~CacheController(){}

  int init() {
    m_imgstore = new RBDImageStore(m_cct);
    int r = m_imgstore->init();
    return r;
  }

  int shutdown() {
    int r = m_imgstore->shutdown();
    return r;
  }

  void handle_signal(int signum){}

  void run() {
    try {
      std::remove("/tmp/rbd_shared_readonly_cache_demo"); 
      server s(io_service, "/tmp/rbd_shared_readonly_cache_demo");
      //std::thread thd([this]() {this->io_service.run();});
      io_service.run();
    } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
    }
  }

 private:
  boost::asio::io_service io_service;
  std::vector<const char*> m_args;
  CephContext *m_cct;
  RBDImageStore *m_imgstore;
  //Mutex &m_lock;
};
