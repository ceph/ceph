#include <thread>
#include <chrono>

#include "ReplicaClient.h"
#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "Types.h"
#include "RpmaOp.h"

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd.h"
#include "librbd/Types.h"

#include "common/errno.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::ReplicaClient: " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

using namespace librbd::cls_client;

ReplicaClient::ReplicaClient(CephContext *cct, uint64_t size, uint32_t copies, std::string pool_name, std::string image_name, librados::IoCtx& ioctx)
    : _size(size), _copies(copies), _pool_name(std::move(pool_name)), _image_name(std::move(image_name)),
      _cct(cct), _reactor(std::make_shared<Reactor>(cct)),
      _ping(std::make_unique<PrimaryPing>(cct, io_ctx, this)),
      rados(ioctx) {}

ReplicaClient::~ReplicaClient() {
  ldout(_cct, 20) << dendl;
  shutdown();
  _reactor_thread->join();
  if (_error_handler_context != nullptr) {
    delete _error_handler_context;
  }
}

void ReplicaClient::shutdown() {
  ldout(_cct, 20) << dendl;
  _reactor->shutdown();
}

int ReplicaClient::init(void *head_ptr, uint64_t size, Context *error_callback) {
  ldout(_cct, 20) << dendl;
  int r = 0;

  if (error_callback == nullptr) {
    //  The default implementation just breaks into the debugger and assert
    _error_handler_context = new LambdaContext([this, cct = _cct](int r) {
      lderr(cct) << "Here has error to need to handle!!!" << dendl;
      ceph_assert(r == 0);
    });
  } else {
    _error_handler_context = error_callback;
  }

  if ((r = init_ioctx()) < 0) {
    lderr(_cct) << "replica: failed to init ioctx" << dendl;
    return r;
  }
  if ((r = cache_request()) < 0) {
    lderr(_cct) << "replica: failed to create cache file in remote replica" << dendl;
    return r;
  }

  set_error_handler_context();

  if((r = set_head(head_ptr, size)) < 0) {
    lderr(_cct) << "replica: failed to set head" << dendl;
    return r;
  }
  write((size_t)0, size);
  flush();
  return 0;
}

void ReplicaClient::close() {
  ldout(_cct, 20) << dendl;
  flush();
  replica_close();
  disconnect();
  cache_free();
}

void ReplicaClient::error_handle(int r) {
  ldout(_cct, 20) << dendl;
  if (_error_handler_context != nullptr) {
    _error_handler_context->complete(r);
    _error_handler_context = nullptr;
  }
}

int ReplicaClient::flush() {
  int r = 0;
  size_t cnt = 0;
  std::mutex _flush_lock;
  std::condition_variable _flushed_var;
  size_t _flush_count{0};

  for (auto &daemon : _daemons) {
    if (!daemon.client_handler || !daemon.client_handler->connecting()) continue;
    cnt++;
    {
      std::lock_guard locker(_flush_lock);
      _flush_count++;
    }
    {
      r = daemon.client_handler->flush([this, &_flush_lock, &_flushed_var, &_flush_count]() mutable {
        {
          std::lock_guard locker(_flush_lock);
          _flush_count--;
          if (_flush_count == 0) {
            _flushed_var.notify_one();
          }
        }
        ldout(_cct, 20) << "flush finished " << dendl;
      });
      ceph_assert(r == 0);
    }
  }
  {
    std::unique_lock locker(_flush_lock);
    _flushed_var.wait(locker, [&_flush_count]{return _flush_count == 0;});
  }
  return (cnt == _daemons.size() ? 0 : -1);
}

int ReplicaClient::write(size_t offset, size_t len) {
  ldout(_cct, 20) << offset << ":" << len << dendl;
  int r = 0;
  size_t cnt = 0;
  len = (len <= 128 ? 128 : len);
  static size_t one_gigabyte = 1024 * 1024 * 1024;
  while (len > one_gigabyte) {
    for (auto &daemon : _daemons) {
      if (!daemon.client_handler || !daemon.client_handler->connecting()) continue;
      cnt++;
      r = daemon.client_handler->write(offset, one_gigabyte);
      ceph_assert(r == 0);
    }
    offset += one_gigabyte;
    len -= one_gigabyte;
  }
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler || !daemon.client_handler->connecting()) continue;
    cnt++;
    r = daemon.client_handler->write(offset, len);
    ceph_assert(r == 0);
  }
  return (cnt == _daemons.size() ? 0 : -1);
}

int ReplicaClient::write(const void* addr, size_t len) {
  ceph_assert(addr);
  size_t offset = static_cast<size_t>(static_cast<const char*>(addr) - static_cast<const char*>(addr));
  return write(offset, len);
}

int ReplicaClient::cache_free() {
  int r = 0;
  cls::rbd::RwlCacheFree free{_cache_id, _need_free_daemons};
  r = rwlcache_free(&io_ctx, free);
  if (r < 0) {
      ldout(_cct, 1) << "rwlcache_free: " << r << cpp_strerror(r) << dendl;
  }
  return r;
}

int ReplicaClient::replica_close() {
  ldout(_cct, 20) << dendl;
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler || !daemon.client_handler->connecting()) continue;
    if (_need_free_daemons.count(daemon.id) == 0) continue;  // part alloc failed, so need to free that successful part
    r = daemon.client_handler->close_replica();
    if (r == 0) {
      _need_free_daemons.erase(daemon.id);  // only close successfully to erase
    }
  }
  return 0;
}

int ReplicaClient::replica_init() {
  ldout(_cct, 10) << "pool_name: " << _pool_name << "\n"
                  << "image_name: " << _image_name << "\n"
                  << dendl;
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler || !daemon.client_handler->connecting()) continue;
    _need_free_daemons.insert(daemon.id); //maybe it create cachefile in replica daemon, but reply failed
    r = daemon.client_handler->init_replica(_cache_id, _size, _pool_name, _image_name);
    if (r < 0) {
      replica_close();
      return r;
    }
  }
  return 0;
}

int ReplicaClient::set_head(void *head_ptr, uint64_t size) {
  ldout(_cct, 10) << "head_ptr: " << head_ptr << "\n"
                  << "size: " << size << "\n"
                  << dendl;
  ceph_assert(size <= _size);
  for (auto &daemon : _daemons) {
    if (daemon.client_handler) {
      int r = daemon.client_handler->set_head(head_ptr, size);
      if (r < 0) {
        return r;
      }
    }
  }
  _local_head_ptr = head_ptr;
  return 0;
}

void ReplicaClient::set_error_handler_context() {
  ldout(_cct, 20) << dendl;
  for (auto &daemon : _daemons) {
    if (daemon.client_handler) {
      daemon.client_handler->set_error_handler_context(new LambdaContext([this](int r){
        if (r >= 0) {
          ldout(this->_cct, 20) << "There is no error." << dendl;
        } else {
          this->error_handle(r);
        }
      }));
    }
  }
  return;
}

int ReplicaClient::init_ioctx() {
  int r = 0;
  ldout(_cct, 20) << rados.get_instance_id() << dendl;
  std::string poolname = _cct->_conf.get_val<std::string>("rbd_persistent_replicated_cache_cls_pool");
  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0) {
    ldout(_cct, 1) << "failed to access pool "<< cpp_strerror(r) << dendl;
  }
  return r;
}

void ReplicaClient::disconnect() {
  ldout(_cct, 20) << dendl;
  for (auto &daemon : _daemons) {
    if (daemon.client_handler && daemon.client_handler->connecting()) {
      daemon.client_handler->disconnect();
      daemon.client_handler->wait_disconnected();
      daemon.client_handler.reset();
    }
  }
}

int ReplicaClient::cache_request() {
  ldout(_cct, 20) << dendl;
  _id = rados.get_instance_id();

  int r = 0;
  cls::rbd::RwlCacheRequest req = {_id, _size, _copies};
  r = rwlcache_request(&io_ctx, req, _cache_id);
  if (r < 0) {
    ldout(_cct, 1) << "rwlcache_request: " << r << cpp_strerror(r) << dendl;
    return r;
  }
  ldout(_cct, 20) << "cache_id: " << _cache_id << dendl;

  cls::rbd::RwlCacheInfo info;
  r = rwlcache_get_cacheinfo(&io_ctx, _cache_id, info);
  if (r < 0) {
    ldout(_cct, 1) << "rwlcache_get_cacheinfo: " << r << cpp_strerror(r) << dendl;
    return r;
  }

  ceph_assert(info.cache_id == _cache_id);

  for (auto &daemon : info.daemons) {
    ldout(_cct, 10) << "daemons infomation: \n"
                    << "id: " << daemon.id << "\n"
                    << "rdma_address: " << daemon.rdma_address << "\n"
                    << "rdma_port: " << daemon.rdma_port << "\n"
                    << dendl;
    _daemons.push_back(DaemonInfo{daemon.id, daemon.rdma_address, std::to_string(daemon.rdma_port), nullptr});
  }

  //connect to replica
  for (auto &daemon : _daemons) {
    try {
      daemon.client_handler = std::make_shared<ClientHandler>(_cct, daemon.rdma_ip, daemon.rdma_port, _reactor);

      r = daemon.client_handler->register_self();
      ceph_assert(r == 0);
    } catch (std::runtime_error &e) {
      ldout(_cct, 1) << "Runtione error: " << e.what() << dendl;
      disconnect();
      goto request_ack;
    }
  }

  _reactor_thread = std::make_unique<std::thread>([this]{
    this->_reactor->handle_events();
    ldout(_cct, 10) << "End with handle events " << dendl;
  });

  for (auto &daemon : _daemons) {
    bool succeed = daemon.client_handler->wait_established();
    if (!succeed) {
      goto request_ack;
    }
  }

  r = replica_init();

  if (r == 0) {
    _ping->timer_ping();
  }

request_ack:
  // if the connection failed, what do we do after?
  // Or it will be blocked on waiting established.
  // TODO: there should have success and failed
  cls::rbd::RwlCacheRequestAck ack{_cache_id, r, _need_free_daemons};
  int ret = rwlcache_request_ack(&io_ctx, ack);
  if (ret < 0) {
    ldout(_cct, 1) << "rwlcache_request_ack: " << ret << cpp_strerror(ret) << dendl;
  }

  return r;
}

bool ReplicaClient::single_ping() {
  ldout(_cct, 20) << dendl;
  int r = 0;
  bool has_removed_daemon = true;
  r = rwlcache_primaryping(&io_ctx, _cache_id, has_removed_daemon);
  return (r == 0 && !has_removed_daemon);
}

}
