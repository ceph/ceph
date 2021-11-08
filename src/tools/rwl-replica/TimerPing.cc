#include "TimerPing.h"
#include <mutex>
#include <filesystem>
namespace fs = std::filesystem;


#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd.h"
#include "librbd/Types.h"

#include "common/errno.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::TimePing: " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

using namespace librbd::cls_client;

DaemonPing::DaemonPing(CephContext *cct, librados::Rados &rados, librados::IoCtx &io_ctx)
  : _cct(cct), _rados(rados), _io_ctx(io_ctx),
  _mutex(ceph::make_mutex("daemon ping")),
  _ping_timer(cct, _mutex) {
}
DaemonPing::~DaemonPing() {
  std::lock_guard locker(_mutex);
  _ping_timer.cancel_all_events();
  _ping_timer.shutdown();
}

void DaemonPing::init(std::shared_ptr<Reactor> reactor) {
  _ping_timer.init();
  _reactor = reactor;
}

int DaemonPing::timer_ping() {
  std::lock_guard locker(_mutex);
  _ping_timer.add_event_after(100, new C_Ping(shared_from_this()));
  return 0;
}

int DaemonPing::free_caches() {
  update_cacheinfos();
  for (auto &id : need_free_caches) {
    if (infos.count(id) == 0) {
      freed_caches.insert(id);
      continue;
    }
    std::string cachefile_name("rbd-pwl." + infos[id].pool_name + "." + infos[id].image_name + ".pool." + std::to_string(infos[id].cache_id));
    std::string path = _cct->_conf->rwl_replica_path;
    fs::remove(path + "/" + cachefile_name);
    freed_caches.insert(id);
    infos.erase(id);
  }
  need_free_caches.clear();
  return 0;
}

int DaemonPing::single_ping() {
  ldout(_cct, 5) << dendl;
  cls::rbd::RwlCacheDaemonPing ping{_rados.get_instance_id(), freed_caches};
  bool has_need_free_cache{false};
  int r = rwlcache_daemonping(&_io_ctx, ping, has_need_free_cache);
  if (r < 0) {
    return r;
  }
  freed_caches.clear();

  if (has_need_free_cache) {
    cls::rbd::RwlCacheDaemonNeedFreeCaches caches;
    r = rwlcache_get_needfree_caches(&_io_ctx, _rados.get_instance_id(), caches);
    if (r < 0) {
      return r;
    }
    need_free_caches.swap(caches.need_free_caches);
  }
  return 0;
}

int DaemonPing::get_cache_info_from_filename(fs::path file, struct RwlCacheInfo& info) {
  info.cache_size = fs::file_size(file);
  std::string filename = file.filename();

  if (filename.compare(0, 7, "rbd-pwl") != 0) {
    return -EINVAL;
  }

  // find pool_name
  std::string::size_type found = filename.find('.');
  if (found == std::string::npos) {
    return -EINVAL;
  }
  info.pool_name = filename.substr(found + 1, filename.find('.', found + 1) - found - 1);

  // find image_name
  found = filename.find('.', found + 1);
  if (found == std::string::npos) {
    return -EINVAL;
  }
  info.image_name = filename.substr(found + 1, filename.find('.', found + 1) - found - 1);

  // find id
  found = filename.rfind('.');
  if (found == std::string::npos) {
    return -EINVAL;
  }
  info.cache_id = std::strtoul(filename.substr(found + 1).c_str(), nullptr, 10);
  return 0;
}

void DaemonPing::update_cacheinfos() {
  std::string path = _cct->_conf->rwl_replica_path;

  std::unordered_map<epoch_t, RwlCacheInfo> old_infos;
  old_infos.swap(infos);
  for (auto& p : fs::directory_iterator(path)) {
    RwlCacheInfo info;
    if (fs::is_regular_file(p.status())
        && !get_cache_info_from_filename(p.path(), info)) {
      infos.emplace(info.cache_id, std::move(info));
      old_infos.erase(info.cache_id);
    }
  }
  for (auto& info : old_infos) {
    freed_caches.insert(info.first);
  }
}

DaemonPing::C_Ping::C_Ping(std::shared_ptr<DaemonPing> dp) : dp(dp) {}
DaemonPing::C_Ping::~C_Ping() {}
void DaemonPing::C_Ping::finish(int r) {
  int ret = dp->single_ping();
  if (ret < 0) {
    if (dp->_reactor) {
      ldout(dp->_cct, 1) << "Error: shutdown in DaemonPing" << dendl;
      dp->_reactor->shutdown();
    }
    return;
  }

  dp->_ping_timer.add_event_after(RBD_RWLCACHE_DAEMON_PING_TIMEOUT >> 1, new C_Ping(dp));

  dp->free_caches();
}

PrimaryPing::PrimaryPing(CephContext *cct, librados::IoCtx &io_ctx, ReplicaClient* client)
  : _cct(cct), _io_ctx(io_ctx), _mutex(ceph::make_mutex("primary ping")),
  _ping_timer(cct, _mutex), _client(client)  {
    _ping_timer.init();
}
PrimaryPing::~PrimaryPing() {
  ldout(_cct, 20) << dendl;
  std::lock_guard locker(_mutex);
  _ping_timer.cancel_all_events();
  _ping_timer.shutdown();
}

int PrimaryPing::timer_ping() {
  std::lock_guard locker(_mutex);
  _ping_timer.add_event_after(5, new C_Ping(this));
  return 0;
}
PrimaryPing::C_Ping::C_Ping(PrimaryPing* ping) : pping(ping) {}
PrimaryPing::C_Ping::~C_Ping() {}
void PrimaryPing::C_Ping::finish(int r) {
  bool ok = pping->_client->single_ping();
  if (ok) {
    pping->_ping_timer.add_event_after(RBD_RWLCACHE_PRIMARY_PING_TIMEOUT >> 1, new C_Ping(pping));
  } else {
    ldout(pping->_cct, 1) << "Error: shutdown in PrimaryPing" << dendl;
    pping->_client->close();
    pping->_client->shutdown();
    // error_handle should stop write and flush cachefile
    pping->_client->error_handle(-ok);
  }
}

}
