// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "ServiceDaemon.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/JSONFormatter.h"
#include "common/Timer.h"
#include "include/Context.h"
#include "include/stringify.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::ServiceDaemon: " << this << " " \
                           << __func__

namespace cephfs {
namespace mirror {

namespace {

struct AttributeDumpVisitor {
  ceph::Formatter *f;
  std::string name;

  AttributeDumpVisitor(ceph::Formatter *f, std::string_view name)
    : f(f), name(name) {
  }

  void operator()(bool val) const {
    f->dump_bool(name.c_str(), val);
  }
  void operator()(uint64_t val) const {
    f->dump_unsigned(name.c_str(), val);
  }
  void operator()(const std::string &val) const {
    f->dump_string(name.c_str(), val);
  }
};

} // anonymous namespace

ServiceDaemon::ServiceDaemon(CephContext *cct, RadosRef rados)
  : m_cct(cct),
    m_rados(rados),
    m_timer(new SafeTimer(cct, m_timer_lock, true)) {
  m_timer->init();
}

ServiceDaemon::~ServiceDaemon() {
  dout(10) << dendl;
  {
    std::scoped_lock timer_lock(m_timer_lock);
    if (m_timer_ctx != nullptr) {
      dout(5) << ": canceling timer task=" << m_timer_ctx << dendl;
      m_timer->cancel_event(m_timer_ctx);
    }
    m_timer->shutdown();
  }

  delete m_timer;
}

int ServiceDaemon::init() {
  dout(20) << dendl;

  std::string id = m_cct->_conf->name.get_id();
  if (id.find(CEPHFS_MIRROR_AUTH_ID_PREFIX) == 0) {
    id = id.substr(CEPHFS_MIRROR_AUTH_ID_PREFIX.size());
  }
  std::string instance_id = stringify(m_rados->get_instance_id());

  std::map<std::string, std::string> service_metadata = {{"id", id},
                                                         {"instance_id", instance_id}};
  int r = m_rados->service_daemon_register("cephfs-mirror", instance_id,
                                           service_metadata);
  if (r < 0) {
    return r;
  }
  return 0;
}

void ServiceDaemon::add_filesystem(fs_cluster_id_t fscid, std::string_view fs_name) {
  dout(10) << ": fscid=" << fscid << ", fs_name=" << fs_name << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_filesystems.emplace(fscid, Filesystem(fs_name));
  }
  schedule_update_status();
}

void ServiceDaemon::remove_filesystem(fs_cluster_id_t fscid) {
  dout(10) << ": fscid=" << fscid << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_filesystems.erase(fscid);
  }
  schedule_update_status();
}

void ServiceDaemon::add_peer(fs_cluster_id_t fscid, const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }
    fs_it->second.peer_attributes.emplace(peer, Attributes{});
  }
  schedule_update_status();
}

void ServiceDaemon::remove_peer(fs_cluster_id_t fscid, const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }
    fs_it->second.peer_attributes.erase(peer);
  }
  schedule_update_status();
}

void ServiceDaemon::add_or_update_fs_attribute(fs_cluster_id_t fscid, std::string_view key,
                                               AttributeValue value) {
  dout(10) << ": fscid=" << fscid << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }

    fs_it->second.fs_attributes[std::string(key)] = value;
  }
  schedule_update_status();
}

void ServiceDaemon::add_or_update_peer_attribute(fs_cluster_id_t fscid, const Peer &peer,
                                                 std::string_view key, AttributeValue value) {
  dout(10) << ": fscid=" << fscid << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }

    auto peer_it = fs_it->second.peer_attributes.find(peer);
    if (peer_it == fs_it->second.peer_attributes.end()) {
      return;
    }

    peer_it->second[std::string(key)] = value;
  }
  schedule_update_status();
}

void ServiceDaemon::schedule_update_status() {
  dout(10) << dendl;

  std::scoped_lock timer_lock(m_timer_lock);
  if (m_timer_ctx != nullptr) {
    return;
  }

  m_timer_ctx = new LambdaContext([this] {
                                    m_timer_ctx = nullptr;
                                    update_status();
                                    update_health();
                                  });
  m_timer->add_event_after(1, m_timer_ctx);
}

void ServiceDaemon::update_status() {
  dout(20) << ": " << m_filesystems.size() << " filesystem(s)" << dendl;

  ceph::JSONFormatter f;
  {
    std::scoped_lock locker(m_lock);
    f.open_object_section("filesystems");
    for (auto &[fscid, filesystem] : m_filesystems) {
      f.open_object_section(stringify(fscid).c_str());
      f.dump_string("name", filesystem.fs_name);
      for (auto &[attr_name, attr_value] : filesystem.fs_attributes) {
            AttributeDumpVisitor visitor(&f, attr_name);
            std::visit(visitor, attr_value);
      }
      f.open_object_section("peers");
      for (auto &[peer, attributes] : filesystem.peer_attributes) {
        f.open_object_section(peer.uuid);
        f.dump_object("remote", peer.remote);
        f.open_object_section("stats");
        for (auto &[attr_name, attr_value] : attributes) {
            AttributeDumpVisitor visitor(&f, attr_name);
            std::visit(visitor, attr_value);
        }
        f.close_section(); // stats
        f.close_section(); // peer.uuid
      }
      f.close_section(); // peers
      f.close_section(); // fscid
    }
    f.close_section(); // filesystems
  }

  std::stringstream ss;
  f.flush(ss);

  int r = m_rados->service_daemon_update_status({{"status_json", ss.str()}});
  if (r < 0) {
    derr << ": failed to update service daemon status: " << cpp_strerror(r)
         << dendl;
  }
}

std::vector<DaemonHealthMetric> ServiceDaemon::get_health_metrics() {
  std::vector<DaemonHealthMetric> health_metrics;
  {
    std::scoped_lock locker(m_lock);
    for (auto &[fscid, filesystem] : m_filesystems) {
      for (auto &[attr_name, attr_value] : filesystem.fs_attributes) {
        if (attr_name == SERVICE_DAEMON_MIRROR_ENABLE_FAILED_KEY) {
          bool failed = std::get<bool>(attr_value);
          if (failed) {
            health_metrics.emplace_back(daemon_metric::CEPHFS_MIRROR_FAILURE,
                                        failed, ceph_clock_now());
          }
        }
      }
      for (auto &[peer, attributes] : filesystem.peer_attributes) {
        uint64_t failed_dir_count = 0;
        uint64_t recovered_dir_count = 0;
        for (auto &[attr_name, attr_value] : attributes) {
          if (attr_name == SERVICE_DAEMON_FAILED_DIR_COUNT_KEY) {
            failed_dir_count = std::get<uint64_t>(attr_value);
          } else if (attr_name == SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY) {
            recovered_dir_count = std::get<uint64_t>(attr_value);
          }
        }
        uint64_t count = failed_dir_count - recovered_dir_count;
        if (count > 0) {
          health_metrics.emplace_back(daemon_metric::CEPHFS_MIRROR_SNAP_SYNC_FAILURE,
                                      count, ceph_clock_now());
        }
      }
    }
  }
  return health_metrics;
}

void ServiceDaemon::update_health()
{
  dout(20) << dendl;
  int r = m_rados->service_daemon_update_health(get_health_metrics());
  if (r < 0) {
    derr << ": failed to update mirror daemon health: " << cpp_strerror(r)
         << dendl;
  }
}

} // namespace mirror
} // namespace cephfs
