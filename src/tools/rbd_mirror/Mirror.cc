// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "Mirror.h"
#include "ServiceDaemon.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Mirror: " << this << " " \
                           << __func__ << ": "

using std::list;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;
using librbd::mirror_peer_t;

namespace rbd {
namespace mirror {

namespace {

class MirrorAdminSocketCommand {
public:
  virtual ~MirrorAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

class StatusCommand : public MirrorAdminSocketCommand {
public:
  explicit StatusCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->print_status(f, ss);
    return true;
  }

private:
  Mirror *mirror;
};

class StartCommand : public MirrorAdminSocketCommand {
public:
  explicit StartCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->start();
    return true;
  }

private:
  Mirror *mirror;
};

class StopCommand : public MirrorAdminSocketCommand {
public:
  explicit StopCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->stop();
    return true;
  }

private:
  Mirror *mirror;
};

class RestartCommand : public MirrorAdminSocketCommand {
public:
  explicit RestartCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->restart();
    return true;
  }

private:
  Mirror *mirror;
};

class FlushCommand : public MirrorAdminSocketCommand {
public:
  explicit FlushCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->flush();
    return true;
  }

private:
  Mirror *mirror;
};

class LeaderReleaseCommand : public MirrorAdminSocketCommand {
public:
  explicit LeaderReleaseCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->release_leader();
    return true;
  }

private:
  Mirror *mirror;
};

} // anonymous namespace

class MirrorAdminSocketHook : public AdminSocketHook {
public:
  MirrorAdminSocketHook(CephContext *cct, Mirror *mirror) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status";
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror");
    if (r == 0) {
      commands[command] = new StatusCommand(mirror);
    }

    command = "rbd mirror start";
    r = admin_socket->register_command(command, command, this,
				       "start rbd mirror");
    if (r == 0) {
      commands[command] = new StartCommand(mirror);
    }

    command = "rbd mirror stop";
    r = admin_socket->register_command(command, command, this,
				       "stop rbd mirror");
    if (r == 0) {
      commands[command] = new StopCommand(mirror);
    }

    command = "rbd mirror restart";
    r = admin_socket->register_command(command, command, this,
				       "restart rbd mirror");
    if (r == 0) {
      commands[command] = new RestartCommand(mirror);
    }

    command = "rbd mirror flush";
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror");
    if (r == 0) {
      commands[command] = new FlushCommand(mirror);
    }

    command = "rbd mirror leader release";
    r = admin_socket->register_command(command, command, this,
				       "release rbd mirror leader");
    if (r == 0) {
      commands[command] = new LeaderReleaseCommand(mirror);
    }
  }

  ~MirrorAdminSocketHook() override {
    for (Commands::const_iterator i = commands.begin(); i != commands.end();
	 ++i) {
      (void)admin_socket->unregister_command(i->first);
      delete i->second;
    }
  }

  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) override {
    Commands::const_iterator i = commands.find(command);
    assert(i != commands.end());
    Formatter *f = Formatter::create(format);
    stringstream ss;
    bool r = i->second->call(f, &ss);
    delete f;
    out.append(ss);
    return r;
  }

private:
  typedef std::map<std::string, MirrorAdminSocketCommand*, std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

Mirror::Mirror(CephContext *cct, const std::vector<const char*> &args) :
  m_cct(cct),
  m_args(args),
  m_lock("rbd::mirror::Mirror"),
  m_local(new librados::Rados()),
  m_asok_hook(new MirrorAdminSocketHook(cct, this))
{
  m_threads =
    &(cct->lookup_or_create_singleton_object<Threads<librbd::ImageCtx>>(
	"rbd_mirror::threads", false, cct));
  m_service_daemon.reset(new ServiceDaemon<>(m_cct, m_local, m_threads));
}

Mirror::~Mirror()
{
  delete m_asok_hook;
}

void Mirror::handle_signal(int signum)
{
  m_stopping = true;
  {
    Mutex::Locker l(m_lock);
    m_cond.Signal();
  }
}

int Mirror::init()
{
  int r = m_local->init_with_context(m_cct);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    return r;
  }

  r = m_local->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    return r;
  }

  r = m_service_daemon->init();
  if (r < 0) {
    derr << "error registering service daemon: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_local_cluster_watcher.reset(new ClusterWatcher(m_local, m_lock,
                                                   m_service_daemon.get()));
  return r;
}

void Mirror::run()
{
  dout(20) << "enter" << dendl;
  while (!m_stopping) {
    m_local_cluster_watcher->refresh_pools();
    Mutex::Locker l(m_lock);
    if (!m_manual_stop) {
      update_pool_replayers(m_local_cluster_watcher->get_pool_peers());
    }
    m_cond.WaitInterval(
      m_lock,
      utime_t(m_cct->_conf->get_val<int64_t>("rbd_mirror_pool_replayers_refresh_interval"), 0));
  }

  // stop all pool replayers in parallel
  Mutex::Locker locker(m_lock);
  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->stop(false);
  }
  dout(20) << "return" << dendl;
}

void Mirror::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  if (f) {
    f->open_object_section("mirror_status");
    f->open_array_section("pool_replayers");
  };

  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->print_status(f, ss);
  }

  if (f) {
    f->close_section();
  }
}

void Mirror::start()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  m_manual_stop = false;

  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->start();
  }
}

void Mirror::stop()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  m_manual_stop = true;

  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->stop(true);
  }
}

void Mirror::restart()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  m_manual_stop = false;

  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->restart();
  }
}

void Mirror::flush()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping || m_manual_stop) {
    return;
  }

  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->flush();
  }
}

void Mirror::release_leader()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  for (auto &pool_replayer : m_pool_replayers) {
    pool_replayer.second->release_leader();
  }
}

void Mirror::update_pool_replayers(const PoolPeers &pool_peers)
{
  dout(20) << "enter" << dendl;
  assert(m_lock.is_locked());

  // remove stale pool replayers before creating new pool replayers
  for (auto it = m_pool_replayers.begin(); it != m_pool_replayers.end();) {
    auto &peer = it->first.second;
    auto pool_peer_it = pool_peers.find(it->first.first);
    if (pool_peer_it == pool_peers.end() ||
        pool_peer_it->second.find(peer) == pool_peer_it->second.end()) {
      dout(20) << "removing pool replayer for " << peer << dendl;
      // TODO: make async
      it->second->shut_down();
      it = m_pool_replayers.erase(it);
    } else {
      ++it;
    }
  }

  for (auto &kv : pool_peers) {
    for (auto &peer : kv.second) {
      PoolPeer pool_peer(kv.first, peer);

      auto pool_replayers_it = m_pool_replayers.find(pool_peer);
      if (pool_replayers_it != m_pool_replayers.end()) {
        auto& pool_replayer = pool_replayers_it->second;
        if (pool_replayer->is_blacklisted()) {
          derr << "restarting blacklisted pool replayer for " << peer << dendl;
          // TODO: make async
          pool_replayer->shut_down();
          pool_replayer->init();
        } else if (!pool_replayer->is_running()) {
          derr << "restarting failed pool replayer for " << peer << dendl;
          // TODO: make async
          pool_replayer->shut_down();
          pool_replayer->init();
        }
      } else {
        dout(20) << "starting pool replayer for " << peer << dendl;
        unique_ptr<PoolReplayer> pool_replayer(new PoolReplayer(
	  m_threads, m_service_daemon.get(), kv.first, peer, m_args));

        // TODO: make async
        pool_replayer->init();
        m_pool_replayers.emplace(pool_peer, std::move(pool_replayer));
      }
    }

    // TODO currently only support a single peer
  }
}

} // namespace mirror
} // namespace rbd
