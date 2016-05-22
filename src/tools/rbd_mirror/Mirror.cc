// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "Mirror.h"
#include "Threads.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: Mirror::" << __func__ << ": "

using std::chrono::seconds;
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

  bool call(Formatter *f, stringstream *ss) {
    mirror->print_status(f, ss);
    return true;
  }

private:
  Mirror *mirror;
};

class StartCommand : public MirrorAdminSocketCommand {
public:
  explicit StartCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) {
    mirror->start();
    return true;
  }

private:
  Mirror *mirror;
};

class StopCommand : public MirrorAdminSocketCommand {
public:
  explicit StopCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) {
    mirror->stop();
    return true;
  }

private:
  Mirror *mirror;
};

class RestartCommand : public MirrorAdminSocketCommand {
public:
  explicit RestartCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) {
    mirror->restart();
    return true;
  }

private:
  Mirror *mirror;
};

class FlushCommand : public MirrorAdminSocketCommand {
public:
  explicit FlushCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) {
    mirror->flush();
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
  }

  ~MirrorAdminSocketHook() {
    for (Commands::const_iterator i = commands.begin(); i != commands.end();
	 ++i) {
      (void)admin_socket->unregister_command(i->first);
      delete i->second;
    }
  }

  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
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
  typedef std::map<std::string, MirrorAdminSocketCommand*> Commands;

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
  cct->lookup_or_create_singleton_object<Threads>(m_threads,
                                                  "rbd_mirror::threads");
}

Mirror::~Mirror()
{
  delete m_asok_hook;
}

void Mirror::handle_signal(int signum)
{
  m_stopping.set(1);
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

  // TODO: make interval configurable
  m_local_cluster_watcher.reset(new ClusterWatcher(m_local, m_lock));

  m_image_deleter.reset(new ImageDeleter(m_local, m_threads->timer,
                                         &m_threads->timer_lock));

  return r;
}

void Mirror::run()
{
  dout(20) << "enter" << dendl;
  while (!m_stopping.read()) {
    m_local_cluster_watcher->refresh_pools();
    Mutex::Locker l(m_lock);
    if (!m_manual_stop) {
      update_replayers(m_local_cluster_watcher->get_peer_configs());
    }
    // TODO: make interval configurable
    m_cond.WaitInterval(g_ceph_context, m_lock, seconds(30));
  }
  dout(20) << "return" << dendl;
}

void Mirror::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  if (f) {
    f->open_object_section("mirror_status");
    f->open_array_section("replayers");
  };

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;
    replayer->print_status(f, ss);
  }

  if (f) {
    f->close_section();
    f->open_object_section("image_deleter");
  }

  m_image_deleter->print_status(f, ss);

  if (f) {
    f->close_section();
    f->close_section();
    f->flush(*ss);
  }
}

void Mirror::start()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;
    replayer->start();
  }
}

void Mirror::stop()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = true;

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;
    replayer->stop();
  }
}

void Mirror::restart()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;
    replayer->restart();
  }
}

void Mirror::flush()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping.read() || m_manual_stop) {
    return;
  }

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;
    replayer->flush();
  }
}

void Mirror::update_replayers(const map<peer_t, set<int64_t> > &peer_configs)
{
  dout(20) << "enter" << dendl;
  assert(m_lock.is_locked());
  for (auto &kv : peer_configs) {
    const peer_t &peer = kv.first;
    if (m_replayers.find(peer) == m_replayers.end()) {
      dout(20) << "starting replayer for " << peer << dendl;
      unique_ptr<Replayer> replayer(new Replayer(m_threads, m_image_deleter,
                                                 m_local, peer, m_args));
      // TODO: make async, and retry connecting within replayer
      int r = replayer->init();
      if (r < 0) {
	continue;
      }
      m_replayers.insert(std::make_pair(peer, std::move(replayer)));
    }
  }

  // TODO: make async
  for (auto it = m_replayers.begin(); it != m_replayers.end();) {
    peer_t peer = it->first;
    if (peer_configs.find(peer) == peer_configs.end()) {
      dout(20) << "removing replayer for " << peer << dendl;
      m_replayers.erase(it++);
    } else {
      ++it;
    }
  }
}

} // namespace mirror
} // namespace rbd
