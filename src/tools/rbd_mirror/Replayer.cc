// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "global/global_context.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/Watcher.h"
#include "librbd/api/Mirror.h"
#include "InstanceWatcher.h"
#include "LeaderWatcher.h"
#include "Replayer.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Replayer: " \
                           << this << " " << __func__ << ": "

using std::chrono::seconds;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

using librbd::cls_client::dir_get_name;
using librbd::util::create_async_context_callback;

namespace rbd {
namespace mirror {

namespace {

class ReplayerAdminSocketCommand {
public:
  virtual ~ReplayerAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

class StatusCommand : public ReplayerAdminSocketCommand {
public:
  explicit StatusCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->print_status(f, ss);
    return true;
  }

private:
  Replayer *replayer;
};

class StartCommand : public ReplayerAdminSocketCommand {
public:
  explicit StartCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->start();
    return true;
  }

private:
  Replayer *replayer;
};

class StopCommand : public ReplayerAdminSocketCommand {
public:
  explicit StopCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->stop(true);
    return true;
  }

private:
  Replayer *replayer;
};

class RestartCommand : public ReplayerAdminSocketCommand {
public:
  explicit RestartCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->restart();
    return true;
  }

private:
  Replayer *replayer;
};

class FlushCommand : public ReplayerAdminSocketCommand {
public:
  explicit FlushCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->flush();
    return true;
  }

private:
  Replayer *replayer;
};

class LeaderReleaseCommand : public ReplayerAdminSocketCommand {
public:
  explicit LeaderReleaseCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->release_leader();
    return true;
  }

private:
  Replayer *replayer;
};

} // anonymous namespace

class ReplayerAdminSocketHook : public AdminSocketHook {
public:
  ReplayerAdminSocketHook(CephContext *cct, const std::string &name,
			  Replayer *replayer) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status " + name;
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StatusCommand(replayer);
    }

    command = "rbd mirror start " + name;
    r = admin_socket->register_command(command, command, this,
				       "start rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StartCommand(replayer);
    }

    command = "rbd mirror stop " + name;
    r = admin_socket->register_command(command, command, this,
				       "stop rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StopCommand(replayer);
    }

    command = "rbd mirror restart " + name;
    r = admin_socket->register_command(command, command, this,
				       "restart rbd mirror " + name);
    if (r == 0) {
      commands[command] = new RestartCommand(replayer);
    }

    command = "rbd mirror flush " + name;
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror " + name);
    if (r == 0) {
      commands[command] = new FlushCommand(replayer);
    }

    command = "rbd mirror leader release " + name;
    r = admin_socket->register_command(command, command, this,
                                       "release rbd mirror leader " + name);
    if (r == 0) {
      commands[command] = new LeaderReleaseCommand(replayer);
    }
  }

  ~ReplayerAdminSocketHook() override {
    for (Commands::const_iterator i = commands.begin(); i != commands.end();
	 ++i) {
      (void)admin_socket->unregister_command(i->first);
      delete i->second;
    }
  }

  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) override {
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
  typedef std::map<std::string, ReplayerAdminSocketCommand*> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

Replayer::Replayer(Threads *threads, std::shared_ptr<ImageDeleter> image_deleter,
                   ImageSyncThrottlerRef<> image_sync_throttler,
                   int64_t local_pool_id, const peer_t &peer,
                   const std::vector<const char*> &args) :
  m_threads(threads),
  m_image_deleter(image_deleter),
  m_image_sync_throttler(image_sync_throttler),
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_args(args),
  m_local_pool_id(local_pool_id),
  m_asok_hook(nullptr),
  m_replayer_thread(this),
  m_leader_listener(this)
{
}

Replayer::~Replayer()
{
  delete m_asok_hook;

  m_stopping.set(1);
  {
    Mutex::Locker l(m_lock);
    m_cond.Signal();
  }
  if (m_replayer_thread.is_started()) {
    m_replayer_thread.join();
  }
  if (m_leader_watcher) {
    m_leader_watcher->shut_down();
  }
  if (m_instance_watcher) {
    m_instance_watcher->shut_down();
  }
}

bool Replayer::is_blacklisted() const {
  Mutex::Locker locker(m_lock);
  return m_blacklisted;
}

bool Replayer::is_leader() const {
  Mutex::Locker locker(m_lock);
  return m_leader_watcher && m_leader_watcher->is_leader();
}

int Replayer::init()
{
  dout(20) << "replaying for " << m_peer << dendl;

  int r = init_rados(g_ceph_context->_conf->cluster,
                     g_ceph_context->_conf->name.to_str(),
                     "local cluster", &m_local_rados);
  if (r < 0) {
    return r;
  }

  r = init_rados(m_peer.cluster_name, m_peer.client_name,
                 std::string("remote peer ") + stringify(m_peer),
                 &m_remote_rados);
  if (r < 0) {
    return r;
  }

  r = m_local_rados->ioctx_create2(m_local_pool_id, m_local_io_ctx);
  if (r < 0) {
    derr << "error accessing local pool " << m_local_pool_id << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote_rados->ioctx_create(m_local_io_ctx.get_pool_name().c_str(),
                                   m_remote_io_ctx);
  if (r < 0) {
    derr << "error accessing remote pool " << m_local_io_ctx.get_pool_name()
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  m_remote_pool_id = m_remote_io_ctx.get_id();

  dout(20) << "connected to " << m_peer << dendl;

  m_leader_watcher.reset(new LeaderWatcher<>(m_threads, m_local_io_ctx,
                                             &m_leader_listener));
  r = m_leader_watcher->init();
  if (r < 0) {
    derr << "error initializing leader watcher: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_instance_watcher.reset(InstanceWatcher<>::create(m_local_io_ctx,
                                                     m_threads->work_queue));
  r = m_instance_watcher->init();
  if (r < 0) {
    derr << "error initializing instance watcher: " << cpp_strerror(r) << dendl;
    return r;
  }

  // Bootstrap existing mirroring images
  init_local_mirroring_images();

  m_pool_watcher.reset(new PoolWatcher(m_remote_io_ctx,
		       g_ceph_context->_conf->rbd_mirror_image_directory_refresh_interval,
		       m_lock, m_cond));
  m_pool_watcher->refresh_images();

  m_replayer_thread.create("replayer");

  return 0;
}

int Replayer::init_rados(const std::string &cluster_name,
                         const std::string &client_name,
                         const std::string &description, RadosRef *rados_ref) {
  rados_ref->reset(new librados::Rados());

  // NOTE: manually bootstrap a CephContext here instead of via
  // the librados API to avoid mixing global singletons between
  // the librados shared library and the daemon
  // TODO: eliminate intermingling of global singletons within Ceph APIs
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << "error initializing cluster handle for " << description << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  cct->_conf->cluster = cluster_name;

  // librados::Rados::conf_read_file
  int r = cct->_conf->parse_config_files(nullptr, nullptr, 0);
  if (r < 0) {
    derr << "could not read ceph conf for " << description << ": "
	 << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }
  cct->_conf->parse_env();

  // librados::Rados::conf_parse_env
  std::vector<const char*> args;
  env_to_vec(args, nullptr);
  r = cct->_conf->parse_argv(args);
  if (r < 0) {
    derr << "could not parse environment for " << description << ":"
         << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }

  if (!m_args.empty()) {
    // librados::Rados::conf_parse_argv
    args = m_args;
    r = cct->_conf->parse_argv(args);
    if (r < 0) {
      derr << "could not parse command line args for " << description << ": "
	   << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  // disable unnecessary librbd cache
  cct->_conf->set_val_or_die("rbd_cache", "false");
  cct->_conf->apply_changes(nullptr);
  cct->_conf->complain_about_parse_errors(cct);

  r = (*rados_ref)->init_with_context(cct);
  assert(r == 0);
  cct->put();

  r = (*rados_ref)->connect();
  if (r < 0) {
    derr << "error connecting to " << description << ": "
	 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

void Replayer::init_local_mirroring_images() {
  rbd_mirror_mode_t mirror_mode;
  int r = librbd::api::Mirror<>::mode_get(m_local_io_ctx, &mirror_mode);
  if (r < 0) {
    derr << "could not tell whether mirroring was enabled for "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return;
  }
  if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
    dout(20) << "pool " << m_local_io_ctx.get_pool_name() << " "
             << "has mirroring disabled" << dendl;
    return;
  }

  ImageIds image_ids;

  std::string last_read = "";
  int max_read = 1024;
  do {
    std::map<std::string, std::string> mirror_images;
    r = librbd::cls_client::mirror_image_list(&m_local_io_ctx, last_read,
                                              max_read, &mirror_images);
    if (r < 0) {
      derr << "error listing mirrored image directory: "
           << cpp_strerror(r) << dendl;
      continue;
    }
    for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {
      std::string image_name;
      r = dir_get_name(&m_local_io_ctx, RBD_DIRECTORY, it->first, &image_name);
      if (r < 0) {
        derr << "error retrieving local image name: " << cpp_strerror(r)
             << dendl;
        continue;
      }
      image_ids.insert(ImageId(it->second, it->first, image_name));
    }
    if (!mirror_images.empty()) {
      last_read = mirror_images.rbegin()->first;
    }
    r = mirror_images.size();
  } while (r == max_read);

  m_init_image_ids = std::move(image_ids);
}

void Replayer::run()
{
  dout(20) << "enter" << dendl;

  while (!m_stopping.read()) {

    std::string asok_hook_name = m_local_io_ctx.get_pool_name() + " " +
                                 m_peer.cluster_name;
    if (m_asok_hook_name != asok_hook_name || m_asok_hook == nullptr) {
      m_asok_hook_name = asok_hook_name;
      delete m_asok_hook;

      m_asok_hook = new ReplayerAdminSocketHook(g_ceph_context,
                                                m_asok_hook_name, this);
    }

    Mutex::Locker locker(m_lock);
    if (m_pool_watcher->is_blacklisted()) {
      m_blacklisted = true;
      m_stopping.set(1);
    } else if (!m_manual_stop && m_leader_watcher->is_leader()) {
      set_sources(m_pool_watcher->get_images());
    }

    if (m_blacklisted) {
      break;
    }
    m_cond.WaitInterval(m_lock,
			utime_t(g_ceph_context->_conf
				->rbd_mirror_image_state_check_interval, 0));
  }

  ImageIds empty_sources;
  while (true) {
    Mutex::Locker locker(m_lock);
    set_sources(empty_sources);
    if (m_image_replayers.empty()) {
      break;
    }
    m_cond.WaitInterval(m_lock, seconds(1));
  }
}

void Replayer::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  if (!f) {
    return;
  }

  Mutex::Locker l(m_lock);

  f->open_object_section("replayer_status");
  f->dump_string("pool", m_local_io_ctx.get_pool_name());
  f->dump_stream("peer") << m_peer;

  bool leader = m_leader_watcher->is_leader();
  f->dump_bool("leader", leader);
  if (leader) {
    std::vector<std::string> instance_ids;
    m_leader_watcher->list_instances(&instance_ids);
    f->open_array_section("instances");
    for (auto instance_id : instance_ids) {
      f->dump_string("instance_id", instance_id);
    }
    f->close_section();
  }
  f->open_array_section("image_replayers");

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->print_status(f, ss);
  }

  f->close_section();
  f->close_section();
  f->flush(*ss);
}

void Replayer::start()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->start(nullptr, true);
  }
}

void Replayer::stop(bool manual)
{
  dout(20) << "enter: manual=" << manual << dendl;

  Mutex::Locker l(m_lock);
  if (!manual) {
    m_stopping.set(1);
    m_cond.Signal();
    return;
  } else if (m_stopping.read()) {
    return;
  }

  m_manual_stop = true;
  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->stop(nullptr, true);
  }
}

void Replayer::restart()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->restart();
  }
}

void Replayer::flush()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read() || m_manual_stop) {
    return;
  }

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->flush();
  }
}

void Replayer::release_leader()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read() || !m_leader_watcher) {
    return;
  }

  m_leader_watcher->release_leader();
}

void Replayer::set_sources(const ImageIds &image_ids)
{
  dout(20) << "enter" << dendl;

  assert(m_lock.is_locked());

  if (!m_init_image_ids.empty() && !m_stopping.read() &&
      m_leader_watcher->is_leader()) {
    dout(20) << "scanning initial local image set" << dendl;
    for (auto &remote_image : image_ids) {
      auto it = m_init_image_ids.find(ImageId(remote_image.global_id));
      if (it != m_init_image_ids.end()) {
        m_init_image_ids.erase(it);
      }
    }

    // the remaining images in m_init_image_ids must be deleted
    for (auto &image_id : m_init_image_ids) {
      dout(20) << "scheduling the deletion of init image: "
               << image_id.global_id << " (" << image_id.id << ")" << dendl;
      m_image_deleter->schedule_image_delete(m_local_rados, m_local_pool_id,
                                             image_id.id, image_id.global_id);
    }
    m_init_image_ids.clear();
  }

  // shut down replayers for non-mirrored images
  for (auto image_it = m_image_replayers.begin();
       image_it != m_image_replayers.end();) {
    auto image_id_it = image_ids.find(ImageId(image_it->first));
    if (image_id_it == image_ids.end()) {
      if (image_it->second->is_running()) {
        dout(20) << "stop image replayer for remote image "
                 << image_it->second->get_global_image_id() << dendl;
      }
      if (stop_image_replayer(image_it->second)) {
        image_it = m_image_replayers.erase(image_it);
        continue;
      }
    }
    ++image_it;
  }

  if (image_ids.empty()) {
    return;
  }

  std::string local_mirror_uuid;
  int r = librbd::cls_client::mirror_uuid_get(&m_local_io_ctx,
                                              &local_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve local mirror uuid from pool "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return;
  }

  std::string remote_mirror_uuid;
  r = librbd::cls_client::mirror_uuid_get(&m_remote_io_ctx,
                                          &remote_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve remote mirror uuid from pool "
         << m_remote_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return;
  }

  for (auto &image_id : image_ids) {
    auto it = m_image_replayers.find(image_id.global_id);
    if (it == m_image_replayers.end()) {
      unique_ptr<ImageReplayer<> > image_replayer(new ImageReplayer<>(
        m_threads, m_image_deleter, m_image_sync_throttler, m_local_rados,
        local_mirror_uuid, m_local_pool_id, image_id.global_id));
      it = m_image_replayers.insert(
        std::make_pair(image_id.global_id, std::move(image_replayer))).first;
    }

    it->second->add_remote_image(remote_mirror_uuid, image_id.id,
                                 m_remote_io_ctx);
    if (!it->second->is_running()) {
      dout(20) << "starting image replayer for remote image "
               << image_id.global_id << dendl;
    }
    start_image_replayer(it->second);
  }
}

void Replayer::start_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer)
{
  assert(m_lock.is_locked());

  std::string global_image_id = image_replayer->get_global_image_id();
  dout(20) << "global_image_id=" << global_image_id << dendl;

  if (!image_replayer->is_stopped()) {
    return;
  } else if (image_replayer->is_blacklisted()) {
    derr << "blacklisted detected during image replay" << dendl;
    m_blacklisted = true;
    m_stopping.set(1);
    return;
  }

  FunctionContext *ctx = new FunctionContext(
      [this, global_image_id] (int r) {
        dout(20) << "image deleter result: r=" << r << ", "
                 << "global_image_id=" << global_image_id << dendl;
        if (r == -ESTALE || r == -ECANCELED) {
          return;
        }

        Mutex::Locker locker(m_lock);
        auto it = m_image_replayers.find(global_image_id);
        if (it == m_image_replayers.end()) {
          return;
        }

        auto &image_replayer = it->second;
        if (r >= 0) {
          image_replayer->start();
        } else {
          start_image_replayer(image_replayer);
        }
     }
  );

  m_image_deleter->wait_for_scheduled_deletion(
    m_local_pool_id, image_replayer->get_global_image_id(), ctx, false);
}

bool Replayer::stop_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer)
{
  assert(m_lock.is_locked());
  dout(20) << "global_image_id=" << image_replayer->get_global_image_id()
           << dendl;

  // TODO: check how long it is stopping and alert if it is too long.
  if (image_replayer->is_stopped()) {
    m_image_deleter->cancel_waiter(m_local_pool_id,
                                   image_replayer->get_global_image_id());

    if (!m_stopping.read() && m_leader_watcher->is_leader()) {
      dout(20) << "scheduling delete" << dendl;
      m_image_deleter->schedule_image_delete(
        m_local_rados,
        image_replayer->get_local_pool_id(),
        image_replayer->get_local_image_id(),
        image_replayer->get_global_image_id());
    }
    return true;
  } else {
    if (!m_stopping.read()) {
      dout(20) << "scheduling delete after image replayer stopped" << dendl;
    }
    FunctionContext *ctx = new FunctionContext(
        [&image_replayer, this] (int r) {
          if (!m_stopping.read() && m_leader_watcher->is_leader() && r >= 0) {
            m_image_deleter->schedule_image_delete(
              m_local_rados,
              image_replayer->get_local_pool_id(),
              image_replayer->get_local_image_id(),
              image_replayer->get_global_image_id());
          }
        }
    );
    image_replayer->stop(ctx);
  }

  return false;
}

void Replayer::handle_post_acquire_leader(Context *on_finish) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_cond.Signal();
  }

  on_finish->complete(0);
}

void Replayer::handle_pre_release_leader(Context *on_finish) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    set_sources(ImageIds());
    if (!m_image_replayers.empty()) {
      Mutex::Locker timer_locker(m_threads->timer_lock);
      Context *task = create_async_context_callback(
        m_threads->work_queue, new FunctionContext(
          [this, on_finish](int r) {
            handle_pre_release_leader(on_finish);
          }));
      m_threads->timer->add_event_after(1, task);
      return;
    }
  }

  on_finish->complete(0);
}

} // namespace mirror
} // namespace rbd
