// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "librbd/journal/Replay.h"
#include "ImageReplayer.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: " << *this << "::" << __func__ << ": "

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

using librbd::util::create_context_callback;

namespace {

struct ReplayHandler : public ::journal::ReplayHandler {
  ImageReplayer *replayer;
  ReplayHandler(ImageReplayer *replayer) : replayer(replayer) {}

  virtual void get() {}
  virtual void put() {}

  virtual void handle_entries_available() {
    replayer->handle_replay_ready();
  }
  virtual void handle_complete(int r) {
    replayer->handle_replay_complete(r);
  }
};

struct C_ReplayCommitted : public Context {
  ImageReplayer *replayer;
  ::journal::ReplayEntry replay_entry;

  C_ReplayCommitted(ImageReplayer *replayer, ::journal::ReplayEntry &&replay_entry) :
    replayer(replayer), replay_entry(std::move(replay_entry)) {
  }
  virtual void finish(int r) {
    replayer->handle_replay_committed(&replay_entry, r);
  }
};

class ImageReplayerAdminSocketCommand {
public:
  virtual ~ImageReplayerAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

class StatusCommand : public ImageReplayerAdminSocketCommand {
public:
  explicit StatusCommand(ImageReplayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    if (f) {
      f->open_object_section("status");
      f->dump_stream("state") << replayer->get_state();
      f->close_section();
      f->flush(*ss);
    } else {
      *ss << "state: " << replayer->get_state();
    }
    return true;
  }

private:
  ImageReplayer *replayer;
};

class FlushCommand : public ImageReplayerAdminSocketCommand {
public:
  explicit FlushCommand(ImageReplayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    int r = replayer->flush();
    if (r < 0) {
      *ss << "flush: " << cpp_strerror(r);
      return false;
    }
    return true;
  }

private:
  ImageReplayer *replayer;
};

} // anonymous namespace

class ImageReplayerAdminSocketHook : public AdminSocketHook {
public:
  ImageReplayerAdminSocketHook(CephContext *cct, ImageReplayer *replayer) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status " + stringify(*replayer);
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror " +
				       stringify(*replayer));
    if (r == 0) {
      commands[command] = new StatusCommand(replayer);
    }

    command = "rbd mirror flush " + stringify(*replayer);
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror " +
				       stringify(*replayer));
    if (r == 0) {
      commands[command] = new FlushCommand(replayer);
    }
  }

  ~ImageReplayerAdminSocketHook() {
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
  typedef std::map<std::string, ImageReplayerAdminSocketCommand*> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

ImageReplayer::ImageReplayer(RadosRef local, RadosRef remote,
			     const std::string &client_id,
                             int64_t local_pool_id,
			     int64_t remote_pool_id,
			     const std::string &remote_image_id) :
  m_local(local),
  m_remote(remote),
  m_client_id(client_id),
  m_remote_pool_id(remote_pool_id),
  m_local_pool_id(local_pool_id),
  m_remote_image_id(remote_image_id),
  m_lock("rbd::mirror::ImageReplayer " + stringify(remote_pool_id) + " " +
	 remote_image_id),
  m_state(STATE_UNINITIALIZED),
  m_local_image_ctx(nullptr),
  m_local_replay(nullptr),
  m_remote_journaler(nullptr),
  m_replay_handler(nullptr)
{
  CephContext *cct = static_cast<CephContext *>(m_local->cct());

  m_asok_hook = new ImageReplayerAdminSocketHook(cct, this);
}

ImageReplayer::~ImageReplayer()
{
  assert(m_local_image_ctx == nullptr);
  assert(m_local_replay == nullptr);
  assert(m_remote_journaler == nullptr);
  assert(m_replay_handler == nullptr);

  delete m_asok_hook;
}

int ImageReplayer::start(const BootstrapParams *bootstrap_params)
{
  // TODO: make async

  dout(20) << "enter" << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_UNINITIALIZED || m_state == STATE_STOPPED);

    m_state = STATE_STARTING;
  }

  std::string remote_journal_id = m_remote_image_id;
  std::string image_name = "";
  C_SaferCond cond, lock_ctx;
  double commit_interval;
  bool registered;
  int r = 0;

  r = m_remote->ioctx_create2(m_remote_pool_id, m_remote_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for remote pool " << m_remote_pool_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  CephContext *cct = static_cast<CephContext *>(m_local->cct());
  commit_interval = cct->_conf->rbd_journal_commit_age;
  bool remote_journaler_initialized = false;
  m_remote_journaler = new ::journal::Journaler(m_remote_ioctx,
						remote_journal_id,
						m_client_id, commit_interval);
  r = get_registered_client_status(&registered);
  if (r < 0) {
    derr << "error obtaining registered client status: "
	 << cpp_strerror(r) << dendl;
    goto fail;
  }

  if (registered) {
    if (bootstrap_params) {
      dout(0) << "ignoring bootsrap params: client already registered" << dendl;
    }
  } else {
    r = bootstrap(bootstrap_params);
    if (r < 0) {
      derr << "bootstrap failed: " << cpp_strerror(r) << dendl;
      goto fail;
    }
  }

  m_remote_journaler->init(&cond);
  r = cond.wait();
  if (r < 0) {
    derr << "error initializing journal: " << cpp_strerror(r) << dendl;
    goto fail;
  }
  remote_journaler_initialized = true;

  r = m_local->ioctx_create2(m_local_pool_id, m_local_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for local pool " << m_local_pool_id
	 << ": " << cpp_strerror(r) << dendl;
    goto fail;
  }

  m_local_image_ctx = new librbd::ImageCtx("", m_local_image_id, NULL,
					   m_local_ioctx, false);
  r = m_local_image_ctx->state->open();
  if (r < 0) {
    derr << "error opening local image " <<  m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    delete m_local_image_ctx;
    m_local_image_ctx = nullptr;
    goto fail;
  }

  {
    RWLock::WLocker owner_locker(m_local_image_ctx->owner_lock);
    m_local_image_ctx->exclusive_lock->request_lock(&lock_ctx);
  }
  r = lock_ctx.wait();
  if (r < 0) {
    derr << "error to lock exclusively local image " <<  m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    goto fail;
  }

  if (m_local_image_ctx->journal == nullptr) {
    derr << "journaling is not enabled on local image " <<  m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    goto fail;
  }

  r = m_local_image_ctx->journal->start_external_replay(&m_local_replay);
  if (r < 0) {
    derr << "error starting external replay on local image "
	 <<  m_local_image_id << ": " << cpp_strerror(r) << dendl;
    goto fail;
  }

  m_replay_handler = new ReplayHandler(this);

  m_remote_journaler->start_live_replay(m_replay_handler,
					1 /* TODO: configurable */);

  dout(20) << "m_remote_journaler=" << *m_remote_journaler << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_STARTING);

    m_state = STATE_REPLAYING;
  }

  return 0;

fail:
  dout(20) << "fail, r=" << r << dendl;

  if (m_remote_journaler) {
    if (remote_journaler_initialized) {
      m_remote_journaler->stop_replay();
      m_remote_journaler->shutdown();
    }
    delete m_remote_journaler;
    m_remote_journaler = nullptr;
  }

  if (m_local_replay) {
    Mutex::Locker locker(m_lock);
    shut_down_journal_replay(true);
    m_local_image_ctx->journal->stop_external_replay();
    m_local_replay = nullptr;
  }

  if (m_replay_handler) {
    delete m_replay_handler;
    m_replay_handler = nullptr;
  }

  if (m_local_image_ctx) {
    bool owner;
    if (librbd::is_exclusive_lock_owner(m_local_image_ctx, &owner) == 0 &&
	owner) {
      librbd::unlock(m_local_image_ctx, "");
    }
    m_local_image_ctx->state->close();
    m_local_image_ctx = nullptr;
  }

  m_local_ioctx.close();
  m_remote_ioctx.close();

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_STARTING);

    m_state = STATE_UNINITIALIZED;
  }

  return r;
}

void ImageReplayer::stop()
{
  dout(20) << "enter" << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_REPLAYING);

    m_state = STATE_STOPPING;
  }

  shut_down_journal_replay(false);

  m_local_image_ctx->journal->stop_external_replay();
  m_local_replay = nullptr;

  m_local_image_ctx->state->close();
  m_local_image_ctx = nullptr;

  m_local_ioctx.close();

  m_remote_journaler->stop_replay();
  m_remote_journaler->shutdown();
  delete m_remote_journaler;
  m_remote_journaler = nullptr;

  delete m_replay_handler;
  m_replay_handler = nullptr;

  m_remote_ioctx.close();

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_STOPPING);

    m_state = STATE_STOPPED;
  }

  dout(20) << "done" << dendl;
}

int ImageReplayer::flush()
{
  // TODO: provide async method

  dout(20) << "enter" << dendl;

  {
    Mutex::Locker locker(m_lock);

    if (m_state != STATE_REPLAYING) {
      return 0;
    }

    m_state = STATE_FLUSHING_REPLAY;
  }

  C_SaferCond replay_flush_ctx;
  m_local_replay->flush(&replay_flush_ctx);
  int r = replay_flush_ctx.wait();
  if (r < 0) {
    derr << "error flushing local replay: " << cpp_strerror(r) << dendl;
  }

  C_SaferCond journaler_flush_ctx;
  m_remote_journaler->flush_commit_position(&journaler_flush_ctx);
  int r1 = journaler_flush_ctx.wait();
  if (r1 < 0) {
    derr << "error flushing remote journal commit position: "
	 << cpp_strerror(r1) << dendl;
  }

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_FLUSHING_REPLAY);

    m_state = STATE_REPLAYING;
  }

  dout(20) << "done" << dendl;

  return r < 0 ? r : r1;
}

void ImageReplayer::handle_replay_ready()
{
  dout(20) << "enter" << dendl;

  ::journal::ReplayEntry replay_entry;
  if (!m_remote_journaler->try_pop_front(&replay_entry)) {
    return;
  }

  dout(20) << "processing entry tid=" << replay_entry.get_commit_tid() << dendl;

  bufferlist data = replay_entry.get_data();
  bufferlist::iterator it = data.begin();
  Context *on_ready = create_context_callback<
    ImageReplayer, &ImageReplayer::handle_replay_process_ready>(this);
  Context *on_commit = new C_ReplayCommitted(this, std::move(replay_entry));
  m_local_replay->process(&it, on_ready, on_commit);
}

void ImageReplayer::handle_replay_process_ready(int r)
{
  // journal::Replay is ready for more events -- attempt to pop another

  dout(20) << "enter" << dendl;

  if (r < 0) {
    derr << "error replaying journal entry: " << cpp_strerror(r)
	 << dendl;
    // TODO: handle error
  }

  assert(r == 0);
  handle_replay_ready();
}

void ImageReplayer::handle_replay_complete(int r)
{
  dout(20) "r=" << r << dendl;

  //m_remote_journaler->stop_replay();
}

void ImageReplayer::handle_replay_committed(
  ::journal::ReplayEntry *replay_entry, int r)
{
  dout(20) << "commit_tid=" << replay_entry->get_commit_tid() << ", r=" << r
	   << dendl;

  m_remote_journaler->committed(*replay_entry);
}

int ImageReplayer::get_registered_client_status(bool *registered)
{
  dout(20) << "enter" << dendl;

  uint64_t minimum_set;
  uint64_t active_set;
  std::set<cls::journal::Client> registered_clients;
  C_SaferCond cond;
  m_remote_journaler->get_mutable_metadata(&minimum_set, &active_set,
					   &registered_clients, &cond);
  int r = cond.wait();
  if (r < 0) {
    derr << "error retrieving remote journal registered clients: "
	 << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto c : registered_clients) {
    if (c.id == m_client_id) {
      *registered = true;
      librbd::journal::ClientData client_data;
      bufferlist::iterator bl = c.data.begin();
      try {
	::decode(client_data, bl);
      } catch (const buffer::error &err) {
	derr << "failed to decode client meta data: " << err.what() << dendl;
	return -EINVAL;
      }
      librbd::journal::MirrorPeerClientMeta &cm =
	boost::get<librbd::journal::MirrorPeerClientMeta>(client_data.client_meta);
      m_local_image_id = cm.image_id;

      // TODO: snap name should be transient
      if (cm.sync_points.empty()) {
        return -ENOENT;
      }
      m_snap_name = cm.sync_points.front().snap_name;

      dout(20) << "client found, pool_id=" << m_local_pool_id << ", image_id="
	       << m_local_image_id << ", snap_name=" << m_snap_name << dendl;
      return 0;
    }
  }

  dout(20) << "client not found" << dendl;

  *registered = false;
  return 0;
}

int ImageReplayer::register_client()
{
  // TODO allocate snap as part of sync process
  std::string m_snap_name = ".rbd-mirror." + m_client_id;

  dout(20) << "mirror_uuid=" << m_client_id << ", "
           << "image_id=" << m_local_image_id << ", "
	   << "snap_name=" << m_snap_name << dendl;

  bufferlist client_data;
  ::encode(librbd::journal::ClientData{librbd::journal::MirrorPeerClientMeta{
    m_local_image_id, {{m_snap_name, boost::none}}}}, client_data);
  int r = m_remote_journaler->register_client(client_data);
  if (r < 0) {
    derr << "error registering client: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

int ImageReplayer::get_bootrstap_params(BootstrapParams *params)
{
  int r = librbd::cls_client::dir_get_name(&m_remote_ioctx, RBD_DIRECTORY,
					   m_remote_image_id,
					   &params->local_image_name);
  if (r < 0) {
    derr << "error looking up name for remote image id " << m_remote_image_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  params->local_pool_name = m_remote_ioctx.get_pool_name();

  return 0;
}

int ImageReplayer::bootstrap(const BootstrapParams *bootstrap_params)
{
  // Register client and sync images

  dout(20) << "enter" << dendl;

  int r;
  BootstrapParams params;

  if (bootstrap_params) {
    dout(20) << "using external bootstrap params" << dendl;
    params = *bootstrap_params;
  } else {
    r = get_bootrstap_params(&params);
    if (r < 0) {
      derr << "error obtaining bootrstap parameters: "
	   << cpp_strerror(r) << dendl;
      return r;
    }
  }

  dout(20) << "bootstrap params: local_pool_name=" << params.local_pool_name
	   << ", local_image_name=" << params.local_image_name << dendl;

  r = create_local_image(params);
  if (r < 0) {
    derr << "error creating local image " << params.local_image_name
	 << " in pool " << params.local_pool_name << ": " << cpp_strerror(r)
	 << dendl;
    return r;
  }

  r = register_client();
  if (r < 0) {
    derr << "error registering journal client: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = copy();
  if (r < 0) {
    derr << "error copying data to local image: " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(20) << "succeeded" << dendl;

  return 0;
}

int ImageReplayer::create_local_image(const BootstrapParams &bootstrap_params)
{
  dout(20) << "enter" << dendl;

  librbd::ImageCtx *image_ctx = new librbd::ImageCtx("", m_remote_image_id, nullptr,
						     m_remote_ioctx, true);
  int r = image_ctx->state->open();
  if (r < 0) {
    derr << "error opening remote image " << m_remote_image_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  uint64_t size = image_ctx->size;
  uint64_t features = image_ctx->features;
  int order = image_ctx->order;
  uint64_t stripe_unit = image_ctx->stripe_unit;
  uint64_t stripe_count = image_ctx->stripe_count;

  image_ctx->state->close();

  r = m_local->pool_lookup(bootstrap_params.local_pool_name.c_str());
  if (r < 0) {
    derr << "error finding local pool " << bootstrap_params.local_pool_name
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  m_local_pool_id = r;

  librados::IoCtx ioctx;
  r = m_local->ioctx_create2(m_local_pool_id, ioctx);
  if (r < 0) {
    derr << "error opening ioctx for local pool " << m_local_pool_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  r = librbd::create(ioctx, bootstrap_params.local_image_name.c_str(), size,
		     false, features, &order, stripe_unit, stripe_count);
  if (r < 0) {
    derr << "error creating local image " << m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  r = get_image_id(ioctx, bootstrap_params.local_image_name, &m_local_image_id);
  if (r < 0) {
    derr << "error resolving ID for local image " << m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(20) << "created, image_id=" << m_local_image_id << dendl;

  return 0;
}

int ImageReplayer::get_image_id(librados::IoCtx &ioctx,
				const std::string &image_name,
				std::string *image_id)
{
  librbd::ImageCtx *image_ctx = new librbd::ImageCtx(image_name, "", NULL,
                                                    ioctx, true);
  int r = image_ctx->state->open();
  if (r < 0) {
    derr << "error opening remote image " << image_name
	 << ": " << cpp_strerror(r) << dendl;
    delete image_ctx;
    return r;
  }

  *image_id = image_ctx->id;
  image_ctx->state->close();
  return 0;
}

int ImageReplayer::copy()
{
  dout(20) << m_remote_pool_id << "/" << m_remote_image_id << "->"
	   << m_local_pool_id << "/" << m_local_image_id << dendl;

  librados::IoCtx local_ioctx;
  librbd::ImageCtx *remote_image_ctx, *local_image_ctx;
  librbd::NoOpProgressContext prog_ctx;
  int r;

  remote_image_ctx = new librbd::ImageCtx("", m_remote_image_id, nullptr,
					  m_remote_ioctx, false);
  r = remote_image_ctx->state->open();
  if (r < 0) {
    derr << "error opening remote image " << m_remote_image_id
	 << ": " << cpp_strerror(r) << dendl;
    delete remote_image_ctx;
    return r;
  }

  dout(20) << "creating temporary snapshot " << m_snap_name << dendl;

  // TODO: use internal snapshots
  r = remote_image_ctx->operations->snap_create(m_snap_name.c_str());
  if (r == -EEXIST) {
    // Probably left after a previous unsuccessful bootsrapt.
    dout(0) << "removing stale snapshot " << m_snap_name << " of remote image "
	    << m_remote_image_id << dendl;
    (void)remote_image_ctx->operations->snap_remove(m_snap_name.c_str());
    r = remote_image_ctx->operations->snap_create(m_snap_name.c_str());
  }
  if (r < 0) {
    derr << "error creating snapshot " << m_snap_name << " of remote image "
	 << m_remote_image_id << ": " << cpp_strerror(r) << dendl;
    goto cleanup;
  }

  remote_image_ctx->state->close();
  remote_image_ctx = new librbd::ImageCtx("", m_remote_image_id,
					  m_snap_name.c_str(), m_remote_ioctx,
					  true);
  r = remote_image_ctx->state->open();
  if (r < 0) {
    derr << "error opening snapshot " << m_snap_name << " of remote image "
	 << m_remote_image_id << ": " << cpp_strerror(r) << dendl;
    delete remote_image_ctx;
    remote_image_ctx = nullptr;
    goto cleanup;
  }

  r = m_local->ioctx_create2(m_local_pool_id, local_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for local pool " << m_local_pool_id
	 << ": " << cpp_strerror(r) << dendl;
    goto cleanup;
  }

  local_image_ctx = new librbd::ImageCtx("", m_local_image_id, nullptr,
					 local_ioctx, false);
  r = local_image_ctx->state->open();
  if (r < 0) {
    derr << "error opening local image " << m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    delete local_image_ctx;
    local_image_ctx = nullptr;
    goto cleanup;
  }

  dout(20) << "copying" << dendl;

  // TODO: show copy progress in image replay status
  r = librbd::copy(remote_image_ctx, local_image_ctx, prog_ctx);
  if (r < 0) {
    derr << "error copying snapshot " << m_snap_name << " of remote image "
	 << m_remote_image_id << " to local image " << m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
  }

  local_image_ctx->state->close();
  local_image_ctx = nullptr;

  remote_image_ctx->state->close();
  remote_image_ctx = nullptr;

  dout(20) << "done" << dendl;

cleanup:
  if (local_image_ctx) {
    local_image_ctx->state->close();
  }
  if (remote_image_ctx) {
    remote_image_ctx->state->close();
  }
  remote_image_ctx = new librbd::ImageCtx("", m_remote_image_id, nullptr,
					  m_remote_ioctx, false);
  int r1 = remote_image_ctx->state->open();
  if (r1 < 0) {
    derr << "error opening remote image " << m_remote_image_id
	 << ": " << cpp_strerror(r1) << dendl;
    delete remote_image_ctx;
  } else {
    dout(20) << "removing temporary snapshot " << m_snap_name << dendl;
    r1 = remote_image_ctx->operations->snap_remove(m_snap_name.c_str());
    if (r1 < 0) {
      derr << "error removing snapshot " << m_snap_name << " of remote image "
	   << m_remote_image_id << ": " << cpp_strerror(r1) << dendl;
    }
    remote_image_ctx->state->close();
  }

  return r;
}

void ImageReplayer::shut_down_journal_replay(bool cancel_ops)
{
  C_SaferCond cond;
  m_local_replay->shut_down(cancel_ops, &cond);
  int r = cond.wait();
  if (r < 0) {
    derr << "error flushing journal replay: " << cpp_strerror(r) << dendl;
  }
}

std::ostream &operator<<(std::ostream &os, const ImageReplayer::State &state)
{
  switch (state) {
  case ImageReplayer::STATE_UNINITIALIZED:
    os << "Uninitialized";
    break;
  case ImageReplayer::STATE_STARTING:
    os << "Starting";
    break;
  case ImageReplayer::STATE_REPLAYING:
    os << "Replaying";
    break;
  case ImageReplayer::STATE_FLUSHING_REPLAY:
    os << "FlushingReplay";
    break;
  case ImageReplayer::STATE_STOPPING:
    os << "Stopping";
    break;
  case ImageReplayer::STATE_STOPPED:
    os << "Stopped";
    break;
  default:
    os << "Unknown(" << state << ")";
    break;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const ImageReplayer &replayer)
{
  os << "ImageReplayer[" << replayer.m_remote_pool_id << "/"
     << replayer.m_remote_image_id << "]";
  return os;
}

} // namespace mirror
} // namespace rbd
