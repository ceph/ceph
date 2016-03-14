// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
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
#include "ImageSync.h"
#include "Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"

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
using namespace rbd::mirror::image_replayer;

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

ImageReplayer::ImageReplayer(Threads *threads, RadosRef local, RadosRef remote,
			     const std::string &client_id,
			     int64_t local_pool_id,
			     int64_t remote_pool_id,
			     const std::string &remote_image_id) :
  m_threads(threads),
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
  m_replay_handler(nullptr),
  m_on_finish(nullptr)
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

void ImageReplayer::start(Context *on_finish,
			  const BootstrapParams *bootstrap_params)
{
  dout(20) << "on_finish=" << on_finish << ", m_on_finish=" << m_on_finish
	   << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(is_stopped_());

    m_state = STATE_STARTING;

    assert(m_on_finish == nullptr);
    m_on_finish = on_finish;
  }

  int r = m_remote->ioctx_create2(m_remote_pool_id, m_remote_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for remote pool " << m_remote_pool_id
	 << ": " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }

  if (bootstrap_params != nullptr && !bootstrap_params->empty()) {
    r = m_local->pool_lookup(bootstrap_params->local_pool_name.c_str());
    if (r < 0) {
      derr << "error finding local pool " << bootstrap_params->local_pool_name
           << ": " << cpp_strerror(r) << dendl;
      on_start_fail_start(r);
      return;
    }
    m_local_pool_id = r;
  }

  r = m_local->ioctx_create2(m_local_pool_id, m_local_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for local pool " << m_local_pool_id
         << ": " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }

  CephContext *cct = static_cast<CephContext *>(m_local->cct());

  double commit_interval = cct->_conf->rbd_journal_commit_age;
  m_remote_journaler = new ::journal::Journaler(m_threads->work_queue,
						m_threads->timer,
						&m_threads->timer_lock,
						m_remote_ioctx,
						m_remote_image_id, m_client_id,
						commit_interval);

  on_start_get_registered_client_status_start(bootstrap_params);
}

void ImageReplayer::on_start_get_registered_client_status_start(
  const BootstrapParams *bootstrap_params)
{
  dout(20) << "enter" << dendl;

  struct Metadata {
    uint64_t minimum_set;
    uint64_t active_set;
    std::set<cls::journal::Client> registered_clients;
    BootstrapParams bootstrap_params;
  } *m = new Metadata();

  if (bootstrap_params) {
    m->bootstrap_params = *bootstrap_params;
  }

  FunctionContext *ctx = new FunctionContext(
    [this, m, bootstrap_params](int r) {
      on_start_get_registered_client_status_finish(r, m->registered_clients,
						   m->bootstrap_params);
      delete m;
    });

  m_remote_journaler->get_mutable_metadata(&m->minimum_set, &m->active_set,
					   &m->registered_clients, ctx);
}

void ImageReplayer::on_start_get_registered_client_status_finish(int r,
  const std::set<cls::journal::Client> &registered_clients,
  const BootstrapParams &bootstrap_params)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error obtaining registered client status: "
	 << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }
  if (on_start_interrupted()) {
    return;
  }

  for (auto c : registered_clients) {
    if (c.id == m_client_id) {
      librbd::journal::ClientData client_data;
      bufferlist::iterator bl = c.data.begin();
      try {
	::decode(client_data, bl);
      } catch (const buffer::error &err) {
	derr << "failed to decode client meta data: " << err.what() << dendl;
	on_start_fail_start(-EINVAL);
	return;
      }

      // TODO: unsafe cast
      m_client_meta =
	boost::get<librbd::journal::MirrorPeerClientMeta>(client_data.client_meta);
      m_local_image_id = m_client_meta.image_id;

      dout(20) << "client found, pool_id=" << m_local_pool_id << ", image_id="
	       << m_local_image_id << dendl;

      if (!bootstrap_params.empty()) {
	dout(0) << "ignoring bootsrap params: client already registered" << dendl;
      }

      on_start_remote_journaler_init_start();
      return;
    }
  }

  dout(20) << "client not found" << dendl;
  bootstrap(bootstrap_params);
}

void ImageReplayer::bootstrap(const BootstrapParams &bootstrap_params) {
  int r;
  BootstrapParams params;

  if (!bootstrap_params.empty()) {
    dout(20) << "using external bootstrap params" << dendl;
    params = bootstrap_params;
  } else {
    r = get_bootstrap_params(&params);
    if (r < 0) {
      derr << "error obtaining bootstrap parameters: "
	   << cpp_strerror(r) << dendl;
      on_start_fail_start(r);
      return;
    }
  }

  dout(20) << "bootstrap params: "
           << "local_pool_name=" << params.local_pool_name << ", "
	   << "local_image_name=" << params.local_image_name << dendl;

  // TODO: add a new bootstrap state and support canceling
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer::handle_bootstrap>(this);
  BootstrapRequest<> *request = BootstrapRequest<>::create(
    m_local_ioctx, m_remote_ioctx, &m_local_image_ctx,
    params.local_image_name, m_remote_image_id, m_threads->work_queue,
    m_threads->timer, &m_threads->timer_lock, m_client_id, m_remote_journaler,
    &m_client_meta, ctx);
  request->send();
}

void ImageReplayer::handle_bootstrap(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    on_start_fail_start(r);
    return;
  }
  if (on_start_interrupted()) {
    return;
  }

  on_start_remote_journaler_init_start();
}

void ImageReplayer::on_start_remote_journaler_init_start()
{
  if (on_start_interrupted()) {
    return;
  }

  dout(20) << "enter" << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this](int r) {
      on_start_remote_journaler_init_finish(r);
    });

  m_remote_journaler->init(ctx);
}

void ImageReplayer::on_start_remote_journaler_init_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error initializing journal: " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }
  if (on_start_interrupted()) {
    return;
  }


  on_start_local_image_open_start();
}

void ImageReplayer::on_start_local_image_open_start()
{
  dout(20) << "enter" << dendl;
  if (m_local_image_ctx != nullptr) {
    // already opened during bootstrap
    on_start_wait_for_local_journal_ready_start();
    return;
  }

  // open and lock the local image
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer::on_start_local_image_open_finish>(this);
  OpenLocalImageRequest<> *request = OpenLocalImageRequest<>::create(
    m_local_ioctx, &m_local_image_ctx, "", m_local_image_id,
    m_threads->work_queue, ctx);
  request->send();
}

void ImageReplayer::on_start_local_image_open_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error opening local image " <<  m_local_image_id
	 << ": " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }
  if (on_start_interrupted()) {
    return;
  }

  on_start_wait_for_local_journal_ready_start();
}

void ImageReplayer::on_start_wait_for_local_journal_ready_start()
{
  dout(20) << "enter" << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this](int r) {
      on_start_wait_for_local_journal_ready_finish(r);
    });
  m_local_image_ctx->journal->wait_for_journal_ready(ctx);
}

void ImageReplayer::on_start_wait_for_local_journal_ready_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error when waiting for local journal ready: " << cpp_strerror(r)
	 << dendl;
    on_start_fail_start(r);
    return;
  }
  if (on_start_interrupted()) {
    return;
  }

  r = m_local_image_ctx->journal->start_external_replay(&m_local_replay);
  if (r < 0) {
    derr << "error starting external replay on local image "
	 <<  m_local_image_id << ": " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }

  m_replay_handler = new ReplayHandler(this);

  m_remote_journaler->start_live_replay(m_replay_handler,
					1 /* TODO: configurable */);

  dout(20) << "m_remote_journaler=" << *m_remote_journaler << dendl;

  assert(r == 0);

  Context *on_finish(nullptr);

  {
    Mutex::Locker locker(m_lock);

    if (m_state == STATE_STOPPING) {
      on_start_fail_start(-EINTR);
      return;
    }

    assert(m_state == STATE_STARTING);
    m_state = STATE_REPLAYING;

    std::swap(m_on_finish, on_finish);
  }

  dout(20) << "start succeeded" << dendl;

  if (on_finish) {
    dout(20) << "on finish complete, r=" << r << dendl;
    on_finish->complete(r);
  }
}

void ImageReplayer::on_start_fail_start(int r)
{
  dout(20) << "r=" << r << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this, r](int r1) {
      assert(r1 == 0);
      on_start_fail_finish(r);
    });

  m_threads->work_queue->queue(ctx, 0);
}

void ImageReplayer::on_start_fail_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (m_remote_journaler) {
    if (m_remote_journaler->is_initialized()) {
      m_remote_journaler->shut_down();
    }
    delete m_remote_journaler;
    m_remote_journaler = nullptr;
  }

  if (m_local_replay) {
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

  Context *on_finish(nullptr);

  {
    Mutex::Locker locker(m_lock);
    if (m_state == STATE_STOPPING) {
      assert(r == -EINTR);
      dout(20) << "start interrupted" << dendl;
      m_state = STATE_STOPPED;
    } else {
      assert(m_state == STATE_STARTING);
      dout(20) << "start failed" << dendl;
      m_state = STATE_UNINITIALIZED;
    }
    std::swap(m_on_finish, on_finish);
  }

  if (on_finish) {
    dout(20) << "on finish complete, r=" << r << dendl;
    on_finish->complete(r);
  }
}

bool ImageReplayer::on_start_interrupted()
{
  Mutex::Locker locker(m_lock);

  if (m_state == STATE_STARTING) {
    return false;
  }

  assert(m_state == STATE_STOPPING);

  on_start_fail_start(-EINTR);
  return true;
}

void ImageReplayer::stop(Context *on_finish)
{
  dout(20) << "on_finish=" << on_finish << ", m_on_finish=" << m_on_finish
	   << dendl;

  Mutex::Locker locker(m_lock);
  assert(is_running_());

  if (m_state == STATE_STARTING) {
    dout(20) << "interrupting start" << dendl;

    if (on_finish) {
      Context *on_start_finish = m_on_finish;
      FunctionContext *ctx = new FunctionContext(
	[this, on_start_finish, on_finish](int r) {
	  if (on_start_finish) {
	    on_start_finish->complete(r);
	  }
	  on_finish->complete(0);
	});

      m_on_finish = ctx;
    }
  } else {
    assert(m_on_finish == nullptr);
    m_on_finish = on_finish;
    on_stop_journal_replay_shut_down_start();
  }
  m_state = STATE_STOPPING;
}

void ImageReplayer::on_stop_journal_replay_shut_down_start()
{
  dout(20) << "enter" << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this](int r) {
      on_stop_journal_replay_shut_down_finish(r);
    });

  m_local_replay->shut_down(false, ctx);
}

void ImageReplayer::on_stop_journal_replay_shut_down_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error flushing journal replay: " << cpp_strerror(r) << dendl;
  }

  m_local_image_ctx->journal->stop_external_replay();
  m_local_replay = nullptr;

  on_stop_local_image_close_start();
}

void ImageReplayer::on_stop_local_image_close_start()
{
  dout(20) << "enter" << dendl;

  // close and delete the image (from outside the image's thread context)
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer::on_stop_local_image_close_finish>(this);
  CloseImageRequest<> *request = CloseImageRequest<>::create(
    &m_local_image_ctx, m_threads->work_queue, ctx);
  request->send();
}

void ImageReplayer::on_stop_local_image_close_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error closing local image: " << cpp_strerror(r) << dendl;
  }

  m_local_ioctx.close();

  m_remote_journaler->stop_replay();
  m_remote_journaler->shut_down();
  delete m_remote_journaler;
  m_remote_journaler = nullptr;

  delete m_replay_handler;
  m_replay_handler = nullptr;

  m_remote_ioctx.close();

  Context *on_finish(nullptr);

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_STOPPING);

    m_state = STATE_STOPPED;

    std::swap(m_on_finish, on_finish);
  }

  dout(20) << "stop complete" << dendl;

  if (on_finish) {
    dout(20) << "on finish complete, r=" << r << dendl;
    on_finish->complete(r);
  }
}

void ImageReplayer::close_local_image(Context *on_finish)
{
  m_local_image_ctx->state->close(on_finish);
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

int ImageReplayer::get_bootstrap_params(BootstrapParams *params)
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
