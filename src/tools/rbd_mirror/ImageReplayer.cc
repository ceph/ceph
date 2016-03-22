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

template <typename I>
std::ostream &operator<<(std::ostream &os,
                         const typename ImageReplayer<I>::State &state);

namespace {

template <typename I>
struct ReplayHandler : public ::journal::ReplayHandler {
  ImageReplayer<I> *replayer;
  ReplayHandler(ImageReplayer<I> *replayer) : replayer(replayer) {}

  virtual void get() {}
  virtual void put() {}

  virtual void handle_entries_available() {
    replayer->handle_replay_ready();
  }
  virtual void handle_complete(int r) {
    replayer->handle_replay_complete(r);
  }
};

template <typename I>
struct C_ReplayCommitted : public Context {
  typedef typename librbd::journal::TypeTraits<I>::ReplayEntry ReplayEntry;

  ImageReplayer<I> *replayer;
  ReplayEntry replay_entry;

  C_ReplayCommitted(ImageReplayer<I> *replayer,
                    ReplayEntry &&replay_entry)
    : replayer(replayer), replay_entry(std::move(replay_entry)) {
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

template <typename I>
class StatusCommand : public ImageReplayerAdminSocketCommand {
public:
  explicit StatusCommand(ImageReplayer<I> *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    replayer->print_status(f, ss);
    return true;
  }

private:
  ImageReplayer<I> *replayer;
};

template <typename I>
class FlushCommand : public ImageReplayerAdminSocketCommand {
public:
  explicit FlushCommand(ImageReplayer<I> *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    C_SaferCond cond;
    replayer->flush(&cond);
    int r = cond.wait();
    if (r < 0) {
      *ss << "flush: " << cpp_strerror(r);
      return false;
    }
    return true;
  }

private:
  ImageReplayer<I> *replayer;
};

template <typename I>
class ImageReplayerAdminSocketHook : public AdminSocketHook {
public:
  ImageReplayerAdminSocketHook(CephContext *cct, const std::string &name,
			       ImageReplayer<I> *replayer) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status " + name;
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StatusCommand<I>(replayer);
    }

    command = "rbd mirror flush " + name;
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror " + name);
    if (r == 0) {
      commands[command] = new FlushCommand<I>(replayer);
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

} // anonymous namespace

template <typename I>
ImageReplayer<I>::ImageReplayer(Threads *threads, RadosRef local, RadosRef remote,
			     const std::string &mirror_uuid,
			     int64_t local_pool_id,
			     int64_t remote_pool_id,
			     const std::string &remote_image_id) :
  m_threads(threads),
  m_local(local),
  m_remote(remote),
  m_mirror_uuid(mirror_uuid),
  m_remote_pool_id(remote_pool_id),
  m_local_pool_id(local_pool_id),
  m_remote_image_id(remote_image_id),
  m_name(stringify(remote_pool_id) + "/" + remote_image_id),
  m_lock("rbd::mirror::ImageReplayer " + stringify(remote_pool_id) + " " +
	 remote_image_id),
  m_state(STATE_UNINITIALIZED),
  m_local_image_ctx(nullptr),
  m_local_replay(nullptr),
  m_remote_journaler(nullptr),
  m_replay_handler(nullptr),
  m_on_finish(nullptr),
  m_asok_hook(nullptr)
{
}

template <typename I>
ImageReplayer<I>::~ImageReplayer()
{
  assert(m_local_image_ctx == nullptr);
  assert(m_local_replay == nullptr);
  assert(m_remote_journaler == nullptr);
  assert(m_replay_handler == nullptr);

  delete m_asok_hook;
}

template <typename I>
void ImageReplayer<I>::start(Context *on_finish,
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
    m_local_image_name = bootstrap_params->local_image_name;
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
  m_remote_journaler = new Journaler(m_threads->work_queue,
                                     m_threads->timer,
				     &m_threads->timer_lock, m_remote_ioctx,
				     m_remote_image_id, m_mirror_uuid,
                                     commit_interval);

  bootstrap();
}

template <typename I>
void ImageReplayer<I>::bootstrap() {
  dout(20) << "bootstrap params: "
	   << "local_image_name=" << m_local_image_name << dendl;

  // TODO: add a new bootstrap state and support canceling
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_bootstrap>(this);
  BootstrapRequest<I> *request = BootstrapRequest<I>::create(
    m_local_ioctx, m_remote_ioctx, &m_local_image_ctx,
    m_local_image_name, m_remote_image_id, m_threads->work_queue,
    m_threads->timer, &m_threads->timer_lock, m_mirror_uuid, m_remote_journaler,
    &m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageReplayer<I>::handle_bootstrap(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    on_start_fail_start(r);
    return;
  } else if (on_start_interrupted()) {
    return;
  }

  {
    Mutex::Locker locker(m_lock);
    m_name = m_local_ioctx.get_pool_name() + "/" + m_local_image_ctx->name;

    CephContext *cct = static_cast<CephContext *>(m_local->cct());
    delete m_asok_hook;
    m_asok_hook = new ImageReplayerAdminSocketHook<I>(cct, m_name, this);
  }

  init_remote_journaler();
}

template <typename I>
void ImageReplayer<I>::init_remote_journaler() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_init_remote_journaler>(this);
  m_remote_journaler->init(ctx);
}

template <typename I>
void ImageReplayer<I>::handle_init_remote_journaler(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to initialize remote journal: " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  } else if (on_start_interrupted()) {
    return;
  }

  start_replay();
}

template <typename I>
void ImageReplayer<I>::start_replay() {
  dout(20) << dendl;

  int r = m_local_image_ctx->journal->start_external_replay(&m_local_replay);
  if (r < 0) {
    derr << "error starting external replay on local image "
	 <<  m_local_image_id << ": " << cpp_strerror(r) << dendl;
    on_start_fail_start(r);
    return;
  }

  m_replay_handler = new ReplayHandler<I>(this);
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

template <typename I>
void ImageReplayer<I>::on_start_fail_start(int r)
{
  dout(20) << "r=" << r << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this, r](int r1) {
      assert(r1 == 0);
      on_start_fail_finish(r);
    });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void ImageReplayer<I>::on_start_fail_finish(int r)
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
    // TODO: switch to async close via CloseImageRequest
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

template <typename I>
bool ImageReplayer<I>::on_start_interrupted()
{
  Mutex::Locker locker(m_lock);

  if (m_state == STATE_STARTING) {
    return false;
  }

  assert(m_state == STATE_STOPPING);

  on_start_fail_start(-EINTR);
  return true;
}

template <typename I>
void ImageReplayer<I>::stop(Context *on_finish)
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
  } else if (m_state == STATE_FLUSHING_REPLAY) {
    dout(20) << "interrupting flush" << dendl;

    if (on_finish) {
      Context *on_flush_finish = m_on_finish;
      FunctionContext *ctx = new FunctionContext(
	[this, on_flush_finish, on_finish](int r) {
	  if (on_flush_finish) {
	    on_flush_finish->complete(r);
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

template <typename I>
void ImageReplayer<I>::on_stop_journal_replay_shut_down_start()
{
  dout(20) << "enter" << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this](int r) {
      on_stop_journal_replay_shut_down_finish(r);
    });

  m_local_replay->shut_down(false, ctx);
}

template <typename I>
void ImageReplayer<I>::on_stop_journal_replay_shut_down_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error flushing journal replay: " << cpp_strerror(r) << dendl;
  }

  m_local_image_ctx->journal->stop_external_replay();
  m_local_replay = nullptr;

  on_stop_local_image_close_start();
}

template <typename I>
void ImageReplayer<I>::on_stop_local_image_close_start()
{
  dout(20) << "enter" << dendl;

  // close and delete the image (from outside the image's thread context)
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::on_stop_local_image_close_finish>(this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_local_image_ctx, m_threads->work_queue, false, ctx);
  request->send();
}

template <typename I>
void ImageReplayer<I>::on_stop_local_image_close_finish(int r)
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

template <typename I>
void ImageReplayer<I>::close_local_image(Context *on_finish)
{
  m_local_image_ctx->state->close(on_finish);
}

template <typename I>
void ImageReplayer<I>::handle_replay_ready()
{
  dout(20) << "enter" << dendl;

  ReplayEntry replay_entry;
  if (!m_remote_journaler->try_pop_front(&replay_entry)) {
    return;
  }

  dout(20) << "processing entry tid=" << replay_entry.get_commit_tid() << dendl;

  bufferlist data = replay_entry.get_data();
  bufferlist::iterator it = data.begin();
  Context *on_ready = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_replay_process_ready>(this);
  Context *on_commit = new C_ReplayCommitted<I>(this, std::move(replay_entry));
  m_local_replay->process(&it, on_ready, on_commit);
}

template <typename I>
void ImageReplayer<I>::flush(Context *on_finish)
{
  dout(20) << "enter" << dendl;

  bool start_flush = false;

  {
    Mutex::Locker locker(m_lock);

    if (m_state == STATE_REPLAYING) {
      assert(m_on_finish == nullptr);
      m_on_finish = on_finish;

      m_state = STATE_FLUSHING_REPLAY;

      start_flush = true;
    }
  }

  if (start_flush) {
    on_flush_local_replay_flush_start();
  } else if (on_finish) {
    on_finish->complete(0);
  }
}

template <typename I>
void ImageReplayer<I>::on_flush_local_replay_flush_start()
{
  dout(20) << "enter" << dendl;

  FunctionContext *ctx = new FunctionContext(
    [this](int r) {
      on_flush_local_replay_flush_finish(r);
    });

  m_local_replay->flush(ctx);
}

template <typename I>
void ImageReplayer<I>::on_flush_local_replay_flush_finish(int r)
{
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error flushing local replay: " << cpp_strerror(r) << dendl;
  }

  if (on_flush_interrupted()) {
    return;
  }

  on_flush_flush_commit_position_start(r);
}

template <typename I>
void ImageReplayer<I>::on_flush_flush_commit_position_start(int last_r)
{

  FunctionContext *ctx = new FunctionContext(
    [this, last_r](int r) {
      on_flush_flush_commit_position_finish(last_r, r);
    });

  m_remote_journaler->flush_commit_position(ctx);
}

template <typename I>
void ImageReplayer<I>::on_flush_flush_commit_position_finish(int last_r, int r)
{
  if (r < 0) {
    derr << "error flushing remote journal commit position: "
	 << cpp_strerror(r) << dendl;
  } else {
    r = last_r;
  }

  Context *on_finish(nullptr);

  {
    Mutex::Locker locker(m_lock);
    if (m_state == STATE_STOPPING) {
      r = -EINTR;
    } else {
      assert(m_state == STATE_FLUSHING_REPLAY);

      m_state = STATE_REPLAYING;
    }
    std::swap(m_on_finish, on_finish);
  }

  dout(20) << "flush complete, r=" << r << dendl;

  if (on_finish) {
    dout(20) << "on finish complete, r=" << r << dendl;
    on_finish->complete(r);
  }
}

template <typename I>
bool ImageReplayer<I>::on_flush_interrupted()
{
  Context *on_finish(nullptr);

  {
    Mutex::Locker locker(m_lock);

    if (m_state == STATE_FLUSHING_REPLAY) {
      return false;
    }

    assert(m_state == STATE_STOPPING);

    std::swap(m_on_finish, on_finish);
  }

  dout(20) << "flush interrupted" << dendl;

  if (on_finish) {
    int r = -EINTR;
    dout(20) << "on finish complete, r=" << r << dendl;
    on_finish->complete(r);
  }

  return true;
}

template <typename I>
void ImageReplayer<I>::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (f) {
    f->open_object_section("image_replayer");
    f->dump_string("name", m_name);
    f->dump_stream("state") << m_state;
    f->close_section();
    f->flush(*ss);
  } else {
    *ss << m_name << ": state: " << m_state;
  }
}

template <typename I>
void ImageReplayer<I>::handle_replay_process_ready(int r)
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

template <typename I>
void ImageReplayer<I>::handle_replay_complete(int r)
{
  dout(20) "r=" << r << dendl;

  //m_remote_journaler->stop_replay();
}

template <typename I>
void ImageReplayer<I>::handle_replay_committed(ReplayEntry *replay_entry, int r)
{
  dout(20) << "commit_tid=" << replay_entry->get_commit_tid() << ", r=" << r
	   << dendl;

  m_remote_journaler->committed(*replay_entry);
}

template <typename I>
void ImageReplayer<I>::shut_down_journal_replay(bool cancel_ops)
{
  C_SaferCond cond;
  m_local_replay->shut_down(cancel_ops, &cond);
  int r = cond.wait();
  if (r < 0) {
    derr << "error flushing journal replay: " << cpp_strerror(r) << dendl;
  }
}

template <typename I>
std::ostream &operator<<(std::ostream &os,
                         const typename ImageReplayer<I>::State &state)
{
  switch (state) {
  case ImageReplayer<I>::STATE_UNINITIALIZED:
    os << "Uninitialized";
    break;
  case ImageReplayer<I>::STATE_STARTING:
    os << "Starting";
    break;
  case ImageReplayer<I>::STATE_REPLAYING:
    os << "Replaying";
    break;
  case ImageReplayer<I>::STATE_FLUSHING_REPLAY:
    os << "FlushingReplay";
    break;
  case ImageReplayer<I>::STATE_STOPPING:
    os << "Stopping";
    break;
  case ImageReplayer<I>::STATE_STOPPED:
    os << "Stopped";
    break;
  default:
    os << "Unknown(" << state << ")";
    break;
  }
  return os;
}

template <typename I>
std::ostream &operator<<(std::ostream &os, const ImageReplayer<I> &replayer)
{
  os << "ImageReplayer[" << replayer.get_remote_pool_id() << "/"
     << replayer.get_remote_image_id() << "]";
  return os;
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;
