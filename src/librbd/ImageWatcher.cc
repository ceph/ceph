// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ImageWatcher.h"
#include "librbd/AioCompletion.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/TaskFinisher.h"
#include "librbd/Utils.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "common/errno.h"
#include <sstream>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageWatcher: "

namespace librbd {

using namespace watch_notify;

static const uint64_t	NOTIFY_TIMEOUT = 5000;
static const double	RETRY_DELAY_SECONDS = 1.0;

ImageWatcher::ImageWatcher(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx),
    m_watch_lock(util::unique_lock_name("librbd::ImageWatcher::m_watch_lock", this)),
    m_watch_ctx(*this), m_watch_handle(0),
    m_watch_state(WATCH_STATE_UNREGISTERED),
    m_task_finisher(new TaskFinisher<Task>(*m_image_ctx.cct)),
    m_async_request_lock(util::unique_lock_name("librbd::ImageWatcher::m_async_request_lock", this)),
    m_owner_client_id_lock(util::unique_lock_name("librbd::ImageWatcher::m_owner_client_id_lock", this))
{
}

ImageWatcher::~ImageWatcher()
{
  delete m_task_finisher;
  {
    RWLock::RLocker l(m_watch_lock);
    assert(m_watch_state != WATCH_STATE_REGISTERED);
  }
}

int ImageWatcher::register_watch() {
  ldout(m_image_ctx.cct, 10) << this << " registering image watcher" << dendl;

  RWLock::WLocker l(m_watch_lock);
  assert(m_watch_state == WATCH_STATE_UNREGISTERED);
  int r = m_image_ctx.md_ctx.watch2(m_image_ctx.header_oid,
				    &m_watch_handle,
				    &m_watch_ctx);
  if (r < 0) {
    return r;
  }

  m_watch_state = WATCH_STATE_REGISTERED;
  return 0;
}

int ImageWatcher::unregister_watch() {
  ldout(m_image_ctx.cct, 10) << this << " unregistering image watcher" << dendl;

  cancel_async_requests();
  m_task_finisher->cancel_all();

  int r = 0;
  {
    RWLock::WLocker l(m_watch_lock);
    if (m_watch_state == WATCH_STATE_REGISTERED) {
      r = m_image_ctx.md_ctx.unwatch2(m_watch_handle);
    }
    m_watch_state = WATCH_STATE_UNREGISTERED;
  }

  librados::Rados rados(m_image_ctx.md_ctx);
  rados.watch_flush();
  return r;
}

void ImageWatcher::schedule_async_progress(const AsyncRequestId &request,
					   uint64_t offset, uint64_t total) {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_async_progress, this, request, offset,
                total));
  m_task_finisher->queue(Task(TASK_CODE_ASYNC_PROGRESS, request), ctx);
}

int ImageWatcher::notify_async_progress(const AsyncRequestId &request,
					uint64_t offset, uint64_t total) {
  ldout(m_image_ctx.cct, 20) << this << " remote async request progress: "
			     << request << " @ " << offset
			     << "/" << total << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(AsyncProgressPayload(request, offset, total)), bl);

  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return 0;
}

void ImageWatcher::schedule_async_complete(const AsyncRequestId &request,
					   int r) {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_async_complete, this, request, r));
  m_task_finisher->queue(ctx);
}

int ImageWatcher::notify_async_complete(const AsyncRequestId &request,
					int r) {
  ldout(m_image_ctx.cct, 20) << this << " remote async request finished: "
			     << request << " = " << r << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(AsyncCompletePayload(request, r)), bl);

  if (r >= 0) {
    librbd::notify_change(m_image_ctx.md_ctx, m_image_ctx.header_oid,
			  &m_image_ctx);
  }
  int ret = m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl,
				       NOTIFY_TIMEOUT, NULL);
  if (ret < 0) {
    lderr(m_image_ctx.cct) << this << " failed to notify async complete: "
			   << cpp_strerror(ret) << dendl;
    if (ret == -ETIMEDOUT) {
      schedule_async_complete(request, r);
    }
  } else {
    RWLock::WLocker l(m_async_request_lock);
    m_async_pending.erase(request);
  }
  return 0;
}

int ImageWatcher::notify_flatten(uint64_t request_id, ProgressContext &prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(FlattenPayload(async_request_id)), bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_resize(uint64_t request_id, uint64_t size,
				ProgressContext &prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(ResizePayload(size, async_request_id)), bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_snap_create(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapCreatePayload(snap_name)), bl);

  return notify_lock_owner(bl);
}

int ImageWatcher::notify_snap_rename(const snapid_t &src_snap_id,
				     const std::string &dst_snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapRenamePayload(src_snap_id, dst_snap_name)), bl);

  return notify_lock_owner(bl);
}
int ImageWatcher::notify_snap_remove(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapRemovePayload(snap_name)), bl);

  return notify_lock_owner(bl);
}

int ImageWatcher::notify_snap_protect(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapProtectPayload(snap_name)), bl);
  return notify_lock_owner(bl);
}

int ImageWatcher::notify_snap_unprotect(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapUnprotectPayload(snap_name)), bl);
  return notify_lock_owner(bl);
}

int ImageWatcher::notify_rebuild_object_map(uint64_t request_id,
                                            ProgressContext &prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(RebuildObjectMapPayload(async_request_id)), bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_rename(const std::string &image_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(RenamePayload(image_name)), bl);
  return notify_lock_owner(bl);
}

void ImageWatcher::notify_header_update(librados::IoCtx &io_ctx,
				        const std::string &oid)
{
  // supports legacy (empty buffer) clients
  bufferlist bl;
  ::encode(NotifyMessage(HeaderUpdatePayload()), bl);

  io_ctx.notify2(oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::schedule_cancel_async_requests() {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::cancel_async_requests, this));
  m_task_finisher->queue(TASK_CODE_CANCEL_ASYNC_REQUESTS, ctx);
}

void ImageWatcher::cancel_async_requests() {
  RWLock::WLocker l(m_async_request_lock);
  for (std::map<AsyncRequestId, AsyncRequest>::iterator iter =
	 m_async_requests.begin();
       iter != m_async_requests.end(); ++iter) {
    iter->second.first->complete(-ERESTART);
  }
  m_async_requests.clear();
}

void ImageWatcher::set_owner_client_id(const ClientId& client_id) {
  assert(m_owner_client_id_lock.is_locked());
  m_owner_client_id = client_id;
  ldout(m_image_ctx.cct, 10) << this << " current lock owner: "
                             << m_owner_client_id << dendl;
}

ClientId ImageWatcher::get_client_id() {
  RWLock::RLocker l(m_watch_lock);
  return ClientId(m_image_ctx.md_ctx.get_instance_id(), m_watch_handle);
}

void ImageWatcher::notify_acquired_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify acquired lock" << dendl;

  ClientId client_id = get_client_id();
  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(client_id);
  }

  bufferlist bl;
  ::encode(NotifyMessage(AcquiredLockPayload(client_id)), bl);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::notify_released_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify released lock" << dendl;

  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  bufferlist bl;
  ::encode(NotifyMessage(ReleasedLockPayload(get_client_id())), bl);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::schedule_request_lock(bool use_timer, int timer_delay) {
  assert(m_image_ctx.owner_lock.is_locked());

  if (m_image_ctx.exclusive_lock == nullptr) {
    // exclusive lock dynamically disabled via image refresh
    return;
  }
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  RWLock::RLocker watch_locker(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    ldout(m_image_ctx.cct, 15) << this << " requesting exclusive lock" << dendl;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::notify_request_lock, this));
    if (use_timer) {
      if (timer_delay < 0) {
        timer_delay = RETRY_DELAY_SECONDS;
      }
      m_task_finisher->add_event_after(TASK_CODE_REQUEST_LOCK, timer_delay,
                                       ctx);
    } else {
      m_task_finisher->queue(TASK_CODE_REQUEST_LOCK, ctx);
    }
  }
}

void ImageWatcher::notify_request_lock() {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  ldout(m_image_ctx.cct, 10) << this << " notify request lock" << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(RequestLockPayload(get_client_id())), bl);

  int r = notify_lock_owner(bl);
  if (r == -ETIMEDOUT) {
    ldout(m_image_ctx.cct, 5) << this << " timed out requesting lock: retrying"
                              << dendl;

    // treat this is a dead client -- so retest acquiring the lock
    m_image_ctx.exclusive_lock->handle_lock_released();
  } else if (r < 0) {
    lderr(m_image_ctx.cct) << this << " error requesting lock: "
                           << cpp_strerror(r) << dendl;
    schedule_request_lock(true);
  } else {
    // lock owner acked -- but resend if we don't see them release the lock
    int retry_timeout = m_image_ctx.cct->_conf->client_notify_timeout;
    ldout(m_image_ctx.cct, 15) << this << " will retry in " << retry_timeout
                               << " seconds" << dendl;
    schedule_request_lock(true, retry_timeout);
  }
}

int ImageWatcher::notify_lock_owner(bufferlist &bl) {
  assert(m_image_ctx.owner_lock.is_locked());

  // since we need to ack our own notifications, release the owner lock just in
  // case another notification occurs before this one and it requires the lock
  bufferlist response_bl;
  m_image_ctx.owner_lock.put_read();
  int r = m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT,
				     &response_bl);
  m_image_ctx.owner_lock.get_read();

  if (r < 0 && r != -ETIMEDOUT) {
    lderr(m_image_ctx.cct) << this << " lock owner notification failed: "
			   << cpp_strerror(r) << dendl;
    return r;
  }

  typedef std::map<std::pair<uint64_t, uint64_t>, bufferlist> responses_t;
  responses_t responses;
  if (response_bl.length() > 0) {
    try {
      bufferlist::iterator iter = response_bl.begin();
      ::decode(responses, iter);
    } catch (const buffer::error &err) {
      lderr(m_image_ctx.cct) << this << " failed to decode response" << dendl;
      return -EINVAL;
    }
  }

  bufferlist response;
  bool lock_owner_responded = false;
  for (responses_t::iterator i = responses.begin(); i != responses.end(); ++i) {
    if (i->second.length() > 0) {
      if (lock_owner_responded) {
	lderr(m_image_ctx.cct) << this << " duplicate lock owners detected"
                               << dendl;
	return -EIO;
      }
      lock_owner_responded = true;
      response.claim(i->second);
    }
  }

  if (!lock_owner_responded) {
    lderr(m_image_ctx.cct) << this << " no lock owners detected" << dendl;
    return -ETIMEDOUT;
  }

  try {
    bufferlist::iterator iter = response.begin();

    ResponseMessage response_message;
    ::decode(response_message, iter);

    r = response_message.result;
  } catch (const buffer::error &err) {
    r = -EINVAL;
  }
  return r;
}

void ImageWatcher::schedule_async_request_timed_out(const AsyncRequestId &id) {
  Context *ctx = new FunctionContext(boost::bind(
    &ImageWatcher::async_request_timed_out, this, id));

  Task task(TASK_CODE_ASYNC_REQUEST, id);
  m_task_finisher->cancel(task);

  m_task_finisher->add_event_after(task, m_image_ctx.request_timed_out_seconds, ctx);
}

void ImageWatcher::async_request_timed_out(const AsyncRequestId &id) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator it =
    m_async_requests.find(id);
  if (it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 10) << this << " request timed-out: " << id << dendl;
    it->second.first->complete(-ERESTART);
  }
}

int ImageWatcher::notify_async_request(const AsyncRequestId &async_request_id,
				       bufferlist &in,
				       ProgressContext& prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());

  ldout(m_image_ctx.cct, 10) << this << " async request: " << async_request_id
                             << dendl;

  C_SaferCond ctx;

  {
    RWLock::WLocker l(m_async_request_lock);
    m_async_requests[async_request_id] = AsyncRequest(&ctx, &prog_ctx);
  }

  BOOST_SCOPE_EXIT( (&ctx)(async_request_id)(&m_task_finisher)
                    (&m_async_requests)(&m_async_request_lock) ) {
    m_task_finisher->cancel(Task(TASK_CODE_ASYNC_REQUEST, async_request_id));

    RWLock::WLocker l(m_async_request_lock);
    m_async_requests.erase(async_request_id);
  } BOOST_SCOPE_EXIT_END

  schedule_async_request_timed_out(async_request_id);
  int r = notify_lock_owner(in);
  if (r < 0) {
    return r;
  }
  return ctx.wait();
}

int ImageWatcher::prepare_async_request(const AsyncRequestId& async_request_id,
                                        bool* new_request, Context** ctx,
                                        ProgressContext** prog_ctx) {
  if (async_request_id.client_id == get_client_id()) {
    return -ERESTART;
  } else {
    RWLock::WLocker l(m_async_request_lock);
    if (m_async_pending.count(async_request_id) == 0) {
      m_async_pending.insert(async_request_id);
      *new_request = true;
      *prog_ctx = new RemoteProgressContext(*this, async_request_id);
      *ctx = new RemoteContext(*this, async_request_id, *prog_ctx);
    } else {
      *new_request = false;
    }
  }
  return 0;
}

bool ImageWatcher::handle_payload(const HeaderUpdatePayload &payload,
				  C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " image header updated" << dendl;

  m_image_ctx.state->handle_update_notification();
  m_image_ctx.perfcounter->inc(l_librbd_notify);
  return true;
}

bool ImageWatcher::handle_payload(const AcquiredLockPayload &payload,
                                  C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " image exclusively locked announcement"
                             << dendl;

  bool cancel_async_requests = true;
  if (payload.client_id.is_valid()) {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    if (payload.client_id == m_owner_client_id) {
      cancel_async_requests = false;
    }
    set_owner_client_id(payload.client_id);
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (cancel_async_requests &&
      (m_image_ctx.exclusive_lock == nullptr ||
       !m_image_ctx.exclusive_lock->is_lock_owner())) {
    schedule_cancel_async_requests();
  }
  return true;
}

bool ImageWatcher::handle_payload(const ReleasedLockPayload &payload,
                                  C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " exclusive lock released" << dendl;

  bool cancel_async_requests = true;
  if (payload.client_id.is_valid()) {
    Mutex::Locker l(m_owner_client_id_lock);
    if (payload.client_id != m_owner_client_id) {
      ldout(m_image_ctx.cct, 10) << this << " unexpected owner: "
                                 << payload.client_id << " != "
                                 << m_owner_client_id << dendl;
      cancel_async_requests = false;
    } else {
      set_owner_client_id(ClientId());
    }
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (cancel_async_requests &&
      (m_image_ctx.exclusive_lock == nullptr ||
       !m_image_ctx.exclusive_lock->is_lock_owner())) {
    schedule_cancel_async_requests();
  }

  // alert the exclusive lock state machine that the lock is available
  if (m_image_ctx.exclusive_lock != nullptr &&
      !m_image_ctx.exclusive_lock->is_lock_owner()) {
    m_task_finisher->cancel(TASK_CODE_REQUEST_LOCK);
    m_image_ctx.exclusive_lock->handle_lock_released();
  }
  return true;
}

bool ImageWatcher::handle_payload(const RequestLockPayload &payload,
                                  C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " exclusive lock requested" << dendl;
  if (payload.client_id == get_client_id()) {
    return true;
  }

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    // need to send something back so the client can detect a missing leader
    ::encode(ResponseMessage(0), ack_ctx->out);

    {
      Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
      if (!m_owner_client_id.is_valid()) {
	return true;
      }
    }

    ldout(m_image_ctx.cct, 10) << this << " queuing release of exclusive lock"
                               << dendl;
    m_image_ctx.exclusive_lock->release_lock(nullptr);
  }
  return true;
}

bool ImageWatcher::handle_payload(const AsyncProgressPayload &payload,
                                  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator req_it =
    m_async_requests.find(payload.async_request_id);
  if (req_it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 20) << this << " request progress: "
			       << payload.async_request_id << " @ "
			       << payload.offset << "/" << payload.total
			       << dendl;
    schedule_async_request_timed_out(payload.async_request_id);
    req_it->second.second->update_progress(payload.offset, payload.total);
  }
  return true;
}

bool ImageWatcher::handle_payload(const AsyncCompletePayload &payload,
                                  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator req_it =
    m_async_requests.find(payload.async_request_id);
  if (req_it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 10) << this << " request finished: "
                               << payload.async_request_id << "="
			       << payload.result << dendl;
    req_it->second.first->complete(payload.result);
  }
  return true;
}

bool ImageWatcher::handle_payload(const FlattenPayload &payload,
				  C_NotifyAck *ack_ctx) {

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    bool new_request;
    Context *ctx;
    ProgressContext *prog_ctx;
    int r = prepare_async_request(payload.async_request_id, &new_request,
                                  &ctx, &prog_ctx);
    if (new_request) {
      ldout(m_image_ctx.cct, 10) << this << " remote flatten request: "
				 << payload.async_request_id << dendl;
      librbd::async_flatten(&m_image_ctx, ctx, *prog_ctx);
    }

    ::encode(ResponseMessage(r), ack_ctx->out);
  }
  return true;
}

bool ImageWatcher::handle_payload(const ResizePayload &payload,
				  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    bool new_request;
    Context *ctx;
    ProgressContext *prog_ctx;
    int r = prepare_async_request(payload.async_request_id, &new_request,
                                  &ctx, &prog_ctx);
    if (new_request) {
      ldout(m_image_ctx.cct, 10) << this << " remote resize request: "
				 << payload.async_request_id << " "
				 << payload.size << dendl;
      librbd::async_resize(&m_image_ctx, ctx, payload.size, *prog_ctx);
    }

    ::encode(ResponseMessage(r), ack_ctx->out);
  }
  return true;
}

bool ImageWatcher::handle_payload(const SnapCreatePayload &payload,
				  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_create request: "
			       << payload.snap_name << dendl;

    librbd::snap_create_helper(&m_image_ctx, new C_ResponseMessage(ack_ctx),
                               payload.snap_name.c_str());
    return false;
  }
  return true;
}

bool ImageWatcher::handle_payload(const SnapRenamePayload &payload,
				  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_rename request: "
			       << payload.snap_id << " to "
			       << payload.snap_name << dendl;

    librbd::snap_rename_helper(&m_image_ctx, new C_ResponseMessage(ack_ctx),
                               payload.snap_id, payload.snap_name.c_str());
    return false;
  }
  return true;
}

bool ImageWatcher::handle_payload(const SnapRemovePayload &payload,
				  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_remove request: "
			       << payload.snap_name << dendl;

    librbd::snap_remove_helper(&m_image_ctx, new C_ResponseMessage(ack_ctx),
                               payload.snap_name.c_str());
    return false;
  }
  return true;
}

bool ImageWatcher::handle_payload(const SnapProtectPayload& payload,
                                  C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_protect request: "
                               << payload.snap_name << dendl;

    librbd::snap_protect_helper(&m_image_ctx, new C_ResponseMessage(ack_ctx),
                                payload.snap_name.c_str());
    return false;
  }
  return true;
}

bool ImageWatcher::handle_payload(const SnapUnprotectPayload& payload,
                                  C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_unprotect request: "
                               << payload.snap_name << dendl;

    librbd::snap_unprotect_helper(&m_image_ctx, new C_ResponseMessage(ack_ctx),
                                  payload.snap_name.c_str());
    return false;
  }
  return true;
}

bool ImageWatcher::handle_payload(const RebuildObjectMapPayload& payload,
                                  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    bool new_request;
    Context *ctx;
    ProgressContext *prog_ctx;
    int r = prepare_async_request(payload.async_request_id, &new_request,
                                  &ctx, &prog_ctx);
    if (new_request) {
      ldout(m_image_ctx.cct, 10) << this
                                 << " remote rebuild object map request: "
                                 << payload.async_request_id << dendl;
      librbd::async_rebuild_object_map(&m_image_ctx, ctx, *prog_ctx);
    }

    ::encode(ResponseMessage(r), ack_ctx->out);
  }
  return true;
}

bool ImageWatcher::handle_payload(const RenamePayload& payload,
                                  C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ldout(m_image_ctx.cct, 10) << this << " remote rename request: "
                               << payload.image_name << dendl;

    librbd::rename_helper(&m_image_ctx, new C_ResponseMessage(ack_ctx),
                          payload.image_name.c_str());
    return false;
  }
  return true;
}

bool ImageWatcher::handle_payload(const UnknownPayload &payload,
				  C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    ::encode(ResponseMessage(-EOPNOTSUPP), ack_ctx->out);
  }
  return true;
}

void ImageWatcher::process_payload(uint64_t notify_id, uint64_t handle,
                                   const Payload &payload, int r) {
  if (r < 0) {
    bufferlist out_bl;
    acknowledge_notify(notify_id, handle, out_bl);
  } else {
    apply_visitor(HandlePayloadVisitor(this, notify_id, handle), payload);
  }
}

void ImageWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
				 bufferlist &bl) {
  NotifyMessage notify_message;
  if (bl.length() == 0) {
    // legacy notification for header updates
    notify_message = NotifyMessage(HeaderUpdatePayload());
  } else {
    try {
      bufferlist::iterator iter = bl.begin();
      ::decode(notify_message, iter);
    } catch (const buffer::error &err) {
      lderr(m_image_ctx.cct) << this << " error decoding image notification: "
			     << err.what() << dendl;
      return;
    }
  }

  // if an image refresh is required, refresh before processing the request
  if (m_image_ctx.state->is_refresh_required()) {
    m_image_ctx.state->refresh(new C_ProcessPayload(this, notify_id, handle,
                                                    notify_message.payload));
  } else {
    process_payload(notify_id, handle, notify_message.payload, 0);
  }
}

void ImageWatcher::handle_error(uint64_t handle, int err) {
  lderr(m_image_ctx.cct) << this << " image watch failed: " << handle << ", "
                         << cpp_strerror(err) << dendl;

  {
    Mutex::Locker l(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  RWLock::WLocker l(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    m_image_ctx.md_ctx.unwatch2(m_watch_handle);
    m_watch_state = WATCH_STATE_ERROR;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::reregister_watch, this));
    m_task_finisher->queue(TASK_CODE_REREGISTER_WATCH, ctx);
  }
}

void ImageWatcher::acknowledge_notify(uint64_t notify_id, uint64_t handle,
				      bufferlist &out) {
  m_image_ctx.md_ctx.notify_ack(m_image_ctx.header_oid, notify_id, handle, out);
}

void ImageWatcher::reregister_watch() {
  ldout(m_image_ctx.cct, 10) << this << " re-registering image watch" << dendl;

  bool was_lock_owner = false;
  C_SaferCond release_lock_ctx;
  {
    RWLock::WLocker l(m_image_ctx.owner_lock);
    if (m_image_ctx.exclusive_lock != nullptr &&
        m_image_ctx.exclusive_lock->is_lock_owner()) {
      was_lock_owner = true;
      m_image_ctx.exclusive_lock->release_lock(&release_lock_ctx);
    }
  }

  int r;
  if (was_lock_owner) {
    r = release_lock_ctx.wait();
    assert(r == 0);
  }

  {
    RWLock::WLocker l(m_watch_lock);
    if (m_watch_state != WATCH_STATE_ERROR) {
      return;
    }

    r = m_image_ctx.md_ctx.watch2(m_image_ctx.header_oid,
                                  &m_watch_handle, &m_watch_ctx);
    if (r < 0) {
      lderr(m_image_ctx.cct) << this << " failed to re-register image watch: "
                             << cpp_strerror(r) << dendl;
      if (r != -ESHUTDOWN) {
        FunctionContext *ctx = new FunctionContext(boost::bind(
          &ImageWatcher::reregister_watch, this));
        m_task_finisher->add_event_after(TASK_CODE_REREGISTER_WATCH,
                                         RETRY_DELAY_SECONDS, ctx);
      }
      return;
    }

    m_watch_state = WATCH_STATE_REGISTERED;
  }
  handle_payload(HeaderUpdatePayload(), NULL);
}

void ImageWatcher::WatchCtx::handle_notify(uint64_t notify_id,
        	                           uint64_t handle,
                                           uint64_t notifier_id,
	                                   bufferlist& bl) {
  image_watcher.handle_notify(notify_id, handle, bl);
}

void ImageWatcher::WatchCtx::handle_error(uint64_t handle, int err) {
  image_watcher.handle_error(handle, err);
}

void ImageWatcher::RemoteContext::finish(int r) {
  m_image_watcher.schedule_async_complete(m_async_request_id, r);
}

ImageWatcher::C_NotifyAck::C_NotifyAck(ImageWatcher *image_watcher,
                                       uint64_t notify_id, uint64_t handle)
  : image_watcher(image_watcher), notify_id(notify_id), handle(handle) {
  CephContext *cct = image_watcher->m_image_ctx.cct;
  ldout(cct, 10) << this << " C_NotifyAck start: id=" << notify_id << ", "
                 << "handle=" << handle << dendl;
}

void ImageWatcher::C_NotifyAck::finish(int r) {
  assert(r == 0);
  CephContext *cct = image_watcher->m_image_ctx.cct;
  ldout(cct, 10) << this << " C_NotifyAck finish: id=" << notify_id << ", "
                 << "handle=" << handle << dendl;

  image_watcher->acknowledge_notify(notify_id, handle, out);
}

void ImageWatcher::C_ResponseMessage::finish(int r) {
  CephContext *cct = notify_ack->image_watcher->m_image_ctx.cct;
  ldout(cct, 10) << this << " C_ResponseMessage: r=" << r << dendl;

  ::encode(ResponseMessage(r), notify_ack->out);
  notify_ack->complete(0);
}

} // namespace librbd
