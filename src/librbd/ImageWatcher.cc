// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ImageWatcher.h"
#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/TaskFinisher.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
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

using namespace WatchNotify;

static const std::string WATCHER_LOCK_TAG = "internal";
static const std::string WATCHER_LOCK_COOKIE_PREFIX = "auto";

static const uint64_t	NOTIFY_TIMEOUT = 5000;
static const double	RETRY_DELAY_SECONDS = 1.0;

ImageWatcher::ImageWatcher(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx),
    m_watch_lock("librbd::ImageWatcher::m_watch_lock"),
    m_watch_ctx(*this), m_watch_handle(0),
    m_watch_state(WATCH_STATE_UNREGISTERED),
    m_lock_owner_state(LOCK_OWNER_STATE_NOT_LOCKED),
    m_task_finisher(new TaskFinisher<Task>(*m_image_ctx.cct)),
    m_async_request_lock("librbd::ImageWatcher::m_async_request_lock"),
    m_aio_request_lock("librbd::ImageWatcher::m_aio_request_lock"),
    m_owner_client_id_lock("librbd::ImageWatcher::m_owner_client_id_lock")
{
}

ImageWatcher::~ImageWatcher()
{
  delete m_task_finisher;
  {
    RWLock::RLocker l(m_watch_lock);
    assert(m_watch_state != WATCH_STATE_REGISTERED);
  }
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    assert(m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED);
  }
}

bool ImageWatcher::is_lock_supported() const {
  assert(m_image_ctx.owner_lock.is_locked());
  RWLock::RLocker l(m_image_ctx.snap_lock);
  uint64_t snap_features;
  m_image_ctx.get_features(m_image_ctx.snap_id, &snap_features);
  return ((snap_features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0 &&
	  !m_image_ctx.read_only && m_image_ctx.snap_id == CEPH_NOSNAP);
}

bool ImageWatcher::is_lock_owner() const {
  assert(m_image_ctx.owner_lock.is_locked());
  return (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED ||
          m_lock_owner_state == LOCK_OWNER_STATE_RELEASING);
}

int ImageWatcher::register_watch() {
  ldout(m_image_ctx.cct, 10) << "registering image watcher" << dendl;

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
  ldout(m_image_ctx.cct, 10)  << "unregistering image watcher" << dendl;

  {
    Mutex::Locker l(m_aio_request_lock);
    assert(m_aio_requests.empty());
  }

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

int ImageWatcher::try_lock() {
  assert(m_image_ctx.owner_lock.is_wlocked());
  assert(m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED);

  while (true) {
    int r = lock();
    if (r != -EBUSY) {
      return r;
    }

    // determine if the current lock holder is still alive
    entity_name_t locker;
    std::string locker_cookie;
    std::string locker_address;
    uint64_t locker_handle;
    r = get_lock_owner_info(&locker, &locker_cookie, &locker_address,
			    &locker_handle);
    if (r < 0) {
      return r;
    }
    if (locker_cookie.empty() || locker_address.empty()) {
      // lock is now unlocked ... try again
      continue;
    }

    std::list<obj_watch_t> watchers;
    r = m_image_ctx.md_ctx.list_watchers(m_image_ctx.header_oid, &watchers);
    if (r < 0) {
      return r;
    }

    for (std::list<obj_watch_t>::iterator iter = watchers.begin();
	 iter != watchers.end(); ++iter) {
      if ((strncmp(locker_address.c_str(),
                   iter->addr, sizeof(iter->addr)) == 0) &&
	  (locker_handle == iter->cookie)) {
	Mutex::Locker l(m_owner_client_id_lock);
	m_owner_client_id = ClientId(iter->watcher_id, locker_handle);
	return 0;
      }
    }

    md_config_t *conf = m_image_ctx.cct->_conf;
    if (conf->rbd_blacklist_on_break_lock) {
      ldout(m_image_ctx.cct, 1) << "blacklisting client: " << locker << "@"
				<< locker_address << dendl;
      librados::Rados rados(m_image_ctx.md_ctx);
      r = rados.blacklist_add(locker_address,
			      conf->rbd_blacklist_expire_seconds);
      if (r < 0) {
        lderr(m_image_ctx.cct) << "unable to blacklist client: "
			       << cpp_strerror(r) << dendl;
        return r;
      }
    }

    ldout(m_image_ctx.cct, 5) << "breaking exclusive lock: " << locker << dendl;
    r = rados::cls::lock::break_lock(&m_image_ctx.md_ctx,
                                     m_image_ctx.header_oid, RBD_LOCK_NAME,
                                     locker_cookie, locker);
    if (r < 0 && r != -ENOENT) {
      return r;
    }
  }
  return 0;
}

void ImageWatcher::request_lock(
    const boost::function<void(AioCompletion*)>& restart_op, AioCompletion* c) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED);

  {
    Mutex::Locker l(m_aio_request_lock);
    bool request_pending = !m_aio_requests.empty();
    ldout(m_image_ctx.cct, 15) << "queuing aio request: " << c
			       << dendl;

    c->get();
    m_aio_requests.push_back(std::make_pair(restart_op, c));
    if (request_pending) {
      return;
    }
  }

  RWLock::RLocker l(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    ldout(m_image_ctx.cct, 10) << "requesting exclusive lock" << dendl;

    // run notify request in finisher to avoid blocking aio path
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::notify_request_lock, this));
    m_task_finisher->queue(TASK_CODE_REQUEST_LOCK, ctx);
  }
}

bool ImageWatcher::try_request_lock() {
  assert(m_image_ctx.owner_lock.is_locked());
  if (is_lock_owner()) {
    return true;
  }

  int r = 0;
  m_image_ctx.owner_lock.put_read();
  {
    RWLock::WLocker l(m_image_ctx.owner_lock);
    if (!is_lock_owner()) {
      r = try_lock();
    }
  }
  m_image_ctx.owner_lock.get_read();

  if (r < 0) {
    ldout(m_image_ctx.cct, 5) << "failed to acquire exclusive lock:"
			      << cpp_strerror(r) << dendl;
    return false;
  }

  if (is_lock_owner()) {
    ldout(m_image_ctx.cct, 15) << "successfully acquired exclusive lock"
			       << dendl;
  } else {
    ldout(m_image_ctx.cct, 15) << "unable to acquire exclusive lock, retrying"
			       << dendl;
  }
  return is_lock_owner();
}

int ImageWatcher::get_lock_owner_info(entity_name_t *locker, std::string *cookie,
				      std::string *address, uint64_t *handle) {
  std::map<rados::cls::lock::locker_id_t,
	   rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type;
  std::string lock_tag;
  int r = rados::cls::lock::get_lock_info(&m_image_ctx.md_ctx,
					  m_image_ctx.header_oid,
					  RBD_LOCK_NAME, &lockers, &lock_type,
					  &lock_tag);
  if (r < 0) {
    return r;
  }

  if (lockers.empty()) {
    ldout(m_image_ctx.cct, 20) << "no lockers detected" << dendl;
    return 0;
  }

  if (lock_tag != WATCHER_LOCK_TAG) {
    ldout(m_image_ctx.cct, 5) << "locked by external mechanism: tag="
			      << lock_tag << dendl;
    return -EBUSY;
  }

  if (lock_type == LOCK_SHARED) {
    ldout(m_image_ctx.cct, 5) << "shared lock type detected" << dendl;
    return -EBUSY;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!decode_lock_cookie(iter->first.cookie, handle)) {
    ldout(m_image_ctx.cct, 5) << "locked by external mechanism: cookie="
			      << iter->first.cookie << dendl;
    return -EBUSY;
  }

  *locker = iter->first.locker;
  *cookie = iter->first.cookie;
  *address = stringify(iter->second.addr);
  ldout(m_image_ctx.cct, 10) << "retrieved exclusive locker: " << *locker
			     << "@" << *address << dendl;
  return 0;
}

int ImageWatcher::lock() {
  int r = rados::cls::lock::lock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				 RBD_LOCK_NAME, LOCK_EXCLUSIVE,
				 encode_lock_cookie(), WATCHER_LOCK_TAG, "",
				 utime_t(), 0);
  if (r < 0) {
    return r;
  }

  ldout(m_image_ctx.cct, 10) << "acquired exclusive lock" << dendl;
  m_lock_owner_state = LOCK_OWNER_STATE_LOCKED;

  {
    Mutex::Locker l(m_owner_client_id_lock);
    m_owner_client_id = get_client_id();
  }

  if (m_image_ctx.object_map.enabled()) {
    r = m_image_ctx.object_map.lock();
    if (r < 0 && r != -ENOENT) {
      unlock();
      return r;
    }
    RWLock::WLocker l2(m_image_ctx.snap_lock);
    m_image_ctx.object_map.refresh(CEPH_NOSNAP);
  }

  bufferlist bl;
  ::encode(NotifyMessage(AcquiredLockPayload(get_client_id())), bl);

  // send the notification when we aren't holding locks
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&IoCtx::notify2, &m_image_ctx.md_ctx, m_image_ctx.header_oid,
		bl, NOTIFY_TIMEOUT, reinterpret_cast<bufferlist *>(NULL)));
  m_task_finisher->queue(TASK_CODE_ACQUIRED_LOCK, ctx);
  return 0;
}

void ImageWatcher::prepare_unlock() {
  assert(m_image_ctx.owner_lock.is_wlocked());
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    m_lock_owner_state = LOCK_OWNER_STATE_RELEASING;
  }
}

void ImageWatcher::cancel_unlock() {
  assert(m_image_ctx.owner_lock.is_wlocked());
  if (m_lock_owner_state == LOCK_OWNER_STATE_RELEASING) {
    m_lock_owner_state = LOCK_OWNER_STATE_LOCKED;
  }
}

int ImageWatcher::unlock()
{
  assert(m_image_ctx.owner_lock.is_wlocked());
  if (m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED) {
    return 0;
  }

  ldout(m_image_ctx.cct, 10) << "releasing exclusive lock" << dendl;
  m_lock_owner_state = LOCK_OWNER_STATE_NOT_LOCKED;
  int r = rados::cls::lock::unlock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				   RBD_LOCK_NAME, encode_lock_cookie());
  if (r < 0 && r != -ENOENT) {
    lderr(m_image_ctx.cct) << "failed to release exclusive lock: "
			   << cpp_strerror(r) << dendl;
    return r;
  }

  if (m_image_ctx.object_map.enabled()) {
    m_image_ctx.object_map.unlock();
  }

  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_released_lock, this));
  m_task_finisher->queue(TASK_CODE_RELEASED_LOCK, ctx);
  return 0;
}

bool ImageWatcher::release_lock()
{
  assert(m_image_ctx.owner_lock.is_wlocked());
  ldout(m_image_ctx.cct, 10) << "releasing exclusive lock by request" << dendl;
  if (!is_lock_owner()) {
    return false;
  }
  prepare_unlock();

  m_image_ctx.owner_lock.put_write();
  m_image_ctx.cancel_async_requests();
  m_image_ctx.owner_lock.get_write();

  if (!is_lock_owner()) {
    return false;
  }

  {
    RWLock::WLocker l2(m_image_ctx.md_lock);
    librbd::_flush(&m_image_ctx);
  }

  unlock();
  return true;
}

void ImageWatcher::assert_header_locked(librados::ObjectWriteOperation *op) {
  rados::cls::lock::assert_locked(op, RBD_LOCK_NAME, LOCK_EXCLUSIVE,
                                  encode_lock_cookie(), WATCHER_LOCK_TAG);
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
  ldout(m_image_ctx.cct, 20) << "remote async request progress: "
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
  ldout(m_image_ctx.cct, 20) << "remote async request finished: "
			     << request << " = " << r << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(AsyncCompletePayload(request, r)), bl);

  librbd::notify_change(m_image_ctx.md_ctx, m_image_ctx.header_oid,
			&m_image_ctx);
  int ret = m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl,
				       NOTIFY_TIMEOUT, NULL);
  if (ret < 0) {
    lderr(m_image_ctx.cct) << "failed to notify async complete: "
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
  assert(!is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(FlattenPayload(async_request_id)), bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_resize(uint64_t request_id, uint64_t size,
				ProgressContext &prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(ResizePayload(size, async_request_id)), bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_snap_create(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapCreatePayload(snap_name)), bl);

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

std::string ImageWatcher::encode_lock_cookie() const {
  RWLock::RLocker l(m_watch_lock);
  std::ostringstream ss;
  ss << WATCHER_LOCK_COOKIE_PREFIX << " " << m_watch_handle;
  return ss.str();
}

bool ImageWatcher::decode_lock_cookie(const std::string &tag,
				      uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != WATCHER_LOCK_COOKIE_PREFIX) {
    return false;
  }
  return true;
}

void ImageWatcher::schedule_retry_aio_requests(bool use_timer) {
  Context *ctx = new FunctionContext(boost::bind(
    &ImageWatcher::retry_aio_requests, this));
  if (use_timer) {
    m_task_finisher->add_event_after(TASK_CODE_RETRY_AIO_REQUESTS,
                                     RETRY_DELAY_SECONDS, ctx);
  } else {
    m_task_finisher->queue(TASK_CODE_RETRY_AIO_REQUESTS, ctx);
  }
}

void ImageWatcher::retry_aio_requests() {
  m_task_finisher->cancel(TASK_CODE_RETRY_AIO_REQUESTS);
  std::vector<AioRequest> lock_request_restarts;
  {
    Mutex::Locker l(m_aio_request_lock);
    lock_request_restarts.swap(m_aio_requests);
  }

  ldout(m_image_ctx.cct, 15) << "retrying pending aio requests" << dendl;
  for (std::vector<AioRequest>::iterator iter = lock_request_restarts.begin();
       iter != lock_request_restarts.end(); ++iter) {
    ldout(m_image_ctx.cct, 20) << "retrying aio request: " << iter->second
			       << dendl;
    iter->first(iter->second);
    iter->second->put();
  }
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

ClientId ImageWatcher::get_client_id() {
  RWLock::RLocker l(m_watch_lock);
  return ClientId(m_image_ctx.md_ctx.get_instance_id(), m_watch_handle);
}

void ImageWatcher::notify_release_lock() {
  RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
  release_lock();
}

void ImageWatcher::notify_released_lock() {
  ldout(m_image_ctx.cct, 10) << "notify released lock" << dendl;
  bufferlist bl;
  ::encode(NotifyMessage(ReleasedLockPayload(get_client_id())), bl);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::notify_request_lock() {
  ldout(m_image_ctx.cct, 10) << "notify request lock" << dendl;
  m_task_finisher->cancel(TASK_CODE_RETRY_AIO_REQUESTS);

  m_image_ctx.owner_lock.get_read();
  if (try_request_lock()) {
    m_image_ctx.owner_lock.put_read();
    retry_aio_requests();
    return;
  }

  bufferlist bl;
  ::encode(NotifyMessage(RequestLockPayload(get_client_id())), bl);

  int r = notify_lock_owner(bl);
  m_image_ctx.owner_lock.put_read();

  if (r == -ETIMEDOUT) {
    ldout(m_image_ctx.cct, 5) << "timed out requesting lock: retrying" << dendl;
    retry_aio_requests();
  } else if (r < 0) {
    lderr(m_image_ctx.cct) << "error requesting lock: " << cpp_strerror(r)
			   << dendl;
    schedule_retry_aio_requests(true);
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
    lderr(m_image_ctx.cct) << "lock owner notification failed: "
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
      lderr(m_image_ctx.cct) << "failed to decode response" << dendl;
      return -EINVAL;
    }
  }

  bufferlist response;
  bool lock_owner_responded = false;
  for (responses_t::iterator i = responses.begin(); i != responses.end(); ++i) {
    if (i->second.length() > 0) {
      if (lock_owner_responded) {
	lderr(m_image_ctx.cct) << "duplicate lock owners detected" << dendl;
	return -EIO;
      }
      lock_owner_responded = true;
      response.claim(i->second);
    }
  }

  if (!lock_owner_responded) {
    lderr(m_image_ctx.cct) << "no lock owners detected" << dendl;
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

  md_config_t *conf = m_image_ctx.cct->_conf;
  m_task_finisher->add_event_after(task, conf->rbd_request_timed_out_seconds,
                                   ctx);
}

void ImageWatcher::async_request_timed_out(const AsyncRequestId &id) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator it =
    m_async_requests.find(id);
  if (it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 10) << "request timed-out: " << id << dendl;
    it->second.first->complete(-ERESTART);
  }
}

int ImageWatcher::notify_async_request(const AsyncRequestId &async_request_id,
				       bufferlist &in,
				       ProgressContext& prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());

  ldout(m_image_ctx.cct, 10) << "async request: " << async_request_id << dendl;

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

void ImageWatcher::handle_payload(const HeaderUpdatePayload &payload,
				  bufferlist *out) {
  ldout(m_image_ctx.cct, 10) << "image header updated" << dendl;

  Mutex::Locker lictx(m_image_ctx.refresh_lock);
  ++m_image_ctx.refresh_seq;
  m_image_ctx.perfcounter->inc(l_librbd_notify);
}

void ImageWatcher::handle_payload(const AcquiredLockPayload &payload,
                                  bufferlist *out) {
  ldout(m_image_ctx.cct, 10) << "image exclusively locked announcement" << dendl;
  if (payload.client_id.is_valid()) {
    Mutex::Locker l(m_owner_client_id_lock);
    if (payload.client_id == m_owner_client_id) {
      // we already know that the remote client is the owner
      return;
    }
    m_owner_client_id = payload.client_id;
  }

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED) {
    schedule_cancel_async_requests();
    schedule_retry_aio_requests(false);
  }
}

void ImageWatcher::handle_payload(const ReleasedLockPayload &payload,
                                  bufferlist *out) {
  ldout(m_image_ctx.cct, 10) << "exclusive lock released" << dendl;
  if (payload.client_id.is_valid()) {
    Mutex::Locker l(m_owner_client_id_lock);
    if (payload.client_id != m_owner_client_id) {
      return;
    }
    m_owner_client_id = ClientId();
  }

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED) {
    schedule_cancel_async_requests();
    schedule_retry_aio_requests(false);
  }
}

void ImageWatcher::handle_payload(const RequestLockPayload &payload,
                                  bufferlist *out) {
  ldout(m_image_ctx.cct, 10) << "exclusive lock requested" << dendl;
  if (payload.client_id == get_client_id()) {
    return;
  }

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    // need to send something back so the client can detect a missing leader
    ::encode(ResponseMessage(0), *out);

    {
      Mutex::Locker l(m_owner_client_id_lock);
      if (!m_owner_client_id.is_valid()) {
	return;
      }
      m_owner_client_id = ClientId();
    }

    ldout(m_image_ctx.cct, 10) << "queuing release of exclusive lock" << dendl;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::notify_release_lock, this));
    m_task_finisher->queue(TASK_CODE_RELEASING_LOCK, ctx);
  }
}

void ImageWatcher::handle_payload(const AsyncProgressPayload &payload,
                                  bufferlist *out) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator req_it =
    m_async_requests.find(payload.async_request_id);
  if (req_it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 20) << "request progress: "
			       << payload.async_request_id << " @ "
			       << payload.offset << "/" << payload.total
			       << dendl;
    schedule_async_request_timed_out(payload.async_request_id);
    req_it->second.second->update_progress(payload.offset, payload.total);
  }
}

void ImageWatcher::handle_payload(const AsyncCompletePayload &payload,
                                  bufferlist *out) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator req_it =
    m_async_requests.find(payload.async_request_id);
  if (req_it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 10) << "request finished: "
                               << payload.async_request_id << "="
			       << payload.result << dendl;
    req_it->second.first->complete(payload.result);
  }
}

void ImageWatcher::handle_payload(const FlattenPayload &payload,
				  bufferlist *out) {

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    int r = 0;
    bool new_request = false;
    if (payload.async_request_id.client_id == get_client_id()) {
      r = -ERESTART;
    } else {
      RWLock::WLocker l(m_async_request_lock);
      if (m_async_pending.count(payload.async_request_id) == 0) {
	m_async_pending.insert(payload.async_request_id);
	new_request = true;
      }
    }

    if (new_request) {
      RemoteProgressContext *prog_ctx =
	new RemoteProgressContext(*this, payload.async_request_id);
      RemoteContext *ctx = new RemoteContext(*this, payload.async_request_id,
					     prog_ctx);

      ldout(m_image_ctx.cct, 10) << "remote flatten request: "
				 << payload.async_request_id << dendl;
      r = librbd::async_flatten(&m_image_ctx, ctx, *prog_ctx);
      if (r < 0) {
	delete ctx;
	lderr(m_image_ctx.cct) << "remove flatten request failed: "
			       << cpp_strerror(r) << dendl;

	RWLock::WLocker l(m_async_request_lock);
	m_async_pending.erase(payload.async_request_id);
      }
    }

    ::encode(ResponseMessage(r), *out);
  }
}

void ImageWatcher::handle_payload(const ResizePayload &payload,
				  bufferlist *out) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    int r = 0;
    bool new_request = false;
    if (payload.async_request_id.client_id == get_client_id()) {
      r = -ERESTART;
    } else {
      RWLock::WLocker l(m_async_request_lock);
      if (m_async_pending.count(payload.async_request_id) == 0) {
	m_async_pending.insert(payload.async_request_id);
	new_request = true;
      }
    }

    if (new_request) {
      RemoteProgressContext *prog_ctx =
	new RemoteProgressContext(*this, payload.async_request_id);
      RemoteContext *ctx = new RemoteContext(*this, payload.async_request_id,
					     prog_ctx);

      ldout(m_image_ctx.cct, 10) << "remote resize request: "
				 << payload.async_request_id << " "
				 << payload.size << dendl;
      r = librbd::async_resize(&m_image_ctx, ctx, payload.size, *prog_ctx);
      if (r < 0) {
	lderr(m_image_ctx.cct) << "remove resize request failed: "
			       << cpp_strerror(r) << dendl;
	delete ctx;

	RWLock::WLocker l(m_async_request_lock);
	m_async_pending.erase(payload.async_request_id);
      }
    }

    ::encode(ResponseMessage(r), *out);
  }
}

void ImageWatcher::handle_payload(const SnapCreatePayload &payload,
				  bufferlist *out) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    ldout(m_image_ctx.cct, 10) << "remote snap_create request: "
			       << payload.snap_name << dendl;
    int r = librbd::snap_create_helper(&m_image_ctx, NULL,
                                       payload.snap_name.c_str());

    ::encode(ResponseMessage(r), *out);
  }
}

void ImageWatcher::handle_payload(const UnknownPayload &payload,
				  bufferlist *out) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (is_lock_owner()) {
    ::encode(ResponseMessage(-EOPNOTSUPP), *out);
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
      lderr(m_image_ctx.cct) << "error decoding image notification: "
			     << err.what() << dendl;
      return;
    }
  }

  apply_visitor(HandlePayloadVisitor(this, notify_id, handle),
		notify_message.payload); 
}

void ImageWatcher::handle_error(uint64_t handle, int err) {
  lderr(m_image_ctx.cct) << "image watch failed: " << handle << ", "
                         << cpp_strerror(err) << dendl;

  {
    Mutex::Locker l(m_owner_client_id_lock);
    m_owner_client_id = ClientId();
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
  ldout(m_image_ctx.cct, 10) << "re-registering image watch" << dendl;

  {
    RWLock::WLocker l(m_image_ctx.owner_lock);
    bool was_lock_owner = false;
    if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
      // ensure all async requests are canceled and IO is flushed
      was_lock_owner = release_lock();
    }

    int r;
    {
      RWLock::WLocker l(m_watch_lock);
      if (m_watch_state != WATCH_STATE_ERROR) {
	return;
      }

      r = m_image_ctx.md_ctx.watch2(m_image_ctx.header_oid,
                                    &m_watch_handle, &m_watch_ctx);
      if (r < 0) {
        lderr(m_image_ctx.cct) << "failed to re-register image watch: "
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

    if (was_lock_owner) {
      r = try_lock();
      if (r == -EBUSY) {
        ldout(m_image_ctx.cct, 5) << "lost image lock while re-registering "
                                  << "image watch" << dendl;
      } else if (r < 0) {
        lderr(m_image_ctx.cct) << "failed to lock image while re-registering "
                               << "image watch" << cpp_strerror(r) << dendl;
      }
    }
  }

  retry_aio_requests();
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

}
