// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ImageWatcher.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
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

using namespace watch_notify;

static const std::string WATCHER_LOCK_TAG = "internal";
static const std::string WATCHER_LOCK_COOKIE_PREFIX = "auto";

static const uint64_t	NOTIFY_TIMEOUT = 5000;
static const double	RETRY_DELAY_SECONDS = 1.0;

ImageWatcher::ImageWatcher(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx),
    m_watch_lock(unique_lock_name("librbd::ImageWatcher::m_watch_lock", this)),
    m_watch_ctx(*this), m_watch_handle(0),
    m_watch_state(WATCH_STATE_UNREGISTERED),
    m_refresh_lock(unique_lock_name("librbd::ImageWatcher::m_refresh_lock",
                                    this)),
    m_lock_supported(false), m_lock_owner_state(LOCK_OWNER_STATE_NOT_LOCKED),
    m_listeners_lock(unique_lock_name("librbd::ImageWatcher::m_listeners_lock", this)),
    m_listeners_in_use(false),
    m_task_finisher(new TaskFinisher<Task>(*m_image_ctx.cct)),
    m_async_request_lock(unique_lock_name("librbd::ImageWatcher::m_async_request_lock", this)),
    m_owner_client_id_lock(unique_lock_name("librbd::ImageWatcher::m_owner_client_id_lock", this))
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
  RWLock::RLocker l(m_image_ctx.snap_lock);
  return is_lock_supported(m_image_ctx.snap_lock);
}

bool ImageWatcher::is_lock_supported(const RWLock &) const {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.snap_lock.is_locked());
  return ((m_image_ctx.features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0 &&
	  !m_image_ctx.read_only && m_image_ctx.snap_id == CEPH_NOSNAP);
}

bool ImageWatcher::is_lock_owner() const {
  assert(m_image_ctx.owner_lock.is_locked());
  return (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED ||
          m_lock_owner_state == LOCK_OWNER_STATE_RELEASING);
}

void ImageWatcher::register_listener(Listener *listener) {
  Mutex::Locker listeners_locker(m_listeners_lock);
  m_listeners.push_back(listener);
}

void ImageWatcher::unregister_listener(Listener *listener) {
  // TODO CoW listener list
  Mutex::Locker listeners_locker(m_listeners_lock);
  while (m_listeners_in_use) {
    m_listeners_cond.Wait(m_listeners_lock);
  }
  m_listeners.remove(listener);
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

int ImageWatcher::refresh() {
  assert(m_image_ctx.owner_lock.is_locked());

  bool lock_support_changed = false;
  {
    Mutex::Locker refresh_locker(m_refresh_lock);
    if (m_lock_supported != is_lock_supported()) {
      m_lock_supported = is_lock_supported();
      lock_support_changed = true;
    }
  }

  int r = 0;
  if (lock_support_changed) {
    if (is_lock_supported()) {
      // image opened, exclusive lock dynamically enabled, or now HEAD
      notify_listeners_updated_lock(LOCK_UPDATE_STATE_RELEASING);
      notify_listeners_updated_lock(LOCK_UPDATE_STATE_UNLOCKED);
    } else if (!is_lock_supported()) {
      if (is_lock_owner()) {
        // exclusive lock dynamically disabled or now snapshot
        m_image_ctx.owner_lock.put_read();
        {
          RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
          r = release_lock();
        }
        m_image_ctx.owner_lock.get_read();
      }
      notify_listeners_updated_lock(LOCK_UPDATE_STATE_NOT_SUPPORTED);
    }
  }
  return r;
}

int ImageWatcher::try_lock() {
  assert(m_image_ctx.owner_lock.is_wlocked());
  assert(m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED);
  assert(is_lock_supported());

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
        set_owner_client_id(ClientId(iter->watcher_id, locker_handle));
	return 0;
      }
    }

    if (m_image_ctx.blacklist_on_break_lock) {
      ldout(m_image_ctx.cct, 1) << this << " blacklisting client: " << locker
                                << "@" << locker_address << dendl;
      librados::Rados rados(m_image_ctx.md_ctx);
      r = rados.blacklist_add(locker_address,
			      m_image_ctx.blacklist_expire_seconds);
      if (r < 0) {
        lderr(m_image_ctx.cct) << this << " unable to blacklist client: "
			       << cpp_strerror(r) << dendl;
        return r;
      }
    }

    ldout(m_image_ctx.cct, 5) << this << " breaking exclusive lock: " << locker
                              << dendl;
    r = rados::cls::lock::break_lock(&m_image_ctx.md_ctx,
                                     m_image_ctx.header_oid, RBD_LOCK_NAME,
                                     locker_cookie, locker);
    if (r < 0 && r != -ENOENT) {
      return r;
    }
  }
  return 0;
}

void ImageWatcher::request_lock() {
  schedule_request_lock(false);
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
    ldout(m_image_ctx.cct, 5) << this << " failed to acquire exclusive lock:"
			      << cpp_strerror(r) << dendl;
    return false;
  }

  if (is_lock_owner()) {
    ldout(m_image_ctx.cct, 15) << this << " successfully acquired exclusive lock"
			       << dendl;
  } else {
    ldout(m_image_ctx.cct, 15) << this
                               << " unable to acquire exclusive lock, retrying"
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
    ldout(m_image_ctx.cct, 20) << this << " no lockers detected" << dendl;
    return 0;
  }

  if (lock_tag != WATCHER_LOCK_TAG) {
    ldout(m_image_ctx.cct, 5) << this << " locked by external mechanism: tag="
			      << lock_tag << dendl;
    return -EBUSY;
  }

  if (lock_type == LOCK_SHARED) {
    ldout(m_image_ctx.cct, 5) << this << " shared lock type detected" << dendl;
    return -EBUSY;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!decode_lock_cookie(iter->first.cookie, handle)) {
    ldout(m_image_ctx.cct, 5) << this << " locked by external mechanism: "
                              << "cookie=" << iter->first.cookie << dendl;
    return -EBUSY;
  }

  *locker = iter->first.locker;
  *cookie = iter->first.cookie;
  *address = stringify(iter->second.addr);
  ldout(m_image_ctx.cct, 10) << this << " retrieved exclusive locker: "
                             << *locker << "@" << *address << dendl;
  return 0;
}

int ImageWatcher::lock() {
  assert(m_image_ctx.owner_lock.is_wlocked());
  assert(m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED);

  int r = rados::cls::lock::lock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				 RBD_LOCK_NAME, LOCK_EXCLUSIVE,
				 encode_lock_cookie(), WATCHER_LOCK_TAG, "",
				 utime_t(), 0);
  if (r < 0) {
    return r;
  }

  ldout(m_image_ctx.cct, 10) << this << " acquired exclusive lock" << dendl;
  m_lock_owner_state = LOCK_OWNER_STATE_LOCKED;

  ClientId owner_client_id = get_client_id();
  {
    Mutex::Locker l(m_owner_client_id_lock);
    set_owner_client_id(owner_client_id);
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

  // send the notification when we aren't holding locks
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_acquired_lock, this));
  m_task_finisher->queue(TASK_CODE_ACQUIRED_LOCK, ctx);
  return 0;
}

int ImageWatcher::unlock()
{
  assert(m_image_ctx.owner_lock.is_wlocked());

  ldout(m_image_ctx.cct, 10) << this << " releasing exclusive lock" << dendl;
  m_lock_owner_state = LOCK_OWNER_STATE_NOT_LOCKED;
  int r = rados::cls::lock::unlock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				   RBD_LOCK_NAME, encode_lock_cookie());
  if (r < 0 && r != -ENOENT) {
    lderr(m_image_ctx.cct) << this << " failed to release exclusive lock: "
			   << cpp_strerror(r) << dendl;
    return r;
  }

  if (m_image_ctx.object_map.enabled()) {
    m_image_ctx.object_map.unlock();
  }

  {
    Mutex::Locker l(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_released_lock, this));
  m_task_finisher->queue(TASK_CODE_RELEASED_LOCK, ctx);
  return 0;
}

int ImageWatcher::release_lock()
{
  assert(m_image_ctx.owner_lock.is_wlocked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " releasing exclusive lock by request" << dendl;
  if (m_lock_owner_state != LOCK_OWNER_STATE_LOCKED) {
    return 0;
  }

  m_lock_owner_state = LOCK_OWNER_STATE_RELEASING;
  m_image_ctx.owner_lock.put_write();

  // ensure all maint operations are canceled
  m_image_ctx.cancel_async_requests();

  int r;
  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

    // alert listeners that all incoming IO needs to be stopped since the
    // lock is being released
    notify_listeners_updated_lock(LOCK_UPDATE_STATE_RELEASING);

    // AioImageRequestWQ will have blocked writes / flushed IO by this point
    assert(m_image_ctx.aio_work_queue->writes_blocked());
  }

  m_image_ctx.owner_lock.get_write();
  assert(m_lock_owner_state == LOCK_OWNER_STATE_RELEASING);
  r = unlock();

  // notify listeners of the change w/ owner read locked
  m_image_ctx.owner_lock.put_write();
  {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    if (m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED) {
      notify_listeners_updated_lock(LOCK_UPDATE_STATE_UNLOCKED);
    }
  }
  m_image_ctx.owner_lock.get_write();

  if (r < 0) {
    lderr(cct) << this << " failed to unlock: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
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

int ImageWatcher::notify_snap_rename(const snapid_t &src_snap_id,
				     const std::string &dst_snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapRenamePayload(src_snap_id, dst_snap_name)), bl);

  return notify_lock_owner(bl);
}
int ImageWatcher::notify_snap_remove(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapRemovePayload(snap_name)), bl);

  return notify_lock_owner(bl);
}

int ImageWatcher::notify_snap_protect(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapProtectPayload(snap_name)), bl);
  return notify_lock_owner(bl);
}

int ImageWatcher::notify_snap_unprotect(const std::string &snap_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapUnprotectPayload(snap_name)), bl);
  return notify_lock_owner(bl);
}

int ImageWatcher::notify_rebuild_object_map(uint64_t request_id,
                                            ProgressContext &prog_ctx) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(RebuildObjectMapPayload(async_request_id)), bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_rename(const std::string &image_name) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(RenamePayload(image_name)), bl);
  return notify_lock_owner(bl);
}

void ImageWatcher::notify_lock_state() {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    // re-send the acquired lock notification so that peers know they can now
    // request the lock
    ldout(m_image_ctx.cct, 10) << this << " notify lock state" << dendl;

    bufferlist bl;
    ::encode(NotifyMessage(AcquiredLockPayload(get_client_id())), bl);

    m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT,
                               NULL);
  }
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

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_lock_owner_state != LOCK_OWNER_STATE_LOCKED) {
    return;
  }

  notify_listeners_updated_lock(LOCK_UPDATE_STATE_LOCKED);

  bufferlist bl;
  ::encode(NotifyMessage(AcquiredLockPayload(get_client_id())), bl);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::notify_release_lock() {
  RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
  release_lock();
}

void ImageWatcher::notify_released_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify released lock" << dendl;
  bufferlist bl;
  ::encode(NotifyMessage(ReleasedLockPayload(get_client_id())), bl);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::schedule_request_lock(bool use_timer, int timer_delay) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED);

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
  ldout(m_image_ctx.cct, 10) << this << " notify request lock" << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (try_request_lock()) {
    return;
  }

  bufferlist bl;
  ::encode(NotifyMessage(RequestLockPayload(get_client_id())), bl);

  int r = notify_lock_owner(bl);
  if (r == -ETIMEDOUT) {
    ldout(m_image_ctx.cct, 5) << this << " timed out requesting lock: retrying"
                              << dendl;
    schedule_request_lock(false);
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

  Mutex::Locker lictx(m_image_ctx.refresh_lock);
  ++m_image_ctx.refresh_seq;
  m_image_ctx.perfcounter->inc(l_librbd_notify);
  return true;
}

bool ImageWatcher::handle_payload(const AcquiredLockPayload &payload,
                                  C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " image exclusively locked announcement"
                             << dendl;

  bool cancel_async_requests = true;
  if (payload.client_id.is_valid()) {
    Mutex::Locker l(m_owner_client_id_lock);
    if (payload.client_id == m_owner_client_id) {
      cancel_async_requests = false;
    }
    set_owner_client_id(payload.client_id);
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED) {
    if (cancel_async_requests) {
      schedule_cancel_async_requests();
    }
    notify_listeners_updated_lock(LOCK_UPDATE_STATE_NOTIFICATION);
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_NOT_LOCKED) {
    if (cancel_async_requests) {
      schedule_cancel_async_requests();
    }
    notify_listeners_updated_lock(LOCK_UPDATE_STATE_NOTIFICATION);
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
    // need to send something back so the client can detect a missing leader
    ::encode(ResponseMessage(0), ack_ctx->out);

    {
      Mutex::Locker l(m_owner_client_id_lock);
      if (!m_owner_client_id.is_valid()) {
	return true;
      }
    }

    bool release_permitted = true;
    {
      Mutex::Locker listeners_locker(m_listeners_lock);
      for (Listeners::iterator it = m_listeners.begin();
           it != m_listeners.end(); ++it) {
        if (!(*it)->handle_requested_lock()) {
          release_permitted = false;
          break;
        }
      }
    }

    if (release_permitted) {
      ldout(m_image_ctx.cct, 10) << this << " queuing release of exclusive lock"
                                 << dendl;
      FunctionContext *ctx = new FunctionContext(
        boost::bind(&ImageWatcher::notify_release_lock, this));
      m_task_finisher->queue(TASK_CODE_RELEASING_LOCK, ctx);
    }
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (m_lock_owner_state == LOCK_OWNER_STATE_LOCKED) {
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
  if (is_lock_owner()) {
    ::encode(ResponseMessage(-EOPNOTSUPP), ack_ctx->out);
  }
  return true;
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

  apply_visitor(HandlePayloadVisitor(this, notify_id, handle),
		notify_message.payload);
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

  if (was_lock_owner) {
    r = try_lock();
    if (r == -EBUSY) {
      ldout(m_image_ctx.cct, 5) << this << "lost image lock while "
                                << "re-registering image watch" << dendl;
    } else if (r < 0) {
      lderr(m_image_ctx.cct) << this
                             << "failed to lock image while re-registering "
                             << "image watch" << cpp_strerror(r) << dendl;
    }
  }
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

void ImageWatcher::notify_listeners_updated_lock(
    LockUpdateState lock_update_state) {
  assert(m_image_ctx.owner_lock.is_locked());

  Listeners listeners;
  {
    Mutex::Locker listeners_locker(m_listeners_lock);
    m_listeners_in_use = true;
    listeners = m_listeners;
  }

  for (Listeners::iterator it = listeners.begin();
       it != listeners.end(); ++it) {
    (*it)->handle_lock_updated(lock_update_state);
  }

  Mutex::Locker listeners_locker(m_listeners_lock);
  m_listeners_in_use = false;
  m_listeners_cond.Signal();
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

