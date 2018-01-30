// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/rados/librados.hpp"
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "global/global_context.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/image/RemoveRequest.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Utils.h"
#include "ImageDeleter.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/RemoveRequest.h"
#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"
#include "tools/rbd_mirror/image_deleter/TrashWatcher.h"
#include <map>
#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror

using std::string;
using std::stringstream;
using std::vector;
using std::pair;
using std::make_pair;

using librados::IoCtx;
using namespace librbd;

namespace rbd {
namespace mirror {

namespace {

class ImageDeleterAdminSocketCommand {
public:
  virtual ~ImageDeleterAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

template <typename I>
class StatusCommand : public ImageDeleterAdminSocketCommand {
public:
  explicit StatusCommand(ImageDeleter<I> *image_del) : image_del(image_del) {}

  bool call(Formatter *f, stringstream *ss) override {
    image_del->print_status(f, ss);
    return true;
  }

private:
  ImageDeleter<I> *image_del;
};

} // anonymous namespace

template <typename I>
class ImageDeleterAdminSocketHook : public AdminSocketHook {
public:
  ImageDeleterAdminSocketHook(CephContext *cct, const std::string& pool_name,
                              ImageDeleter<I> *image_del) :
    admin_socket(cct->get_admin_socket()) {

    std::string command;
    int r;

    command = "rbd mirror deletion status " + pool_name;
    r = admin_socket->register_command(command, command, this,
				       "get status for image deleter");
    if (r == 0) {
      commands[command] = new StatusCommand<I>(image_del);
    }

  }

  ~ImageDeleterAdminSocketHook() override {
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
  typedef std::map<std::string, ImageDeleterAdminSocketCommand*,
		   std::less<>> Commands;
  AdminSocket *admin_socket;
  Commands commands;
};

template <typename I>
ImageDeleter<I>::ImageDeleter(librados::IoCtx& local_io_ctx,
                              Threads<librbd::ImageCtx>* threads,
                              ServiceDaemon<librbd::ImageCtx>* service_daemon)
  : m_local_io_ctx(local_io_ctx), m_threads(threads),
    m_service_daemon(service_daemon), m_trash_listener(this),
    m_lock(librbd::util::unique_lock_name("rbd::mirror::ImageDeleter::m_lock",
                                          this)) {
}

#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageDeleter: " << " " \
                           << __func__ << ": "

template <typename I>
void ImageDeleter<I>::trash_move(librados::IoCtx& local_io_ctx,
                                 const std::string& global_image_id,
                                 bool resync,
                                 ContextWQ* work_queue, Context* on_finish) {
  dout(10) << "global_image_id=" << global_image_id << ", "
           << "resync=" << resync << dendl;

  auto req = rbd::mirror::image_deleter::TrashMoveRequest<>::create(
    local_io_ctx, global_image_id, resync, work_queue, on_finish);
  req->send();
}

#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageDeleter: " << this << " " \
                           << __func__ << ": "

template <typename I>
void ImageDeleter<I>::init(Context* on_finish) {
  dout(10) << dendl;

  m_asok_hook = new ImageDeleterAdminSocketHook<I>(
    g_ceph_context, m_local_io_ctx.get_pool_name(), this);

  m_trash_watcher = image_deleter::TrashWatcher<I>::create(m_local_io_ctx,
                                                           m_threads,
                                                           m_trash_listener);
  m_trash_watcher->init(on_finish);
}

template <typename I>
void ImageDeleter<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  delete m_asok_hook;
  m_asok_hook = nullptr;

  shut_down_trash_watcher(on_finish);
}

template <typename I>
void ImageDeleter<I>::shut_down_trash_watcher(Context* on_finish) {
  dout(10) << dendl;
  assert(m_trash_watcher);
  auto ctx = new FunctionContext([this, on_finish](int r) {
      delete m_trash_watcher;
      m_trash_watcher = nullptr;

      wait_for_ops(on_finish);
    });
  m_trash_watcher->shut_down(ctx);
}

template <typename I>
void ImageDeleter<I>::wait_for_ops(Context* on_finish) {
  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    m_running = false;
    cancel_retry_timer();
  }

  auto ctx = new FunctionContext([this, on_finish](int) {
      cancel_all_deletions(on_finish);
    });
  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void ImageDeleter<I>::cancel_all_deletions(Context* on_finish) {
  {
    Mutex::Locker locker(m_lock);
    // wake up any external state machines waiting on deletions
    assert(m_in_flight_delete_queue.empty());
    for (auto& queue : {&m_delete_queue, &m_retry_delete_queue}) {
      for (auto& info : *queue) {
        notify_on_delete(info->image_id, -ECANCELED);
      }
      queue->clear();
    }
  }
  on_finish->complete(0);
}

template <typename I>
void ImageDeleter<I>::wait_for_deletion(const std::string& image_id,
                                        bool scheduled_only,
                                        Context* on_finish) {
  dout(5) << "image_id=" << image_id << dendl;

  on_finish = new FunctionContext([this, on_finish](int r) {
      m_threads->work_queue->queue(on_finish, r);
    });

  Mutex::Locker locker(m_lock);
  auto del_info = find_delete_info(image_id);
  if (!del_info && scheduled_only) {
    // image not scheduled for deletion
    on_finish->complete(0);
    return;
  }

  notify_on_delete(image_id, -ESTALE);
  m_on_delete_contexts[image_id] = on_finish;
}

template <typename I>
void ImageDeleter<I>::complete_active_delete(DeleteInfoRef* delete_info,
                                             int r) {
  dout(20) << "info=" << *delete_info << ", r=" << r << dendl;
  Mutex::Locker locker(m_lock);
  notify_on_delete((*delete_info)->image_id, r);
  delete_info->reset();
}

template <typename I>
void ImageDeleter<I>::enqueue_failed_delete(DeleteInfoRef* delete_info,
                                            int error_code,
                                            double retry_delay) {
  dout(20) << "info=" << *delete_info << ", r=" << error_code << dendl;
  if (error_code == -EBLACKLISTED) {
    Mutex::Locker locker(m_lock);
    derr << "blacklisted while deleting local image" << dendl;
    complete_active_delete(delete_info, error_code);
    return;
  }

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  auto& delete_info_ref = *delete_info;
  notify_on_delete(delete_info_ref->image_id, error_code);
  delete_info_ref->error_code = error_code;
  ++delete_info_ref->retries;
  delete_info_ref->retry_time = ceph_clock_now();
  delete_info_ref->retry_time += retry_delay;
  m_retry_delete_queue.push_back(delete_info_ref);

  schedule_retry_timer();
}

template <typename I>
typename ImageDeleter<I>::DeleteInfoRef
ImageDeleter<I>::find_delete_info(const std::string &image_id) {
  assert(m_lock.is_locked());
  DeleteQueue delete_queues[] = {m_in_flight_delete_queue,
                                 m_retry_delete_queue,
                                 m_delete_queue};

  DeleteInfo delete_info{image_id};
  for (auto& queue : delete_queues) {
    auto it = std::find_if(queue.begin(), queue.end(),
                           [&delete_info](const DeleteInfoRef& ref) {
                             return delete_info == *ref;
                           });
    if (it != queue.end()) {
      return *it;
    }
  }
  return {};
}

template <typename I>
void ImageDeleter<I>::print_status(Formatter *f, stringstream *ss) {
  dout(20) << dendl;

  if (f) {
    f->open_object_section("image_deleter_status");
    f->open_array_section("delete_images_queue");
  }

  Mutex::Locker l(m_lock);
  for (const auto& image : m_delete_queue) {
    image->print_status(f, ss);
  }

  if (f) {
    f->close_section();
    f->open_array_section("failed_deletes_queue");
  }

  for (const auto& image : m_retry_delete_queue) {
    image->print_status(f, ss, true);
  }

  if (f) {
    f->close_section();
    f->close_section();
    f->flush(*ss);
  }
}

template <typename I>
vector<string> ImageDeleter<I>::get_delete_queue_items() {
  vector<string> items;

  Mutex::Locker l(m_lock);
  for (const auto& del_info : m_delete_queue) {
    items.push_back(del_info->image_id);
  }

  return items;
}

template <typename I>
vector<pair<string, int> > ImageDeleter<I>::get_failed_queue_items() {
  vector<pair<string, int> > items;

  Mutex::Locker l(m_lock);
  for (const auto& del_info : m_retry_delete_queue) {
    items.push_back(make_pair(del_info->image_id,
                              del_info->error_code));
  }

  return items;
}

template <typename I>
void ImageDeleter<I>::remove_images() {
  dout(10) << dendl;

  uint64_t max_concurrent_deletions = g_ceph_context->_conf->get_val<uint64_t>(
    "rbd_mirror_concurrent_image_deletions");

  Mutex::Locker locker(m_lock);
  while (true) {
    if (!m_running || m_delete_queue.empty() ||
        m_in_flight_delete_queue.size() >= max_concurrent_deletions) {
      return;
    }

    DeleteInfoRef delete_info = m_delete_queue.front();
    m_delete_queue.pop_front();

    assert(delete_info);
    remove_image(delete_info);
  }
}

template <typename I>
void ImageDeleter<I>::remove_image(DeleteInfoRef delete_info) {
  dout(10) << "info=" << *delete_info << dendl;
  assert(m_lock.is_locked());

  m_in_flight_delete_queue.push_back(delete_info);
  m_async_op_tracker.start_op();

  auto ctx = new FunctionContext([this, delete_info](int r) {
      handle_remove_image(delete_info, r);
      m_async_op_tracker.finish_op();
    });

  auto req = image_deleter::RemoveRequest<I>::create(
    m_local_io_ctx, delete_info->image_id, &delete_info->error_result,
    m_threads->work_queue, ctx);
  req->send();
}

template <typename I>
void ImageDeleter<I>::handle_remove_image(DeleteInfoRef delete_info,
                                          int r) {
  dout(10) << "info=" << *delete_info << ", r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_lock.is_locked());
    auto it = std::find(m_in_flight_delete_queue.begin(),
                        m_in_flight_delete_queue.end(), delete_info);
    assert(it != m_in_flight_delete_queue.end());
    m_in_flight_delete_queue.erase(it);
  }

  if (r < 0) {
    if (delete_info->error_result == image_deleter::ERROR_RESULT_COMPLETE) {
      complete_active_delete(&delete_info, r);
    } else if (delete_info->error_result ==
                 image_deleter::ERROR_RESULT_RETRY_IMMEDIATELY) {
      enqueue_failed_delete(&delete_info, r, m_busy_interval);
    } else {
      double failed_interval = g_ceph_context->_conf->get_val<double>(
        "rbd_mirror_delete_retry_interval");
      enqueue_failed_delete(&delete_info, r, failed_interval);
    }
  } else {
    complete_active_delete(&delete_info, 0);
  }

  // process the next queued image to delete
  remove_images();
}

template <typename I>
void ImageDeleter<I>::schedule_retry_timer() {
  assert(m_threads->timer_lock.is_locked());
  assert(m_lock.is_locked());
  if (!m_running || m_timer_ctx != nullptr || m_retry_delete_queue.empty()) {
    return;
  }

  dout(10) << dendl;
  auto &delete_info = m_retry_delete_queue.front();
  m_timer_ctx = new FunctionContext([this](int r) {
      handle_retry_timer();
    });
  m_threads->timer->add_event_at(delete_info->retry_time, m_timer_ctx);
}

template <typename I>
void ImageDeleter<I>::cancel_retry_timer() {
  dout(10) << dendl;
  assert(m_threads->timer_lock.is_locked());
  if (m_timer_ctx != nullptr) {
    bool canceled = m_threads->timer->cancel_event(m_timer_ctx);
    m_timer_ctx = nullptr;
    assert(canceled);
  }
}

template <typename I>
void ImageDeleter<I>::handle_retry_timer() {
  dout(10) << dendl;
  assert(m_threads->timer_lock.is_locked());
  Mutex::Locker locker(m_lock);

  assert(m_timer_ctx != nullptr);
  m_timer_ctx = nullptr;

  assert(m_running);
  assert(!m_retry_delete_queue.empty());

  // move all ready-to-ready items back to main queue
  utime_t now = ceph_clock_now();
  while (!m_retry_delete_queue.empty()) {
    auto &delete_info = m_retry_delete_queue.front();
    if (delete_info->retry_time > now) {
      break;
    }

    m_delete_queue.push_back(delete_info);
    m_retry_delete_queue.pop_front();
  }

  // schedule wake up for any future retries
  schedule_retry_timer();

  // start (concurrent) removal of images
  m_async_op_tracker.start_op();
  auto ctx = new FunctionContext([this](int r) {
      remove_images();
      m_async_op_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void ImageDeleter<I>::handle_trash_image(const std::string& image_id,
                                         const utime_t& deferment_end_time) {
  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  auto del_info = find_delete_info(image_id);
  if (del_info != nullptr) {
    dout(20) << "image " << image_id << " "
             << "was already scheduled for deletion" << dendl;
    return;
  }

  dout(10) << "image_id=" << image_id << ", "
           << "deferment_end_time=" << deferment_end_time << dendl;

  del_info.reset(new DeleteInfo(image_id));
  del_info->retry_time = deferment_end_time;
  m_retry_delete_queue.push_back(del_info);

  schedule_retry_timer();
}

template <typename I>
void ImageDeleter<I>::notify_on_delete(const std::string& image_id,
                                       int r) {
  dout(10) << "image_id=" << image_id << ", r=" << r << dendl;
  auto it = m_on_delete_contexts.find(image_id);
  if (it == m_on_delete_contexts.end()) {
    return;
  }

  it->second->complete(r);
  m_on_delete_contexts.erase(it);
}

template <typename I>
void ImageDeleter<I>::DeleteInfo::print_status(Formatter *f, stringstream *ss,
                                               bool print_failure_info) {
  if (f) {
    f->open_object_section("delete_info");
    f->dump_string("image_id", image_id);
    if (print_failure_info) {
      f->dump_string("error_code", cpp_strerror(error_code));
      f->dump_int("retries", retries);
    }
    f->close_section();
    f->flush(*ss);
  } else {
    *ss << *this;
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageDeleter<librbd::ImageCtx>;
