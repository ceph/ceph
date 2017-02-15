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
#include <boost/bind.hpp>
#include <map>
#include <set>
#include <sstream>

#include "include/rados/librados.hpp"
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "global/global_context.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/journal/Policy.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Utils.h"
#include "ImageDeleter.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageDeleter: " << this << " " \
                           << __func__ << ": "

using std::string;
using std::map;
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

class StatusCommand : public ImageDeleterAdminSocketCommand {
public:
  explicit StatusCommand(ImageDeleter *image_del) : image_del(image_del) {}

  bool call(Formatter *f, stringstream *ss) {
    image_del->print_status(f, ss);
    return true;
  }

private:
  ImageDeleter *image_del;
};

struct DeleteJournalPolicy : public librbd::journal::Policy {
  virtual bool append_disabled() const {
    return true;
  }
  virtual bool journal_disabled() const {
    return false;
  }

  virtual void allocate_tag_on_lock(Context *on_finish) {
    on_finish->complete(0);
  }
};

} // anonymous namespace

class ImageDeleterAdminSocketHook : public AdminSocketHook {
public:
  ImageDeleterAdminSocketHook(CephContext *cct, ImageDeleter *image_del) :
    admin_socket(cct->get_admin_socket()) {

    std::string command;
    int r;

    command = "rbd mirror deletion status";
    r = admin_socket->register_command(command, command, this,
				       "get status for image deleter");
    if (r == 0) {
      commands[command] = new StatusCommand(image_del);
    }

  }

  ~ImageDeleterAdminSocketHook() {
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
  typedef std::map<std::string, ImageDeleterAdminSocketCommand*> Commands;
  AdminSocket *admin_socket;
  Commands commands;
};

ImageDeleter::ImageDeleter(ContextWQ *work_queue, SafeTimer *timer,
                           Mutex *timer_lock)
  : m_running(1),
    m_work_queue(work_queue),
    m_delete_lock("rbd::mirror::ImageDeleter::Delete"),
    m_image_deleter_thread(this),
    m_failed_timer(timer),
    m_failed_timer_lock(timer_lock),
    m_asok_hook(new ImageDeleterAdminSocketHook(g_ceph_context, this))
{
  m_image_deleter_thread.create("image_deleter");
}

ImageDeleter::~ImageDeleter() {
  dout(20) << "enter" << dendl;

  m_running.set(0);
  {
    Mutex::Locker l (m_delete_lock);
    m_delete_queue_cond.Signal();
  }
  if (m_image_deleter_thread.is_started()) {
    m_image_deleter_thread.join();
  }

  delete m_asok_hook;
  dout(20) << "return" << dendl;
}

void ImageDeleter::run() {
  dout(20) << "enter" << dendl;
  while(m_running.read()) {
    m_delete_lock.Lock();
    while (m_delete_queue.empty()) {
      dout(20) << "waiting for delete requests" << dendl;
      m_delete_queue_cond.Wait(m_delete_lock);

      if (!m_running.read()) {
        m_delete_lock.Unlock();
        dout(20) << "return" << dendl;
        return;
      }
    }

    m_active_delete = std::move(m_delete_queue.back());
    m_delete_queue.pop_back();
    m_delete_lock.Unlock();

    bool move_to_next = process_image_delete();
    if (!move_to_next) {
      if (!m_running.read()) {
       dout(20) << "return" << dendl;
       return;
      }

      Mutex::Locker l(m_delete_lock);
      if (m_delete_queue.size() == 1) {
        m_delete_queue_cond.Wait(m_delete_lock);
      }
    }
  }
}

void ImageDeleter::schedule_image_delete(RadosRef local_rados,
                                         int64_t local_pool_id,
                                         const std::string& local_image_id,
                                         const std::string& local_image_name,
                                         const std::string& global_image_id) {
  dout(20) << "enter" << dendl;

  Mutex::Locker locker(m_delete_lock);

  auto del_info = find_delete_info(local_pool_id, global_image_id);
  if (del_info != nullptr) {
    dout(20) << "image " << local_image_name << " (" << global_image_id << ") "
             << "was already scheduled for deletion" << dendl;
    return;
  }

  m_delete_queue.push_front(unique_ptr<DeleteInfo>(
        new DeleteInfo(local_rados, local_pool_id, local_image_id,
                       local_image_name, global_image_id)));
  m_delete_queue_cond.Signal();
}

void ImageDeleter::wait_for_scheduled_deletion(int64_t local_pool_id,
                                               const std::string &global_image_id,
                                               Context *ctx,
                                               bool notify_on_failed_retry) {

  ctx = new FunctionContext([this, ctx](int r) {
      m_work_queue->queue(ctx, r);
    });

  Mutex::Locker locker(m_delete_lock);
  auto del_info = find_delete_info(local_pool_id, global_image_id);
  if (!del_info) {
    // image not scheduled for deletion
    ctx->complete(0);
    return;
  }

  if ((*del_info)->on_delete != nullptr) {
    (*del_info)->on_delete->complete(-ESTALE);
  }
  (*del_info)->on_delete = ctx;
  (*del_info)->notify_on_failed_retry = notify_on_failed_retry;
}

void ImageDeleter::cancel_waiter(int64_t local_pool_id,
                                 const std::string &global_image_id) {
  Mutex::Locker locker(m_delete_lock);
  auto del_info = find_delete_info(local_pool_id, global_image_id);
  if (!del_info) {
    return;
  }

  if ((*del_info)->on_delete != nullptr) {
    (*del_info)->on_delete->complete(-ECANCELED);
    (*del_info)->on_delete = nullptr;
  }
}

bool ImageDeleter::process_image_delete() {

  stringstream ss;
  m_active_delete->to_string(ss);
  std::string del_info_str = ss.str();
  dout(10) << "start processing delete request: " << del_info_str << dendl;
  int r;
  cls::rbd::MirrorImage mirror_image;

  // remote image was disabled, now we need to delete local image
  IoCtx ioctx;
  r = m_active_delete->local_rados->ioctx_create2(
    m_active_delete->local_pool_id, ioctx);
  if (r < 0) {
    derr << "error accessing local pool: " << cpp_strerror(r) << dendl;
    enqueue_failed_delete(r);
    return true;
  }

  dout(20) << "connected to local pool: " << ioctx.get_pool_name() << dendl;

  bool is_primary = false;
  r = Journal<>::is_tag_owner(ioctx, m_active_delete->local_image_id,
                              &is_primary);
  if (r < 0 && r != -ENOENT) {
    derr << "error retrieving image primary info: " << cpp_strerror(r)
         << dendl;
    enqueue_failed_delete(r);
    return true;
  }
  if (is_primary) {
    dout(10) << "local image is the primary image, aborting deletion..."
             << dendl;
    complete_active_delete(-EISPRM);
    return true;
  }

  dout(20) << "local image is not the primary" << dendl;

  bool has_snapshots;
  r = image_has_snapshots_and_children(&ioctx, m_active_delete->local_image_id,
                                       &has_snapshots);
  if (r < 0) {
    enqueue_failed_delete(r);
    return true;
  }

  mirror_image.global_image_id = m_active_delete->global_image_id;
  mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  r = cls_client::mirror_image_set(&ioctx, m_active_delete->local_image_id,
                                   mirror_image);
  if (r == -ENOENT) {
    dout(10) << "local image is not mirrored, aborting deletion..." << dendl;
    complete_active_delete(r);
    return true;
  } else if (r == -EEXIST || r == -EINVAL) {
    derr << "cannot disable mirroring for image id "
         << m_active_delete->local_image_id
         << ": global_image_id has changed/reused, aborting deletion: "
         << cpp_strerror(r) << dendl;
    complete_active_delete(r);
    return true;
  } else if (r < 0) {
    derr << "cannot disable mirroring for image id "
         << m_active_delete->local_image_id << ": " << cpp_strerror(r) << dendl;
    enqueue_failed_delete(r);
    return true;
  }

  dout(20) << "set local image mirroring to disable" << dendl;

  if (has_snapshots) {
    dout(20) << "local image has snapshots" << dendl;

    ImageCtx *imgctx = new ImageCtx("", m_active_delete->local_image_id,
                                    nullptr, ioctx, false);
    r = imgctx->state->open(false);
    if (r < 0) {
      derr << "error opening image id " << m_active_delete->local_image_id
           << ": " << cpp_strerror(r) << dendl;
      enqueue_failed_delete(r);
      delete imgctx;
      return true;
    }

    {
      RWLock::WLocker snap_locker(imgctx->snap_lock);
      imgctx->set_journal_policy(new DeleteJournalPolicy());
    }

    std::vector<librbd::snap_info_t> snaps;
    r = librbd::snap_list(imgctx, snaps);
    if (r < 0) {
      derr << "error listing snapshot of image " << imgctx->name
           << cpp_strerror(r) << dendl;
      imgctx->state->close();
      enqueue_failed_delete(r);
      return true;
    }

    for (const auto& snap : snaps) {
      dout(20) << "processing deletion of snapshot " << imgctx->name << "@"
               << snap.name << dendl;

      bool is_protected;
      r = librbd::snap_is_protected(imgctx, snap.name.c_str(), &is_protected);
      if (r < 0) {
        derr << "error checking snapshot protection of snapshot "
             << imgctx->name << "@" << snap.name << ": " << cpp_strerror(r)
             << dendl;
        imgctx->state->close();
        enqueue_failed_delete(r);
        return true;
      }
      if (is_protected) {
        dout(20) << "snapshot " << imgctx->name << "@" << snap.name
                 << " is protected, issuing unprotect command" << dendl;

        r = imgctx->operations->snap_unprotect(snap.name.c_str());
        if (r == -EBUSY) {
          // there are still clones of snapshots of this image, therefore send
          // the delete request to the end of the queue
          dout(10) << "local image id " << m_active_delete->local_image_id << " has "
                   << "snapshots with cloned children, postponing deletion..."
                   << dendl;
          imgctx->state->close();
          Mutex::Locker l(m_delete_lock);
          m_active_delete->notify(r);
          m_delete_queue.push_front(std::move(m_active_delete));
          return false;
        } else if (r < 0) {
          derr << "error unprotecting snapshot " << imgctx->name << "@"
               << snap.name << ": " << cpp_strerror(r) << dendl;
          imgctx->state->close();
          enqueue_failed_delete(r);
          return true;
        }
      }

      r = imgctx->operations->snap_remove(snap.name.c_str());
      if (r < 0) {
        derr << "error removing snapshot " << imgctx->name << "@"
             << snap.name << ": " << cpp_strerror(r) << dendl;
        imgctx->state->close();
        enqueue_failed_delete(r);
        return true;
      }

      dout(10) << "snapshot " << imgctx->name << "@" << snap.name
               << " was deleted" << dendl;
    }

    imgctx->state->close();
  }

  librbd::NoOpProgressContext ctx;
  r = librbd::remove(ioctx, "", m_active_delete->local_image_id, ctx, true);
  if (r < 0 && r != -ENOENT) {
    derr << "error removing image " << m_active_delete->local_image_name
         << " from local pool: " << cpp_strerror(r) << dendl;
    enqueue_failed_delete(r);
    return true;
  }

  // image was already deleted from rbd_directory, now we will make sure
  // that will be also removed from rbd_mirroring
  if (r == -ENOENT) {
    dout(20) << "local image does not exist, removing image from rbd_mirroring"
             << dendl;
  }

  r = cls_client::mirror_image_remove(&ioctx, m_active_delete->local_image_id);
  if (r < 0 && r != -ENOENT) {
    derr << "error removing image from mirroring directory: "
         << cpp_strerror(r) << dendl;
    enqueue_failed_delete(r);
    return true;
  }

  dout(10) << "Successfully deleted image: "
           << m_active_delete->local_image_name << dendl;

  complete_active_delete(0);
  return true;
}

int ImageDeleter::image_has_snapshots_and_children(IoCtx *ioctx,
                                                   string& image_id,
                                                   bool *has_snapshots) {

  string header_oid = librbd::util::header_name(image_id);
  ::SnapContext snapc;
  int r = cls_client::get_snapcontext(ioctx, header_oid, &snapc);
  if (r < 0 && r != -ENOENT) {
    derr << "error retrieving snapshot context for image id " << image_id
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  *has_snapshots = !snapc.snaps.empty();

  return 0;
}

void ImageDeleter::complete_active_delete(int r) {
  dout(20) << dendl;

  m_delete_lock.Lock();
  DeleteInfo *del_info = m_active_delete.release();
  assert(del_info != nullptr);
  m_delete_lock.Unlock();
  del_info->notify(r);
}

void ImageDeleter::enqueue_failed_delete(int error_code) {
  dout(20) << "enter" << dendl;

  if (error_code == -EBLACKLISTED) {
    derr << "blacklisted while deleting local image" << dendl;
    complete_active_delete(error_code);
    return;
  }

  m_delete_lock.Lock();
  if (m_active_delete->notify_on_failed_retry) {
    m_active_delete->notify(error_code);
  }
  m_active_delete->error_code = error_code;
  bool was_empty = m_failed_queue.empty();
  m_failed_queue.push_front(std::move(m_active_delete));
  m_delete_lock.Unlock();
  if (was_empty) {
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageDeleter::retry_failed_deletions, this));
    Mutex::Locker l(*m_failed_timer_lock);
    m_failed_timer->add_event_after(m_failed_interval, ctx);
  }
}

void ImageDeleter::retry_failed_deletions() {
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_delete_lock);

  bool empty = m_failed_queue.empty();
  while (!m_failed_queue.empty()) {
    m_delete_queue.push_back(std::move(m_failed_queue.back()));
    m_delete_queue.back()->retries++;
    m_failed_queue.pop_back();
  }
  if (!empty) {
    m_delete_queue_cond.Signal();
  }
}

unique_ptr<ImageDeleter::DeleteInfo> const* ImageDeleter::find_delete_info(
    int64_t local_pool_id, const std::string &global_image_id) {
  assert(m_delete_lock.is_locked());

  if (m_active_delete && m_active_delete->match(local_pool_id,
                                                global_image_id)) {
    return &m_active_delete;
  }

  for (const auto& del_info : m_delete_queue) {
    if (del_info->match(local_pool_id, global_image_id)) {
      return &del_info;
    }
  }

  for (const auto& del_info : m_failed_queue) {
    if (del_info->match(local_pool_id, global_image_id)) {
      return &del_info;
    }
  }

  return nullptr;
}

void ImageDeleter::print_status(Formatter *f, stringstream *ss) {
  dout(20) << "enter" << dendl;

  if (f) {
    f->open_object_section("image_deleter_status");
    f->open_array_section("delete_images_queue");
  }

  Mutex::Locker l(m_delete_lock);
  for (const auto& image : m_delete_queue) {
    image->print_status(f, ss);
  }

  if (f) {
    f->close_section();
    f->open_array_section("failed_deletes_queue");
  }

  for (const auto& image : m_failed_queue) {
    image->print_status(f, ss, true);
  }

  if (f) {
    f->close_section();
    f->close_section();
    f->flush(*ss);
  }
}

void ImageDeleter::DeleteInfo::notify(int r) {
  if (on_delete) {
    dout(20) << "executing image deletion handler r=" << r << dendl;

    Context *ctx = on_delete;
    on_delete = nullptr;
    ctx->complete(r);
  }
}

void ImageDeleter::DeleteInfo::to_string(stringstream& ss) {
  ss << "[" << "local_pool_id=" << local_pool_id << ", ";
  ss << "local_image_id=" << local_image_id << ", ";
  ss << "global_image_id=" << global_image_id << "]";
}

void ImageDeleter::DeleteInfo::print_status(Formatter *f, stringstream *ss,
                                            bool print_failure_info) {
  if (f) {
    f->open_object_section("delete_info");
    f->dump_int("local_pool_id", local_pool_id);
    f->dump_string("local_image_id", local_image_id);
    f->dump_string("global_image_id", global_image_id);
    if (print_failure_info) {
      f->dump_string("error_code", cpp_strerror(error_code));
      f->dump_int("retries", retries);
    }
    f->close_section();
    f->flush(*ss);
  } else {
    this->to_string(*ss);
  }
}

vector<string> ImageDeleter::get_delete_queue_items() {
  vector<string> items;

  Mutex::Locker l(m_delete_lock);
  for (const auto& del_info : m_delete_queue) {
    items.push_back(del_info->local_image_name);
  }

  return items;
}

vector<pair<string, int> > ImageDeleter::get_failed_queue_items() {
  vector<pair<string, int> > items;

  Mutex::Locker l(m_delete_lock);
  for (const auto& del_info : m_failed_queue) {
    items.push_back(make_pair(del_info->local_image_name,
                              del_info->error_code));
  }

  return items;
}

void ImageDeleter::set_failed_timer_interval(double interval) {
  this->m_failed_interval = interval;
}

} // namespace mirror
} // namespace rbd
