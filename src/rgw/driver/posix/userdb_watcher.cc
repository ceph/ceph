// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "userdb_watcher.h"

#ifdef __linux__

#include <sys/inotify.h>
#include <sys/eventfd.h>
#include <poll.h>
#include <unistd.h>
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw::sal {

void UserDBWatcher::watch_loop()
{
  constexpr size_t buf_size = 4096;
  char buf[buf_size]
    __attribute__((aligned(__alignof__(struct inotify_event))));

  struct pollfd fds[2] = {{inotify_fd, POLLIN, 0}, {event_fd, POLLIN, 0}};

  while (!shutdown.load(std::memory_order_acquire)) {
    int ret = poll(fds, 2, -1);
    if (shutdown.load(std::memory_order_acquire)) {
      return;
    }
    if (ret == -1) {
      if (errno == EINTR) {
        continue;
      }
      ldpp_dout(&dp, 0) << "poll failed: " << cpp_strerror(errno) << dendl;
      return;
    }
    if (ret > 0 && (fds[0].revents & POLLIN)) {
      ssize_t len = read(inotify_fd, buf, buf_size);
      if (len <= 0) {
        continue;
      }
      bool invalidated = false;
      for (char* ptr = buf; ptr < buf + len; ) {
        auto* event = reinterpret_cast<struct inotify_event*>(ptr);

        if (event->mask & IN_MODIFY) {
            ldpp_dout(&dp, 20) << "UserCacheWatcher received event MODIFY" << dendl;

        } else if (event->mask & IN_DELETE_SELF) {
            ldpp_dout(&dp, 20) << "UserCacheWatcher received event DELETE_SELF " << dendl;

        } else if (event->mask & IN_MOVE_SELF) {
            ldpp_dout(&dp, 20) << "UserCacheWatcher received event MOVE_SELF" << dendl;

        }
        if ((event->mask & IN_MODIFY) ||
            (event->mask & IN_DELETE_SELF) ||
            (event->mask & IN_MOVE_SELF)) {
          if (!invalidated) {
            ldpp_dout(&dp, 20) << "userDB file modified, invalidating UserCache"
              << dendl;
            user_cache.invalidate_all(&dp);
            invalidated = true;
          }
        }
        if (event->mask & (IN_DELETE_SELF | IN_MOVE_SELF)) {
          watch_fd = inotify_add_watch(
            inotify_fd, db_path.c_str(),
            IN_CLOSE_WRITE | IN_MODIFY | IN_DELETE_SELF | IN_MOVE_SELF);
        }
        ptr += sizeof(struct inotify_event) + event->len;
      }
    }
  }
}

int UserDBWatcher::start(const DoutPrefixProvider* dpp)
{
  inotify_fd = inotify_init1(IN_NONBLOCK);
  if (inotify_fd == -1) {
    ldpp_dout(dpp, 0) << "UserDBWatcher: inotify_init1 failed: "
      << cpp_strerror(errno) << dendl;
    return -errno;
  }

  event_fd = eventfd(0, EFD_NONBLOCK);
  if (event_fd == -1) {
    ldpp_dout(dpp, 0) << "UserDBWatcher: eventfd failed: "
      << cpp_strerror(errno) << dendl;
    ::close(inotify_fd);
    inotify_fd = -1;
    return -errno;
  }

  watch_fd = inotify_add_watch(
    inotify_fd, db_path.c_str(),
    IN_MODIFY | IN_DELETE_SELF | IN_MOVE_SELF);
  if (watch_fd == -1) {
    ldpp_dout(dpp, 0) << "UserDBWatcher: inotify_add_watch failed for "
      << db_path << ": " << cpp_strerror(errno) << dendl;
    ::close(event_fd);
    ::close(inotify_fd);
    event_fd = -1;
    inotify_fd = -1;
    return -errno;
  }

  ldpp_dout(dpp, 1) << "UserDBWatcher: watching " << db_path << dendl;
  thrd = std::thread(&UserDBWatcher::watch_loop, this);
  return 0;
}

void UserDBWatcher::stop()
{
  if (!shutdown.exchange(true, std::memory_order_release)) {
    if (event_fd >= 0) {
      uint64_t val = 1;
      std::ignore = write(event_fd, &val, sizeof(val));
    }
    if (thrd.joinable()) {
      thrd.join();
    }
    if (watch_fd >= 0) {
      inotify_rm_watch(inotify_fd, watch_fd);
    }
    if (inotify_fd >= 0) {
      ::close(inotify_fd);
    }
    if (event_fd >= 0) {
      ::close(event_fd);
    }
  }
}

} // namespace rgw::sal

#endif // __linux__
