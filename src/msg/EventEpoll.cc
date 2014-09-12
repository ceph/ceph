#include "common/errno.h"
#include "EventEpoll.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "EpollDriver."

int EpollDriver::init(int nevent)
{
  events = (struct epoll_event*)malloc(sizeof(struct epoll_event)*nevent);
  if (!events) {
    lderr(cct) << __func__ << " unable to malloc memory: "
                           << cpp_strerror(errno) << dendl;
    return -errno;
  }
  memset(events, 0, sizeof(struct epoll_event)*nevent);

  epfd = epoll_create(1024); /* 1024 is just an hint for the kernel */
  if (epfd == -1) {
    lderr(cct) << __func__ << " unable to do epoll_create: "
                       << cpp_strerror(errno) << dendl;
    return -errno;
  }

  return 0;
}

int EpollDriver::add_event(int fd, int cur_mask, int add_mask)
{
  struct epoll_event ee;
  /* If the fd was already monitored for some event, we need a MOD
   * operation. Otherwise we need an ADD operation. */
  int op, pos;
  map<int, int>::iterator it = fds.find(fd);
  if (it == fds.end()) {
    op = EPOLL_CTL_ADD;
    if (deleted_fds.size()) {
      pos = deleted_fds.front();
      deleted_fds.pop_front();
    } else {
      pos = next_pos;
      next_pos++;
    }
    fds[fd] = pos;
  } else {
    op = cur_mask == EVENT_NONE ? EPOLL_CTL_ADD: EPOLL_CTL_MOD;
  }

  ee.events = 0;
  add_mask |= cur_mask; /* Merge old events */
  if (add_mask & EVENT_READABLE)
    ee.events |= EPOLLIN;
  if (add_mask & EVENT_WRITABLE)
    ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (epoll_ctl(epfd, op, fd, &ee) == -1) {
    lderr(cct) << __func__ << " unable to add event: "
                       << cpp_strerror(errno) << dendl;
    return -errno;
  }

  ldout(cct, 10) << __func__ << " add event to fd=" << fd << " mask=" << add_mask
                 << dendl;
  return 0;
}

void EpollDriver::del_event(int fd, int cur_mask, int delmask)
{
  struct epoll_event ee;
  map<int, int>::iterator it = fds.find(fd);
  if (it == fds.end())
    return ;

  int mask = cur_mask & (~delmask);

  ee.events = 0;
  if (mask & EVENT_READABLE) ee.events |= EPOLLIN;
  if (mask & EVENT_WRITABLE) ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (mask != EVENT_NONE) {
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: modify fd=" << fd << " mask=" << mask
                 << " failed." << cpp_strerror(errno) << dendl;
    }
  } else {
    /* Note, Kernel < 2.6.9 requires a non null event pointer even for
     * EPOLL_CTL_DEL. */
    if (epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &ee) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: delete fd=" << fd
                 << " failed." << cpp_strerror(errno) << dendl;
    }

    if (next_pos == it->second)
      next_pos--;
    else
      deleted_fds.push_back(it->second);
    fds.erase(fd);
  }
  ldout(cct, 10) << __func__ << " del event fd=" << fd << " cur mask=" << mask
                 << dendl;
}

int EpollDriver::resize_events(int newsize)
{
  events = (struct epoll_event*)realloc(events, sizeof(struct epoll_event)*newsize);
  return 0;
}

int EpollDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
  int retval, numevents = 0;

  retval = epoll_wait(epfd, events, next_pos,
                      tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
  if (retval > 0) {
    int j;

    numevents = retval;
    fired_events.resize(numevents);
    for (j = 0; j < numevents; j++) {
      int mask = 0;
      struct epoll_event *e = events + j;

      if (e->events & EPOLLIN) mask |= EVENT_READABLE;
      if (e->events & EPOLLOUT) mask |= EVENT_WRITABLE;
      if (e->events & EPOLLERR) mask |= EVENT_WRITABLE;
      if (e->events & EPOLLHUP) mask |= EVENT_WRITABLE;
      fired_events[j].fd = e->data.fd;
      fired_events[j].mask = mask;
    }
  }
  return numevents;
}
