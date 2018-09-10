// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel <mahati.chamarthy@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRADOS_EPOLLEVENT_H
#define CEPH_LIBRADOS_EPOLLEVENT_H

#include <unistd.h>
#include <sys/epoll.h>
#include "include/Context.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/dout.h" 

class CephContext;

class EpollEvent{
  int size;

 public:
  struct epoll_event *events;
  int ep_fd;
  CephContext *cct;

  typedef void (*aio_callback_t)(void *arg);

  explicit EpollEvent(CephContext *c): size(0), events(NULL), ep_fd(-1), cct(c) {}
  //EpollEvent& operator=(const EpollEvent& e);
  ~EpollEvent() {
     /*if (ep_fd != -1)
       close(ep_fd);*/
  }

  struct FiredAIOEvent {
    int fd;
  };

  struct AIOEvent {
    aio_callback_t complete_cb;
    void *complete_arg;
  };

  vector<AIOEvent> aio_events;

  //int n_event = 5000;

  AIOEvent *_get_aio_event(int fd) {
    assert(fd < 5000);
    return &aio_events[fd];
  }

  int init(){
    size = 5000;
    events = (struct epoll_event*)malloc(sizeof(struct epoll_event)*size);
    if (!events) {
     return -ENOMEM;
    }
    memset(events, 0, sizeof(struct epoll_event)*size);

    ep_fd = epoll_create(1024);
    if (ep_fd == -1) {
      return -errno;
    }

    aio_events.resize(size);
    return 0;
  }

  template <typename T, void(T::*MF)(int)>
  static void aio_callback(void *arg) {
    T *obj = reinterpret_cast<T*>(arg);
    (obj->*MF)(0);
  }

  template <typename T, Context*(T::*MF)(int*), bool destroy>
  static void aio_callback(void *arg) {
    T *obj = reinterpret_cast<T*>(arg);
    int r = 0;
    Context *on_finish = (obj->*MF)(&r);
    if (on_finish != nullptr) {
      on_finish->complete(r);
      if (destroy) {
        delete obj;
      }
    }
  }

  template <typename T, void(T::*MF)(int)>
  void add_aiocallbacks(T *obj, int efd){
    EpollEvent::AIOEvent *event = _get_aio_event(efd);
    event->complete_cb = &EpollEvent::aio_callback<T, MF>;
    event->complete_arg = obj;
  }

  template <typename T, Context*(T::*MF)(int*), bool destroy>
  void add_aiocallbacks(T *obj, int efd){
    EpollEvent::AIOEvent *event = _get_aio_event(efd);
    event->complete_cb = &EpollEvent::aio_callback<T, MF, destroy>;
    event->complete_arg = obj;
  }

  void process_callbacks(int fd){
    EpollEvent::AIOEvent *event = _get_aio_event(fd);
    event->complete_cb(event->complete_arg);
  }

  int add_event(int fd){
   struct epoll_event ee;

   ee.events = EPOLLET;
   ee.events |= EPOLLIN;
   ee.data.u64 = 0; /* avoid valgrind warning */
   ee.data.fd = fd;
   if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, fd, &ee) == -1) {
     return -errno;
   }
   return 0;
  }

  int process_events(vector<FiredAIOEvent> &fired_events){
    int retval, numevents = 0;
    const unsigned timeout_microseconds = 30000000;
    struct timeval tv;
    tv.tv_sec = timeout_microseconds / 1000000;
    tv.tv_usec = timeout_microseconds % 1000000;

    retval = epoll_wait(ep_fd, events, size,
                       (tv.tv_sec*1000 + tv.tv_usec/1000));
    if (retval > 0) {
      numevents = retval;
      fired_events.resize(numevents);
      for (int j = 0; j < numevents; j++) {
        struct epoll_event *e = events + j;
        fired_events[j].fd = e->data.fd;
      }
    }

    return numevents;
  }

  int cleanup_events(int fd){
   struct epoll_event ee;

   ee.events = EPOLLET;
   ee.events |= EPOLLIN;
   ee.data.u64 = 0; /* avoid valgrind warning */
   ee.data.fd = fd;
   if (epoll_ctl(ep_fd, EPOLL_CTL_DEL, fd, &ee) < 0) {
     return -errno;
   }

   close(fd);
   return 0;
  }
 
};

#endif
