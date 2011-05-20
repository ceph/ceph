// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LOCKLESS_QUEUE_H
#define CEPH_LOCKLESS_QUEUE_H

#include <unistd.h>

/* A simple signal-safe queue. It uses pipe2 internally. */

class SignalSafeQueue
{
public:
  static SignalSafeQueue* create_queue();

  ~SignalSafeQueue();

  /* Unblock all readers.
   * After this function has been called, no further push() operations may be
   * done. However, the memory for the class will continue to exist until the
   * destructor is called.
   *
   * Assumes no concurrent writers exist while we are shutting down.
   */
  void wake_readers_and_shutdown();

  /* Initialize the queue. Item size is set to item_sz.
   *
   * Returns: 0 on success; error code otherwise.
   */
  int init(size_t item_sz);

  /* Puts an item into the queue using a blocking write().
   * This is safe to call from a signal handler.
   *
   * It is assumed that buf is a buffer of length 'item_sz'
   *
   * This function is reentrant and any number of writers and readers can
   * exist.
   */
  void push(void *buf);

  /* Blocks until there is something available in the queue.
   * When it is available, it will be copied into the provided buffer,
   * which we assume is of length 'item_sz'
   *
   * This function is reentrant and any number of writers and readers can
   * exist.
   *
   * Returns: 0 on success
   *          EPIPE if the queue has been shut down
   *	      Other error codes if an unexpected error has occurred.
   */
  int pop(void *buf);

private:
  /* Force heap allocation. */
  SignalSafeQueue();

  size_t _item_sz;
  int _fd[2];
};

#endif
