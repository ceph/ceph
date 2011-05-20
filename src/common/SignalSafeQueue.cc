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

#include "common/config.h"
#include "common/SignalSafeQueue.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

SignalSafeQueue *SignalSafeQueue::
create_queue()
{
  return new SignalSafeQueue();
}

SignalSafeQueue::
SignalSafeQueue()
  : _item_sz(0)
{
  _fd[0] = -1;
  _fd[1] = -1;
}

SignalSafeQueue::
~SignalSafeQueue()
{
  if (_fd[0] != -1) {
    TEMP_FAILURE_RETRY(close(_fd[0]));
    _fd[0] = -1;
  }
  if (_fd[1] != -1) {
    TEMP_FAILURE_RETRY(close(_fd[1]));
    _fd[1] = -1;
  }
}

void SignalSafeQueue::
wake_readers_and_shutdown(void)
{
  /* Close write file descriptor.
   * Readers will get EPIPE. */
  TEMP_FAILURE_RETRY(close(_fd[1]));
  _fd[1] = -1;
}

int SignalSafeQueue::
init(size_t item_sz)
{
  int ret;
  assert(_fd[0] < 0);
  assert(_fd[1] < 0);
  assert(_item_sz < PIPE_BUF);

  _item_sz = item_sz;
  ret = pipe2(_fd, O_CLOEXEC);
  if (ret)
    return ret;
  return 0;
}

void SignalSafeQueue::
push(void *buf)
{
  /* Writing less than PIPE_BUF bytes to the pipe should always be atomic */
  int ret = write(_fd[1], buf, _item_sz);
  assert(ret == (int)_item_sz);
}

int SignalSafeQueue::
pop(void *buf)
{
  int ret;
  /* Read less than PIPE_BUF bytes from the pipe should always be atomic */
  ret = read(_fd[0], buf, _item_sz);
  if (ret < 0) {
    ret = errno;
    if (ret == EPIPE) {
      /* EPIPE means that the other side has closed the pipe */
      return ret;
    }
    derr << "SignalSafeQueue::dequeue() failed with error " << ret << dendl;
    return ret;
  }
  else if (ret == 0) {
    return EPIPE;
  }
  else if (ret != (int)_item_sz) {
    derr << "SignalSafeQueue::dequeue() only read " << ret << " bytes (should "
         << "not be possible)" << dendl;
    return EIO; 
  }
  return 0;
}
