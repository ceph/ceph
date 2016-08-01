// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "msg/Message.h"
#include "Pipe.h"
#include "SimpleMessenger.h"
#include "PipeConnection.h"

PipeConnection::~PipeConnection()
{
  if (pipe) {
    pipe->put();
    pipe = NULL;
  }
}

Pipe* PipeConnection::get_pipe()
{
  Mutex::Locker l(lock);
  if (pipe)
    return pipe->get();
  return NULL;
}

bool PipeConnection::try_get_pipe(Pipe **p)
{
  Mutex::Locker l(lock);
  if (failed) {
    *p = NULL;
  } else {
    if (pipe)
      *p = pipe->get();
    else
      *p = NULL;
  }
  return !failed;
}

bool PipeConnection::clear_pipe(Pipe *old_p)
{
  Mutex::Locker l(lock);
  if (old_p == pipe) {
    pipe->put();
    pipe = NULL;
    failed = true;
    return true;
  }
  return false;
}

void PipeConnection::reset_pipe(Pipe *p)
{
  Mutex::Locker l(lock);
  if (pipe)
    pipe->put();
  pipe = p->get();
}

bool PipeConnection::is_connected()
{
  return static_cast<SimpleMessenger*>(msgr)->is_connected(this);
}

int PipeConnection::send_message(Message *m)
{
  assert(msgr);
  return static_cast<SimpleMessenger*>(msgr)->send_message(m, this);
}

void PipeConnection::send_keepalive()
{
  static_cast<SimpleMessenger*>(msgr)->send_keepalive(this);
}

void PipeConnection::mark_down()
{
  if (msgr)
    static_cast<SimpleMessenger*>(msgr)->mark_down(this);
}

void PipeConnection::mark_disposable()
{
  if (msgr)
    static_cast<SimpleMessenger*>(msgr)->mark_disposable(this);
}
