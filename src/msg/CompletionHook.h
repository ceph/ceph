// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifndef CEPH_MESSAGE_COMPLETION_HOOK_H
#define CEPH_MESSAGE_COMPLETION_HOOK_H

#include "Message.h"

#include "include/Context.h"

class Message::CompletionHook : public Context {
protected:
  Message *m;
  friend class Message;
public:
  explicit CompletionHook(Message *_m) : m(_m) {}
  virtual void set_message(Message *_m) { m = _m; }
};

#endif
