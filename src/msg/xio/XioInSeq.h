// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_IN_SEQ_H
#define XIO_IN_SEQ_H

#include <boost/intrusive/list.hpp>
#include "msg/SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}

/* For inbound messages (Accelio-owned) ONLY, use the message's
 * user_context as an SLIST */
class XioInSeq {
private:
  int cnt;
  int sz;
  struct xio_msg* head;
  struct xio_msg* tail;

public:
  XioInSeq() : cnt(0), sz(0), head(NULL), tail(NULL) {}
  XioInSeq(const XioInSeq& seq) {
    cnt = seq.cnt;
    sz = seq.sz;
    head = seq.head;
    tail = seq.tail;
  }

  int count() { return cnt; }

  int size() { return sz; }

  bool p() { return !!head; }

  void set_count(int _cnt) { cnt = _cnt; }

  void append(struct xio_msg* msg) {
    msg->user_context = NULL;
    if (!head) {
      head = tail = msg;
    } else {
      tail->user_context = msg;
      tail = msg;
    }
    ++sz;
    --cnt;
  }

  struct xio_msg* begin() { return head; }

  struct xio_msg* end() { return NULL; }

  void next(struct xio_msg** msg) {
    *msg = static_cast<struct xio_msg *>((*msg)->user_context);
  }

  struct xio_msg* dequeue() {
    struct xio_msg* msgs = head;
    clear();
    return msgs;
  }

  void clear() {
    head = tail = NULL;
    cnt = 0;
    sz = 0;
  }
};

#endif /* XIO_IN_SEQ_H */
