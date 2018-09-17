// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once
#include "include/types.h"

class MDSRank;

class OpQueueable {
public:
  OpQueueable() = default;
  OpQueueable(OpQueueable&&) = default;
  OpQueueable(const OpQueueable&) = delete;
  virtual ~OpQueueable() {}
  OpQueueable& operator=(const OpQueueable&) = delete;
  virtual void set_seq(uint64_t s) {}
  virtual uint64_t get_seq() const { return 0; }
  virtual uint32_t get_queue_token() const = 0;
  virtual OpQueueable* move_to(char *buf) = 0;
  virtual void run(MDSRank *mds) = 0;
};

class OpQueueItem {
public:
  static const size_t INLINE_BUF_SIZE = 32;

  OpQueueItem() { op = nullptr; }
  OpQueueItem(OpQueueable&& _op) {
    op = _op.move_to(inline_buf);
  }
  OpQueueItem(OpQueueItem&& other) {
    op = other.op->move_to(inline_buf);
  }
  OpQueueItem(const OpQueueItem&) = delete;
  OpQueueItem& operator=(OpQueueItem&& other) {
    if (op)
      op->~OpQueueable();
    op = other.op->move_to(inline_buf);
    return *this;
  }
  OpQueueItem& operator=(const OpQueueItem&) = delete;
  ~OpQueueItem() {
    if (op)
      op->~OpQueueable();
  }

  OpQueueable& get_op() { return *op; }
  uint64_t get_seq() const { return op->get_seq(); }
  bool operator<(const OpQueueItem& other) const {
    return get_seq() < other.get_seq();
  }
private:
  OpQueueable *op;
  char inline_buf[INLINE_BUF_SIZE];
};

template<class T>
class OpQueueableType : public OpQueueable {
public:
  virtual OpQueueable* move_to(char *buf) override {
    static_assert(sizeof(T) < OpQueueItem::INLINE_BUF_SIZE,
                  "inline buffer too small");
    return new (buf) T(std::move(*static_cast<T*>(this)));
  }
};
