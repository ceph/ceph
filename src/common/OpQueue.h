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

#ifndef OP_QUEUE_H
#define OP_QUEUE_H

#include "include/msgr.h"
#include "osd/osd_types.h"

#include <list>
#include <functional>

namespace ceph {
  class Formatter;
}

/**
 * Abstract class for all Op Queues
 *
 * In order to provide optimized code, be sure to declare all
 * virtual functions as final in the derived class.
 */

template <typename T, typename K>
class OpQueue {
public:
  // Ops of this class should be deleted immediately. If out isn't
  // nullptr then items should be added to the front in
  // front-to-back order. The typical strategy is to visit items in
  // the queue in *reverse* order and to use *push_front* to insert
  // them into out.
  virtual void remove_by_class(K k, std::list<T> *out) = 0;

  // Enqueue op in the back of the strict queue
  virtual void enqueue_strict(K cl, unsigned priority, T &&item) = 0;

  // Enqueue op in the front of the strict queue
  virtual void enqueue_strict_front(K cl, unsigned priority, T &&item) = 0;

  // Enqueue op in the back of the regular queue
  virtual void enqueue(K cl, unsigned priority, unsigned cost, T &&item) = 0;

  // Enqueue the op in the front of the regular queue
  virtual void enqueue_front(
    K cl, unsigned priority, unsigned cost, T &&item) = 0;

  // Returns if the queue is empty
  virtual bool empty() const = 0;

  // Return an op to be dispatch
  virtual T dequeue() = 0;

  // Formatted output of the queue
  virtual void dump(ceph::Formatter *f) const = 0;

  // Human readable brief description of queue and relevant parameters
  virtual void print(std::ostream &f) const = 0;

  // Get the type of OpQueue implementation
  virtual op_queue_type_t get_type() const = 0;

  // Don't leak resources on destruction
  virtual ~OpQueue() {};
};

#endif
