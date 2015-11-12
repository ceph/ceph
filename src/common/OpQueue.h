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

#include "common/Formatter.h"

#include <list>

/**
 * Abstract class for all Op Queues
 *
 * In order to provide optimized code, be sure to declare all
 * virutal functions as final in the derived class.
 */

template <typename T, typename K>
class OpQueue {

  public:
    // How many Ops are in the queue
    virtual unsigned length() const = 0;
    // Ops will be removed and placed in *removed if f is true
    virtual void remove_by_filter(
	std::function<bool (T)> f, list<T> *removed = 0) = 0;
    // Ops of this priority should be deleted immediately
    virtual void remove_by_class(K k, list<T> *out = 0) = 0;
    // Queue this Op (peering, etc) to be run before "normal OPs",
    // queued in back based on it's priority
    virtual void enqueue_strict(
	K cl, unsigned priority, T item) = 0;
    // Queue this Op to be dispatched before any other ops of
    // this priority
    virtual void enqueue_strict_front(
	K cl, unsigned priority, T item) = 0;
    // Queue Op
    virtual void enqueue(
	K cl, unsigned priority, unsigned cost, T item) = 0;
    // Queue Op in front of other Ops in this priority
    virtual void enqueue_front(
	K cl, unsigned priority, unsigned cost, T item) = 0;
    // Returns if the queue is empty
    virtual bool empty() const = 0;
    // Return an op to be dispatch
    virtual T dequeue() = 0;
    // Formatted output of the queue
    virtual void dump(Formatter *f) const = 0;
    // Don't leak resources on destruction
    virtual ~OpQueue() {}; 
};

#endif
