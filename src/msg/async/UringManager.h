// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_URING_MANAGER_H
#define CEPH_URING_MANAGER_H

#include "Event.h"
#include "io/uring/Queue.hxx"

class EventCenter;

class UringManager final : public Uring::Queue {
  EventCenter &center;

  class SubmitCallback;
  class CompletionCallback;
  const EventCallbackRef submit_handler, completion_handler;

  uint64_t submit_timer_id = 0;

public:
  explicit UringManager(EventCenter &_center);
  ~UringManager() noexcept;

  // virtual methods from class Uring::Queue
  void Submit() override;

private:
  void DoSubmit() noexcept;
};

#endif
