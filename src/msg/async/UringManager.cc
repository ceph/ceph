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

#include "UringManager.h"
#include "Event.h"

class UringManager::SubmitCallback final : public EventCallback {
  UringManager &uring;

public:
  explicit SubmitCallback(UringManager &_uring) noexcept:uring(_uring) {}

  void do_request(uint64_t fd_or_id) override {
    uring.submit_timer_id = 0;
    uring.DoSubmit();
  }
};

class UringManager::CompletionCallback final : public EventCallback {
  UringManager &uring;

public:
  explicit CompletionCallback(UringManager &_uring) noexcept:uring(_uring) {}

  void do_request(uint64_t fd_or_id) override {
    uring.DispatchCompletions();
  }
};

UringManager::UringManager(EventCenter &_center)
  :Uring::Queue(1024, IORING_SETUP_SUBMIT_ALL|IORING_SETUP_COOP_TASKRUN|IORING_SETUP_TASKRUN_FLAG|IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN), center(_center),
   submit_handler(new SubmitCallback(*this)),
   completion_handler(new CompletionCallback(*this))
{
  center.create_file_event(GetFileDescriptor().Get(), EVENT_READABLE, completion_handler);
}

UringManager::~UringManager() noexcept
{
  center.delete_file_event(GetFileDescriptor().Get(), EVENT_READABLE);
  delete submit_handler;

  if (submit_timer_id != 0)
    center.delete_time_event(submit_timer_id);
  delete completion_handler;
}

void UringManager::Submit() {
  if (submit_timer_id != 0)
    return;

  submit_timer_id = center.create_time_event(0, submit_handler);
}

inline void UringManager::DoSubmit() noexcept try {
  Uring::Queue::Submit();
} catch (...) {
  // TODO?
}
