// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef  __NVMEOFGWMONITORGROUPCLIENT_H__
#define  __NVMEOFGWMONITORGROUPCLIENT_H__
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "monitor.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class NVMeofGwMonitorGroupClient {
 public:
  NVMeofGwMonitorGroupClient(std::shared_ptr<Channel> channel)
      : stub_(MonitorGroup::NewStub(channel)) {}

  bool set_group_id(const uint32_t& id);

 private:
  std::unique_ptr<MonitorGroup::Stub> stub_;
};
#endif
