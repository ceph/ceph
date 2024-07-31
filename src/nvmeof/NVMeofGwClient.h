// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef  __NVMEOFGWCLIENT_H__
#define  __NVMEOFGWCLIENT_H__
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "gateway.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class NVMeofGwClient {
 public:
  NVMeofGwClient(std::shared_ptr<Channel> channel)
      : stub_(Gateway::NewStub(channel)) {}

  bool get_subsystems(subsystems_info& reply);
  bool set_ana_state(const ana_info& info);

 private:
  std::unique_ptr<Gateway::Stub> stub_;
};
#endif
