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

#include "NVMeofGwMonitorGroupClient.h"

bool NVMeofGwMonitorGroupClient::set_group_id(const uint32_t& id) {
  group_id_req request;
  request.set_id(id);
  google::protobuf::Empty reply;
  ClientContext context;

  Status status = stub_->group_id(&context, request, &reply);

  return status.ok();
}
