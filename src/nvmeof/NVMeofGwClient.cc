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

#include "NVMeofGwClient.h"

bool NVMeofGwClient::get_subsystems(subsystems_info& reply) {
  get_subsystems_req request;
  ClientContext context;

  Status status = stub_->get_subsystems(&context, request, &reply);

  return status.ok();
}

bool NVMeofGwClient::set_ana_state(const ana_info& info) {
  req_status reply;
  ClientContext context;

  Status status = stub_->set_ana_state(&context, info, &reply);

  return status.ok() && reply.status();
}
