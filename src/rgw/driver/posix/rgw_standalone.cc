// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal.h"

extern "C" {

rgw::sal::Driver* newRadosStore(void* io_context, CephContext* cct)
{
  return NULL;
}

rgw::sal::Driver* newMotrStore(CephContext *cct)
{
  return NULL;
}

rgw::sal::Driver* newDaosStore(CephContext *cct)
{
  return NULL;
}

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, boost::asio::io_context& io_context, bool admin)
{
  return NULL;
}

}
