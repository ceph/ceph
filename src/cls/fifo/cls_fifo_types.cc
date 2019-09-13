// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "cls_fifo_types.h"


string rados::cls::fifo::fifo_info_t::next_part_oid()
{
  auto part = next_part();

  char buf[oid_prefix.size() + 32];
  snprintf(buf, sizeof(buf), "%s.%lld", oid_prefix.c_str(), (long long)part);

  return string(buf);
}

