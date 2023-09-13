
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

enum RGWMDLogSyncType {
  APPLY_ALWAYS,
  APPLY_UPDATES,
  APPLY_NEWER,
  APPLY_EXCLUSIVE
};

enum RGWMDLogStatus {
  MDLOG_STATUS_UNKNOWN,
  MDLOG_STATUS_WRITE,
  MDLOG_STATUS_SETATTRS,
  MDLOG_STATUS_REMOVE,
  MDLOG_STATUS_COMPLETE,
  MDLOG_STATUS_ABORT,
};

