// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_PIDFILE_H
#define CEPH_COMMON_PIDFILE_H

struct md_config_t;

// Write a pidfile with the current pid, using the configuration in the
// provided conf structure.
int pidfile_write(const md_config_t *conf);

// Remove the pid file that was previously written by pidfile_write.
// This is safe to call in a signal handler context.
void pidfile_remove();

#endif
