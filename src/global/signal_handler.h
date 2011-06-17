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

#ifndef CEPH_GLOBAL_SIGNAL_HANDLER_H
#define CEPH_GLOBAL_SIGNAL_HANDLER_H

#include <signal.h>
#include <string>

typedef void (*signal_handler_t)(int);

void install_sighandler(int signum, signal_handler_t handler, int flags);

// handles SIGHUP
void sighup_handler(int signum);

// Install the standard Ceph signal handlers
void install_standard_sighandlers(void);

#endif
