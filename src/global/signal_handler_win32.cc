// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2019 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "global/signal_handler.h"

void install_sighandler(int signum, signal_handler_t handler, int flags) {}
void sighup_handler(int signum) {}

// Install the standard Ceph signal handlers
void install_standard_sighandlers(void){}

/// initialize async signal handler framework
void init_async_signal_handler(){}

/// shutdown async signal handler framework
void shutdown_async_signal_handler(){}

/// queue an async signal
void queue_async_signal(int signum){}

/// install a safe, async, callback for the given signal
void register_async_signal_handler(int signum, signal_handler_t handler){}
void register_async_signal_handler_oneshot(int signum, signal_handler_t handler){}

/// uninstall a safe async signal callback
void unregister_async_signal_handler(int signum, signal_handler_t handler){}
