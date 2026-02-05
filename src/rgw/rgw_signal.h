// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once


namespace rgw {
class AppMain; // forward declaration

namespace signal {

void sig_handler_noop(int signum);
void signal_shutdown();
void wait_shutdown();
int signal_fd_init();
void signal_fd_finalize();
void handle_sigterm(int signum);
void handle_sigterm(int signum);
void sighup_handler(int signum);
void handle_sigpause(int signum);
void handle_sigresume(int signum);
void set_app_main(rgw::AppMain* app_main);

} // namespace signal
} // namespace rgw
