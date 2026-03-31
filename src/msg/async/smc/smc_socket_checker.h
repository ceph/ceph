// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * SMC Socket Checker Platform Selector
 *
 * Provides platform-independent interface by selecting the appropriate
 * implementation based on the target platform.
 *
 * Author(s): Aliaksei Makarau <aliaksei.makarau@ibm.com>
 * 
 * Copyright IBM Corp. 2026
 */
#ifndef CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_H
#define CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_H

#include "smc_socket_checker_base.h"

#ifdef __linux__
  // Linux implementation using netlink
  #include "smc_socket_checker_linux.h"
  using SmcSocketChecker = SmcSocketCheckerLinux;
#else
  // No-op implementation for non-Linux platforms
  #include "smc_socket_checker_noop.h"
  using SmcSocketChecker = SmcSocketCheckerNoop;
#endif

#endif /* CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_H */
