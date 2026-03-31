// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * SMC Socket Checker Linux Implementation
 *
 * Linux-specific implementation for checking SMC fallback connections.
 * Based on smc-tools code.
 *
 * Author(s): Aliaksei Makarau <aliaksei.makarau@ibm.com>
 * 
 * Copyright IBM Corp. 2026
 */
#ifndef CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_LINUX_H
#define CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_LINUX_H

#include <memory>

#include "smc_socket_checker_base.h"
#include "libnetlink.h"
#include "smctools_common.h"

/**
 * @brief Linux implementation of SMC Socket Checker
 * 
 * This class provides Linux-specific functionality to check SMC socket
 * connections using netlink, identify fallback connections, and gather
 * statistics about SMC usage.
 */
class SmcSocketCheckerLinux : public SmcSocketCheckerBase {
public:
  SmcSocketCheckerLinux();
  ~SmcSocketCheckerLinux() override;

  SmcSocketCheckerLinux(SmcSocketCheckerLinux&& other) noexcept;
  SmcSocketCheckerLinux& operator=(SmcSocketCheckerLinux&& other) noexcept;

  int updateStatistics() override;
  const SmcSocketStats& getStatistics() const override;
  void resetStatistics() override;

private:
  /**
   * @brief Process a single socket from netlink message
   * 
   * @param nlh Pointer to netlink message header
   */
  void processSocket(struct nlmsghdr *nlh);

  std::unique_ptr<NetlinkHandler> m_netlinkHandler;
  SmcSocketStats m_stats;
};

#endif /* CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_LINUX_H */
