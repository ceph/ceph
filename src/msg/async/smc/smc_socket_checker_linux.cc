// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "smc_socket_checker_linux.h"

#define MAGIC_SEQ 123456

SmcSocketCheckerLinux::SmcSocketCheckerLinux() 
  : m_netlinkHandler(std::make_unique<NetlinkHandler>()) {
}

SmcSocketCheckerLinux::~SmcSocketCheckerLinux() {
  // Unique_ptr will automatically clean up
}

SmcSocketCheckerLinux::SmcSocketCheckerLinux(SmcSocketCheckerLinux&& other) noexcept
  : m_netlinkHandler(std::move(other.m_netlinkHandler)),
    m_stats(other.m_stats) {
}

SmcSocketCheckerLinux& SmcSocketCheckerLinux::operator=(SmcSocketCheckerLinux&& other) noexcept {
  if (this != &other) {
    m_netlinkHandler = std::move(other.m_netlinkHandler);
    m_stats = other.m_stats;
  }
  return *this;
}

void SmcSocketCheckerLinux::processSocket(struct nlmsghdr *nlh) {
  struct smc_diag_msg *r = (smc_diag_msg *) NLMSG_DATA(nlh);
  struct rtattr *tb[SMC_DIAG_MAX + 1];

  parse_rtattr(tb, SMC_DIAG_MAX, (struct rtattr *)(r + 1), nlh->nlmsg_len - NLMSG_LENGTH(sizeof(*r)));
  m_stats.total_sockets++;

  // Check if this is a fallback connection
  if (r->diag_mode == SMC_DIAG_MODE_FALLBACK_TCP) {
    m_stats.fallback_count++;
  }
}

int SmcSocketCheckerLinux::updateStatistics() {
  m_stats.reset();

  if (!m_netlinkHandler->isOpen()) {
    int rc = m_netlinkHandler->open();
    if (rc != 0) {
      return EXIT_FAILURE;
    }
  }

  // Set dump sequence
  m_netlinkHandler->setDumpSeq(MAGIC_SEQ);

  // Request socket diagnostics with fallback information
  int rc = m_netlinkHandler->sendDiagRequest(1 << (SMC_DIAG_FALLBACK - 1));
  if (rc != 0) {
    return EXIT_FAILURE;
  }

  rc = m_netlinkHandler->dump([this](struct nlmsghdr* nlh) {
    this->processSocket(nlh);
  });

  if (rc < 0) {
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

const SmcSocketStats& SmcSocketCheckerLinux::getStatistics() const {
  return m_stats;
}

void SmcSocketCheckerLinux::resetStatistics() {
  m_stats.reset();
}
