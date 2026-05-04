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

#include "smc_socket_checker.h"

#if defined(AF_SMC)
  #define MAGIC_SEQ 123456

  SmcSocketChecker::SmcSocketChecker() noexcept
    : m_netlinkHandler(std::make_unique<NetlinkHandler>()) {
  }

  SmcSocketChecker::~SmcSocketChecker() {
    // Unique_ptr will automatically clean up
  }

  SmcSocketChecker::SmcSocketChecker(SmcSocketChecker&& other) noexcept
    : m_netlinkHandler(std::move(other.m_netlinkHandler)),
      m_stats(other.m_stats) {
  }

  SmcSocketChecker& SmcSocketChecker::operator=(SmcSocketChecker&& other) noexcept {
    if (this != &other) {
      m_netlinkHandler = std::move(other.m_netlinkHandler);
      m_stats = other.m_stats;
    }
    return *this;
  }

  void SmcSocketChecker::processSocket(struct nlmsghdr *nlh) {
    struct smc_diag_msg *r = (smc_diag_msg *) NLMSG_DATA(nlh);
    struct rtattr *tb[SMC_DIAG_MAX + 1];
  
    parse_rtattr(tb, SMC_DIAG_MAX, (struct rtattr *)(r + 1), nlh->nlmsg_len - NLMSG_LENGTH(sizeof(*r)));
    m_stats.total_sockets++;
  
    // Check if this is a fallback connection
    if (r->diag_mode == SMC_DIAG_MODE_FALLBACK_TCP) {
      m_stats.fallback_count++;
    }
  }

  int SmcSocketChecker::updateStatistics() {
    m_stats.reset();
    int rc = 0;

    if (!m_netlinkHandler->isOpen()) {
      rc = m_netlinkHandler->open();
      if (rc != 0) {
        return rc;
      }
    }

    // Set dump sequence
    m_netlinkHandler->setDumpSeq(MAGIC_SEQ);

    // Request socket diagnostics with fallback information
    rc = m_netlinkHandler->sendDiagRequest(1 << (SMC_DIAG_FALLBACK - 1));
    if (rc != 0) {
      return rc;
    }

    rc = m_netlinkHandler->dump([this](struct nlmsghdr* nlh) {
      this->processSocket(nlh);
    });

    return rc;
  }

  const SmcSocketStats& SmcSocketChecker::getStatistics() const {
    return m_stats;
  }

  void SmcSocketChecker::resetStatistics() {
    m_stats.reset();
  }
#else
  SmcSocketChecker::SmcSocketChecker() noexcept {
  }

  SmcSocketChecker::~SmcSocketChecker() {
    // Unique_ptr will automatically clean up
  }

  SmcSocketChecker::SmcSocketChecker(SmcSocketChecker&& other) noexcept
    : m_stats(other.m_stats) {
  }

  SmcSocketChecker& SmcSocketChecker::operator=(SmcSocketChecker&& other) noexcept {
    if (this != &other) {
      m_stats = other.m_stats;
    }
    return *this;
  }

  int SmcSocketChecker::updateStatistics() {
    m_stats.reset();
    return 0;
  }

  const SmcSocketStats& SmcSocketChecker::getStatistics() const {
    return m_stats;
  }

  void SmcSocketChecker::resetStatistics() {
    m_stats.reset();
  }
#endif
