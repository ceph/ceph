// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * SMC Socket Checker No-Op Implementation
 *
 * No-op implementation for platforms that don't support SMC (non-Linux).
 * Provides empty implementations that do nothing.
 *
 * Author(s): Aliaksei Makarau <aliaksei.makarau@ibm.com>
 * 
 * Copyright IBM Corp. 2026
 */
#ifndef CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_NOOP_H
#define CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_NOOP_H

#include "smc_socket_checker_base.h"

/**
 * @brief No-op SMC Socket Checker for non-Linux platforms
 * 
 * This implementation does nothing and always returns empty statistics.
 * Used on platforms that don't support SMC (Windows, FreeBSD, etc.).
 */
class SmcSocketCheckerNoop : public SmcSocketCheckerBase {
public:
  SmcSocketCheckerNoop() = default;
  ~SmcSocketCheckerNoop() override = default;

  SmcSocketCheckerNoop(SmcSocketCheckerNoop&&) noexcept = default;
  SmcSocketCheckerNoop& operator=(SmcSocketCheckerNoop&&) noexcept = default;

  int updateStatistics() override {
    // No-op: always succeeds
    return 0;
  }

  const SmcSocketStats& getStatistics() const override {
    return m_stats;
  }

  void resetStatistics() override {
    m_stats.reset();
  }

private:
  SmcSocketStats m_stats;
};

#endif /* CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_NOOP_H */
