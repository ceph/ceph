// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * SMC Socket Checker Base Class
 *
 * Abstract base class for SMC socket checking functionality.
 * Provides platform-independent interface with platform-specific implementations.
 *
 * Author(s): Aliaksei Makarau <aliaksei.makarau@ibm.com>
 * 
 * Copyright IBM Corp. 2026
 */
#ifndef CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_BASE_H
#define CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_BASE_H

struct SmcSocketStats {
  int total_sockets;
  int fallback_count;

  SmcSocketStats() : total_sockets(0), fallback_count(0) {}

  void reset() {
    total_sockets = 0;
    fallback_count = 0;
  }
};

/**
 * @brief Abstract base class for SMC Socket Checker
 * 
 * This class provides a platform-independent interface for checking
 * SMC socket connections and gathering statistics.
 */
class SmcSocketCheckerBase {
public:
  virtual ~SmcSocketCheckerBase() = default;

  SmcSocketCheckerBase(const SmcSocketCheckerBase&) = delete;
  SmcSocketCheckerBase& operator=(const SmcSocketCheckerBase&) = delete;

  SmcSocketCheckerBase(SmcSocketCheckerBase&&) = default;
  SmcSocketCheckerBase& operator=(SmcSocketCheckerBase&&) = default;

  /**
   * @brief Update socket statistics
   * 
   * @return int 0 on success, non-zero on error
   */
  virtual int updateStatistics() = 0;

  /**
   * @brief Get current statistics
   * 
   * @return const SmcSocketStats& Reference to statistics
   */
  virtual const SmcSocketStats& getStatistics() const = 0;

  /**
   * @brief Reset statistics to zero
   */
  virtual void resetStatistics() = 0;

protected:
  SmcSocketCheckerBase() = default;
};

#endif /* CEPH_MSG_ASYNC_SMC_SMCSOCKET_CHECKER_BASE_H */
