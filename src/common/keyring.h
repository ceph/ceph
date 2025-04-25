// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <ostream>
#include <system_error>
#include <utility>

#include "fmt/ostream.h"
#include "include/expected.hpp"

namespace ceph {

/// RAII style wrapper around a secret stored in the Linux Key
/// Retention Service.
/// See: add_key(2), keyctl_read(3), keyctl_invalidate(3)
class LinuxKeyringSecret {
  size_t _len;
  int _serial;

  explicit LinuxKeyringSecret(int serial, size_t len) noexcept;
 public:
  LinuxKeyringSecret() = delete;
  LinuxKeyringSecret(const LinuxKeyringSecret&) = delete;
  LinuxKeyringSecret& operator=(const LinuxKeyringSecret&) = delete;
  LinuxKeyringSecret(LinuxKeyringSecret&& other) noexcept
      : _len(std::exchange(other._len, 0)),
        _serial(std::exchange(other._serial, -1)) {};
  LinuxKeyringSecret& operator=(LinuxKeyringSecret&& other) noexcept {
    if (this != &other) {
      if (this->initialized()) {
	this->reset();
      }
      _len = std::exchange(other._len, 0);
      _serial = std::exchange(other._serial, -1);
    }
    return *this;
  };

  ~LinuxKeyringSecret() noexcept;

  // Add a secret to the kernel process keyring. Beware: calling this
  // with with an existing key _updates_ the value and causes multiple
  // instances to refer to the same key.
  static tl::expected<LinuxKeyringSecret, std::error_code> add(
      const std::string& key, const std::string& secret) noexcept;
  static bool supported(std::error_code* ec = nullptr) noexcept;
  // Initialize the process keyring. Do this before starting any
  // threads that want to share possession of keys in the process
  // keyring
  static void initialize_process_keyring() noexcept;
  [[nodiscard]] std::error_code read(std::string& out) const;
  [[nodiscard]] std::error_code remove() const;
  [[nodiscard]] bool initialized() const;
 private:
  std::error_code reset();

  friend std::ostream& operator<<(
      std::ostream& os, const LinuxKeyringSecret& secret) {
    if (secret._serial == -1) {
      return os << "LinuxKeyringSecret{}";
    }
    return os << "LinuxKeyringSecret{" << secret._serial << "}";
  }
  friend class LinuxKeyringTest_LifecycleMoveAssignResetsDestination_Test;
};
}  // namespace ceph

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<ceph::LinuxKeyringSecret> : fmt::ostream_formatter {};
#endif
