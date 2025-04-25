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

class LinuxKeyringSecret {
  size_t _len;
  int _serial;

  explicit LinuxKeyringSecret(int serial, size_t len) noexcept;

 public:
  LinuxKeyringSecret() = default;
  LinuxKeyringSecret(const LinuxKeyringSecret&) = delete;
  LinuxKeyringSecret& operator=(const LinuxKeyringSecret&) = delete;
  LinuxKeyringSecret(LinuxKeyringSecret&& other) noexcept
      : _len(std::exchange(other._len, 0)),
        _serial(std::exchange(other._serial, -1)) {};
  LinuxKeyringSecret& operator=(LinuxKeyringSecret&& other) noexcept {
    if (this != &other) {
      _len = std::exchange(other._len, 0);
      _serial = std::exchange(other._serial, -1);
    }
    return *this;
  };

  ~LinuxKeyringSecret() noexcept;

  static tl::expected<LinuxKeyringSecret, std::error_code> add(
      const std::string& key, const std::string& secret) noexcept;
  static bool supported() noexcept;
  [[nodiscard]] std::error_code read(std::string& out) const;
  [[nodiscard]] std::error_code remove() const;

 private:
  std::error_code reset();

  friend std::ostream& operator<<(
      std::ostream& os, const LinuxKeyringSecret& secret) {
    if (secret._serial == -1) {
      return os << "LinuxKeyringSecret{}";
    }
    return os << "LinuxKeyringSecret{" << secret._serial << "}";
  }
};
}  // namespace ceph

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<ceph::LinuxKeyringSecret> : fmt::ostream_formatter {};
#endif
