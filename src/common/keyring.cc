
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

#include "keyring.h"

namespace ceph {

#ifdef _WIN32

LinuxKeyringSecret::LinuxKeyringSecret(key_serial_t serial, size_t len) noexcept
    : _len(len), _serial(serial) {}

LinuxKeyringSecret::~LinuxKeyringSecret() noexcept {}

tl::expected<LinuxKeyringSecret, std::error_code> LinuxKeyringSecret::add(
    const std::string& key, const std::string& secret) noexcept {
  return tl::unexpected(std::error_code(-ENOSYS, std::system_category()));
}

bool LinuxKeyringSecret::supported() noexcept {
  return false;
}

[[nodiscard]] std::error_code LinuxKeyringSecret::read(std::string& out) const {
  return {-ENOSYS, std::system_category()};
}

[[nodiscard]] std::error_code LinuxKeyringSecret::remove() const {
  return {-ENOSYS, std::system_category()};
}

[[nodiscard]] std::error_code LinuxKeyringSecret::reset() {
  return {-ENOSYS, std::system_category()};
}

#else

#include <keyutils.h>

LinuxKeyringSecret::LinuxKeyringSecret(key_serial_t serial, size_t len) noexcept
    : _len(len), _serial(serial) {}

LinuxKeyringSecret::~LinuxKeyringSecret() noexcept {
  reset();
}

tl::expected<LinuxKeyringSecret, std::error_code> LinuxKeyringSecret::add(
    const std::string& key, const std::string& secret) noexcept {
  const auto serial = add_key(
      "user", key.c_str(), secret.c_str(), secret.size(),
      KEY_SPEC_SESSION_KEYRING);
  if (serial == -1) {
    return tl::unexpected(std::error_code(errno, std::system_category()));
  }
  return LinuxKeyringSecret(serial, secret.size());
}

bool LinuxKeyringSecret::supported() noexcept {
  auto secret = LinuxKeyringSecret::add("ceph_test_keyring_support", "ceph");
  if (!secret) {
    return false;
  }
  std::string out;
  if (auto err = secret.value().read(out); !err) {
    return false;
  }
  if (auto err = secret.value().reset(); !err) {
    return false;
  }
  return true;
}

[[nodiscard]] std::error_code LinuxKeyringSecret::read(std::string& out) const {
  out.clear();
  out.resize(_len);
  const auto ret = keyctl_read(_serial, out.data(), _len);
  if (ret == -1) {
    return {errno, std::system_category()};
  } else if (static_cast<size_t>(ret) != _len) {
    return {-EINVAL, std::generic_category()};
  }
  return {};
}

[[nodiscard]] std::error_code LinuxKeyringSecret::remove() const {
  const auto ret = keyctl_invalidate(_serial);
  if (ret != 0) {
    return {errno, std::system_category()};
  }
  return {};
}

[[nodiscard]] std::error_code LinuxKeyringSecret::reset() {
  if (_serial == -1) {
    return {-EINVAL, std::generic_category()};
  }
  if (const auto ret = remove(); !ret) {
    return ret;
  }
  _serial = -1;
  _len = 0;
  return {};
}

#endif

}  // namespace ceph
