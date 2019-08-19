// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <system_error>

namespace ceph::osd {
class error : private std::system_error {
public:
  error(const std::errc ec)
    : system_error(std::make_error_code(ec)) {
  }

  using system_error::code;
  using system_error::what;

private:
  error(const int ret) noexcept
    : system_error(ret, std::system_category()) {
  }
};


struct object_not_found : public error {
  object_not_found() : error(std::errc::no_such_file_or_directory) {}
};

struct object_corrupted : public error {
  object_corrupted() : error(std::errc::illegal_byte_sequence) {}
};

struct invalid_argument : public error {
  invalid_argument() : error(std::errc::invalid_argument) {}
};

} // namespace ceph::osd
