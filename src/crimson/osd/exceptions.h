// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <system_error>

#include "crimson/common/errorator.h"

namespace crimson::osd {
class error : private std::system_error {
public:
  error(const std::errc ec)
    : system_error(std::make_error_code(ec)) {
  }

  using system_error::code;
  using system_error::what;

  friend error make_error(int ret);

private:
  error(const int ret) noexcept
    : system_error(ret, std::system_category()) {
  }
};

inline error make_error(const int ret) {
  return error{ret};
}

struct object_not_found : public error {
  object_not_found() : error(std::errc::no_such_file_or_directory) {}
};

struct invalid_argument : public error {
  invalid_argument() : error(std::errc::invalid_argument) {}
};

// FIXME: error handling
struct permission_denied : public error {
  permission_denied() : error(std::errc::operation_not_permitted) {}
};

} // namespace crimson::osd
