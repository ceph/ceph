/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#ifndef CEPH_RGW_D4N_LOG_H
 #define CEPH_RGW_D4N_LOG_H

#include <cerrno>
#include <concepts>
#include <exception>
#include <source_location>
#include <string_view>
#include <system_error>

#include "common/dout_fmt.h"

class DoutPrefixProvider;

namespace ceph::libfdb {

template <typename ErrorT>
struct transaction_failure;

} // namespace ceph::libfdb

namespace rgw::d4n::detail {

struct diagnostic_message
{
 fmt::string_view pattern;
 fmt::format_args arguments;
};

} // rgw::d4n::detail

// As Ceph uses libfmt rather than std::format(), we'll cater to that:
template <>
struct fmt::formatter<rgw::d4n::detail::diagnostic_message>
{
 constexpr auto parse(fmt::format_parse_context& context) { return context.begin(); }

 template <typename FormatContext>
 auto format(const rgw::d4n::detail::diagnostic_message& message, FormatContext& context) const
 {
   return fmt::vformat_to(context.out(), message.pattern, message.arguments);
 }

};

namespace rgw::d4n::detail {

template <int Level = 0, typename ...Args>
void log_message(const DoutPrefixProvider *dpp, const std::source_location whence,
                 const fmt::format_string<Args...> pattern, Args&& ...args)
{
  const auto arguments = fmt::make_format_args(args...);
  ldpp_dout_fmt(dpp, Level, "{} at {}:{}: {}",
                whence.function_name(), whence.file_name(), whence.line(),
                diagnostic_message {pattern, arguments});
}

template <typename ErrorT>
requires (std::same_as<ErrorT, int> or std::same_as<ErrorT, void>)
int return_d4n_error(const DoutPrefixProvider *dpp,
                     const ceph::libfdb::transaction_failure<ErrorT>& failure,
                     std::source_location whence = std::source_location::current(),
                     std::string_view resource = {});

int report_fdb_exception(const DoutPrefixProvider *dpp, std::exception_ptr failure,
                        std::source_location whence = std::source_location::current(),
                        std::string_view resource = {});

// Convert to the negative errno convention used by RGW:
inline int to_errno(const std::error_code error) noexcept
{
  if (!error) {
    return 0;
  }

  if (std::generic_category() == error.category() ||
      std::system_category() == error.category()) {
    return -error.value();
  }

  // Native FDB and libfdb values are not errno values:
  return -EIO;
}

} // namespace rgw::d4n::detail

#endif // CEPH_RGW_D4N_LOG_H
