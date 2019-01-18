// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

namespace ceph::auth {

enum class error {
  success = 0,
  key_not_found,
  invalid_key,
  unknown_service, // no ticket handler for required service
};

const std::error_category& auth_category();

inline std::error_code make_error_code(error e)
{
  return {static_cast<int>(e), auth_category()};
}

inline std::error_condition make_error_condition(error e)
{
  return {static_cast<int>(e), auth_category()};
}

class auth_error : public std::runtime_error {};

} // namespace ceph::auth

namespace std {

/// enables implicit conversion to std::error_condition
template <>
struct is_error_condition_enum<ceph::auth::error> : public true_type {};

} // namespace std
