// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc. <contact@redhat.com>
 *
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */

#ifndef COMMON_CEPH_ERROR_CODE
#define COMMON_CEPH_ERROR_CODE

#include <cerrno>
#ifdef __has_include
#  if __has_include(<format>)
#    include <format>
#  endif
#endif
#include <functional>
#include <new>
#include <optional>
#include <regex>
#include <stdexcept>
#include <system_error>
#include <variant>

#include <boost/system/error_category.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/error_condition.hpp>
#include <boost/system/generic_category.hpp>
#include <boost/system/system_error.hpp>

#include <fmt/format.h>

#include <netdb.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"

namespace ceph {
// This is for error categories we define, so we can specify the
// equivalent integral value at the point of definition.
class converting_category : public boost::system::error_category {
public:
  virtual int from_code(int code) const noexcept = 0;
};

const boost::system::error_category& ceph_category() noexcept;

enum class errc {
  not_in_map = 1, // The requested item was not found in the map
  does_not_exist, // Item does not exist
  failure, // An internal fault or inconsistency
  exists, // Already exists
  limit_exceeded, // Attempting to use too much of something
  auth, // May not be an auth failure. It could be that the
	// preconditions to attempt auth failed.
  conflict, // Conflict or precondition failure
};
}

namespace boost::system {
template<>
struct is_error_condition_enum<::ceph::errc> {
  static const bool value = true;
};
template<>
struct is_error_code_enum<::ceph::errc> {
  static const bool value = false;
};
}

namespace ceph {
//  explicit conversion:
inline boost::system::error_code make_error_code(errc e) noexcept {
  return { static_cast<int>(e), ceph_category() };
}

// implicit conversion:
inline boost::system::error_condition make_error_condition(errc e) noexcept {
  return { static_cast<int>(e), ceph_category() };
}

[[nodiscard]] boost::system::error_code to_error_code(int ret) noexcept;
[[nodiscard]] int from_error_code(boost::system::error_code e) noexcept;
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

inline int from_exception(std::exception_ptr eptr) {
  if (!eptr) [[likely]] {
    return 0;
  }
  try {
    std::rethrow_exception(eptr);
  } catch (const boost::system::system_error& e) {
    return from_error_code(e.code());
  } catch (const std::system_error& e) {
    return from_error_code(e.code());
  } catch (const std::invalid_argument&) {
    return -EINVAL;
  } catch (const std::domain_error&) {
    return -EDOM;
  } catch (const std::length_error&) {
    return -ERANGE;
  } catch (const std::out_of_range&) {
    return -ERANGE;
  } catch (const std::range_error&) {
    return -ERANGE;
  } catch (const std::overflow_error&) {
    return -EOVERFLOW;
  } catch (const std::underflow_error&) {
    return -EOVERFLOW;
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  } catch (const std::regex_error& e) {
    using namespace std::regex_constants;
    switch (e.code()) {
    case error_space:
    case error_stack:
      return -ENOMEM;
    case error_complexity:
      return -ENOTSUP;
    default:
      return -EINVAL;
    }
#ifdef __has_include
#  if __has_include(<format>)
  } catch (const std::format_error& e) {
    return -EINVAL;
#  endif
#endif
  } catch (const fmt::format_error& e) {
    return -EINVAL;
  } catch (const std::bad_typeid&) {
    return -EFAULT;
  } catch (const std::bad_cast&) {
    return -EFAULT;
  } catch (const std::bad_optional_access&) {
    return -EFAULT;
  } catch (const std::bad_weak_ptr&) {
    return -EFAULT;
  } catch (const std::bad_function_call&) {
    return -EFAULT;
  } catch (const std::bad_exception&) {
    return -EFAULT;
  } catch (const std::bad_variant_access&) {
    return -EFAULT;
  } catch (...) {
    return -EIO;
  }
}
}

// Moved here from buffer.h so librados doesn't gain a dependency on
// Boost.System

namespace ceph::buffer {
inline namespace v15_2_0 {
const boost::system::error_category& buffer_category() noexcept;
enum class errc { bad_alloc = 1,
		  end_of_buffer,
		  malformed_input };
}
}

namespace boost::system {
template<>
struct is_error_code_enum<::ceph::buffer::errc> {
  static const bool value = true;
};

template<>
struct is_error_condition_enum<::ceph::buffer::errc> {
  static const bool value = false;
};
}

namespace ceph::buffer {
inline namespace v15_2_0 {

// implicit conversion:
inline boost::system::error_code make_error_code(errc e) noexcept {
  return { static_cast<int>(e), buffer_category() };
}

// explicit conversion:
inline boost::system::error_condition
make_error_condition(errc e) noexcept {
  return { static_cast<int>(e), buffer_category() };
}

struct error : boost::system::system_error {
  using system_error::system_error;
};

struct bad_alloc : public error {
  bad_alloc() : error(errc::bad_alloc) {}
  bad_alloc(const char* what_arg) : error(errc::bad_alloc, what_arg) {}
  bad_alloc(const std::string& what_arg) : error(errc::bad_alloc, what_arg) {}
};
struct end_of_buffer : public error {
  end_of_buffer() : error(errc::end_of_buffer) {}
  end_of_buffer(const char* what_arg) : error(errc::end_of_buffer, what_arg) {}
  end_of_buffer(const std::string& what_arg)
    : error(errc::end_of_buffer, what_arg) {}
};

struct malformed_input : public error {
  malformed_input() : error(errc::malformed_input) {}
  malformed_input(const char* what_arg)
    : error(errc::malformed_input, what_arg) {}
  malformed_input(const std::string& what_arg)
    : error(errc::malformed_input, what_arg) {}
};
struct error_code : public error {
  error_code(int r) : error(-r, boost::system::generic_category()) {}
  error_code(int r, const char* what_arg)
    : error(-r, boost::system::generic_category(), what_arg) {}
  error_code(int r, const std::string& what_arg)
    : error(-r, boost::system::generic_category(), what_arg) {}
};
}
}

#endif // COMMON_CEPH_ERROR_CODE
