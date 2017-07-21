// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_RGW_CRYPT_SANITIZE_H_
#define RGW_RGW_CRYPT_SANITIZE_H_

#include <boost/utility/string_view.hpp>

#include "rgw_common.h"

namespace rgw {
namespace crypt_sanitize {

/*
 * Temporary container for suppressing printing if variable contains secret key.
 */
struct env {
  boost::string_ref name;
  boost::string_ref value;

  env(boost::string_ref name, boost::string_ref value)
  : name(name), value(value) {}
};

/*
 * Temporary container for suppressing printing if aws meta attributes contains secret key.
 */
struct x_meta_map {
  boost::string_ref name;
  boost::string_ref value;
  x_meta_map(boost::string_ref name, boost::string_ref value)
  : name(name), value(value) {}
};

/*
 * Temporary container for suppressing printing if s3_policy calculation variable contains secret key.
 */
struct s3_policy {
  boost::string_ref name;
  boost::string_ref value;
  s3_policy(boost::string_ref name, boost::string_ref value)
  : name(name), value(value) {}
};

/*
 * Temporary container for suppressing printing if auth string contains secret key.
 */
struct auth {
  const req_state* const s;
  boost::string_ref value;
  auth(const req_state* const s, boost::string_ref value)
  : s(s), value(value) {}
};

/*
 * Temporary container for suppressing printing if log made from civetweb may contain secret key.
 */
struct log_content {
  const boost::string_view buf;
  log_content(const boost::string_view buf)
  : buf(buf) {}
};

std::ostream& operator<<(std::ostream& out, const env& e);
std::ostream& operator<<(std::ostream& out, const x_meta_map& x);
std::ostream& operator<<(std::ostream& out, const s3_policy& x);
std::ostream& operator<<(std::ostream& out, const auth& x);
std::ostream& operator<<(std::ostream& out, const log_content& x);
}
}
#endif /* RGW_RGW_CRYPT_SANITIZE_H_ */
