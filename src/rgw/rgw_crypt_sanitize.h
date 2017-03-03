// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_RGW_CRYPT_SANITIZE_H_
#define RGW_RGW_CRYPT_SANITIZE_H_

#include "rgw_common.h"

namespace rgw {
namespace crypt_sanitize {


struct env {
  boost::string_ref name;
  boost::string_ref value;

  env(boost::string_ref name, boost::string_ref value)
  : name(name), value(value) {}
};

struct x_meta_map {
  boost::string_ref name;
  boost::string_ref value;
  x_meta_map(boost::string_ref name, boost::string_ref value)
  : name(name), value(value) {}
};

struct s3_policy {
  boost::string_ref name;
  boost::string_ref value;
  s3_policy(boost::string_ref name, boost::string_ref value)
  : name(name), value(value) {}
};

struct auth {
  const req_state* const s;
  boost::string_ref value;
  auth(const req_state* const s, boost::string_ref value)
  : s(s), value(value) {}
};

struct log_content {
  const char* buf;
  log_content(const char* buf)
  : buf(buf) {}
};

}
}

namespace std {
std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::env& e);
std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::x_meta_map& x);
std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::s3_policy& x);
std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::auth& x);
std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::log_content& x);
}
#endif /* RGW_RGW_CRYPT_SANITIZE_H_ */
