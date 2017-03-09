/*
 * rgw_crypt_sanitize.cc
 *
 *  Created on: Mar 3, 2017
 *      Author: adam
 */

#include "rgw_common.h"
#include "rgw_crypt_sanitize.h"
#include "boost/algorithm/string/predicate.hpp"

namespace rgw {
namespace crypt_sanitize {
const char* HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY = "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY";
const char* x_amz_server_side_encryption_customer_key = "x-amz-server-side-encryption-customer-key";
const char* dollar_x_amz_server_side_encryption_customer_key = "$x-amz-server-side-encryption-customer-key";
const char* suppression_message = "=suppressed due to key presence=";

std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::env& e) {
  if (g_ceph_context->_conf->rgw_crypt_suppress_logs) {
    if (boost::algorithm::iequals(
        e.name,
        rgw::crypt_sanitize::HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY))
    {
      out << rgw::crypt_sanitize::suppression_message;
      return out;
    }
    if (boost::algorithm::iequals(e.name, "QUERY_STRING") &&
        boost::algorithm::ifind_first(
            e.value,
            rgw::crypt_sanitize::x_amz_server_side_encryption_customer_key))
    {
      out << rgw::crypt_sanitize::suppression_message;
      return out;
    }
  }
  out << e.value;
  return out;
}

std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::x_meta_map& x) {
  if (g_ceph_context->_conf->rgw_crypt_suppress_logs &&
      boost::algorithm::iequals(x.name, rgw::crypt_sanitize::x_amz_server_side_encryption_customer_key))
  {
    out << rgw::crypt_sanitize::suppression_message;
    return out;
  }
  out << x.value;
  return out;
}

std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::s3_policy& x) {
  if (g_ceph_context->_conf->rgw_crypt_suppress_logs &&
      boost::algorithm::iequals(x.name, rgw::crypt_sanitize::dollar_x_amz_server_side_encryption_customer_key))
  {
    out << rgw::crypt_sanitize::suppression_message;
    return out;
  }
  out << x.value;
  return out;
}

std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::auth& x) {
  if (g_ceph_context->_conf->rgw_crypt_suppress_logs &&
      x.s->info.env->get(rgw::crypt_sanitize::HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, nullptr) != nullptr)
  {
    out << rgw::crypt_sanitize::suppression_message;
    return out;
  }
  out << x.value;
  return out;
}

std::ostream& operator<<(std::ostream& out, const rgw::crypt_sanitize::log_content& x) {
  if (g_ceph_context->_conf->rgw_crypt_suppress_logs &&
      boost::algorithm::ifind_first(x.buf, rgw::crypt_sanitize::x_amz_server_side_encryption_customer_key)) {
    out << rgw::crypt_sanitize::suppression_message;
    return out;
  }
  out << x.buf;
  return out;
}

}
}
