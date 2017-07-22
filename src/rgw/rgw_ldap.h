// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_LDAP_H
#define RGW_LDAP_H

#include "acconfig.h"

#if defined(HAVE_OPENLDAP)
#define LDAP_DEPRECATED 1
#include "ldap.h"
#endif

#include <stdint.h>
#include <tuple>
#include <vector>
#include <string>
#include <iostream>
#include <mutex>

namespace rgw {

#if defined(HAVE_OPENLDAP)

  class LDAPHelper
  {
    std::string uri;
    std::string binddn;
    std::string bindpw;
    std::string searchdn;
    std::string searchfilter;
    std::string dnattr;
    LDAP *ldap;
    bool msad = false; /* TODO: possible future specialization */
    std::mutex mtx;

  public:
    using lock_guard = std::lock_guard<std::mutex>;

    LDAPHelper(std::string _uri, std::string _binddn, std::string _bindpw,
	       std::string _searchdn, std::string _searchfilter, std::string _dnattr)
      : uri(std::move(_uri)), binddn(std::move(_binddn)),
	bindpw(std::move(_bindpw)), searchdn(_searchdn), searchfilter(_searchfilter), dnattr(_dnattr),
	ldap(nullptr) {
      // nothing
    }

    int init() {
      int ret;
      ret = ldap_initialize(&ldap, uri.c_str());
      if (ret == LDAP_SUCCESS) {
	unsigned long ldap_ver = LDAP_VERSION3;
	ret = ldap_set_option(ldap, LDAP_OPT_PROTOCOL_VERSION,
			      (void*) &ldap_ver);
      }
      if (ret == LDAP_SUCCESS) {
	ret = ldap_set_option(ldap, LDAP_OPT_REFERRALS, LDAP_OPT_OFF); 
      }
      return (ret == LDAP_SUCCESS) ? ret : -EINVAL;
    }

    int bind() {
      int ret;
      ret = ldap_simple_bind_s(ldap, binddn.c_str(), bindpw.c_str());
      return (ret == LDAP_SUCCESS) ? ret : -EINVAL;
    }

    int rebind() {
      if (ldap) {
	(void) ldap_unbind(ldap);
	(void) init();
	return bind();
      }
      return -EINVAL;
    }

    int simple_bind(const char *dn, const std::string& pwd) {
      LDAP* tldap;
      int ret = ldap_initialize(&tldap, uri.c_str());
      if (ret == LDAP_SUCCESS) {
	unsigned long ldap_ver = LDAP_VERSION3;
	ret = ldap_set_option(tldap, LDAP_OPT_PROTOCOL_VERSION,
			      (void*) &ldap_ver);
	if (ret == LDAP_SUCCESS) {
	  ret = ldap_simple_bind_s(tldap, dn, pwd.c_str());
	  if (ret == LDAP_SUCCESS) {
	    (void) ldap_unbind(tldap);
	  }
	}
      }
      return ret; // OpenLDAP client error space
    }

    int auth(const std::string uid, const std::string pwd);

    ~LDAPHelper() {
      if (ldap)
	(void) ldap_unbind(ldap);
    }

  }; /* LDAPHelper */

#else

  class LDAPHelper
  {
  public:
    LDAPHelper(std::string _uri, std::string _binddn, std::string _bindpw,
	       std::string _searchdn, std::string _searchfilter, std::string _dnattr)
      {}

    int init() {
      return -ENOTSUP;
    }

    int bind() {
      return -ENOTSUP;
    }

    int auth(const std::string uid, const std::string pwd) {
      return -EACCES;
    }

    ~LDAPHelper() {}

  }; /* LDAPHelper */


#endif /* HAVE_OPENLDAP */
  
} /* namespace rgw */

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/safe_io.h"
#include <boost/algorithm/string.hpp>

#include "include/assert.h"

std::string parse_rgw_ldap_bindpw(CephContext* ctx);

#endif /* RGW_LDAP_H */
