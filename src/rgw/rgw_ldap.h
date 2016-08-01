// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_LDAP_H
#define RGW_LDAP_H

#if defined(HAVE_OPENLDAP)
#define LDAP_DEPRECATED 1
#include "ldap.h"
#endif

#include <stdint.h>
#include <tuple>
#include <vector>
#include <string>
#include <iostream>

namespace rgw {

#if defined(HAVE_OPENLDAP)

  class LDAPHelper
  {
    std::string uri;
    std::string binddn;
    std::string bindpw;
    std::string searchdn;
    std::string dnattr;
    LDAP *ldap;
    bool msad = false; /* TODO: possible future specialization */

  public:
    LDAPHelper(std::string _uri, std::string _binddn, std::string _bindpw,
	       std::string _searchdn, std::string _dnattr)
      : uri(std::move(_uri)), binddn(std::move(_binddn)),
	bindpw(std::move(_bindpw)), searchdn(_searchdn), dnattr(_dnattr),
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

    int simple_bind(const char *dn, const std::string& pwd) {
      LDAP* tldap;
      int ret = ldap_initialize(&tldap, uri.c_str());
      ret = ldap_simple_bind_s(tldap, dn, pwd.c_str());
      if (ret == LDAP_SUCCESS) {
	ldap_unbind(tldap);
      }
      return ret; // OpenLDAP client error space
    }

    int auth(const std::string uid, const std::string pwd) {
      int ret;
      std::string filter;
      if (msad) {
	filter = "(&(objectClass=user)(sAMAccountName=";
	filter += uid;
	filter += "))";
      } else {
	/* openldap */
	filter = "(";
	filter += dnattr;
	filter += "=";
	filter += uid;
	filter += ")";
      }
      char *attrs[] = { const_cast<char*>(dnattr.c_str()), nullptr };
      LDAPMessage *answer = nullptr, *entry = nullptr;
      ret = ldap_search_s(ldap, searchdn.c_str(), LDAP_SCOPE_SUBTREE,
			  filter.c_str(), attrs, 0, &answer);
      if (ret == LDAP_SUCCESS) {
	entry = ldap_first_entry(ldap, answer);
	if (entry) {
	  char *dn = ldap_get_dn(ldap, entry);
	  ret = simple_bind(dn, pwd);
	  ldap_memfree(dn);
	} else {
	  ret = LDAP_NO_SUCH_ATTRIBUTE; // fixup result
	}
	ldap_msgfree(answer);
      }
      return (ret == LDAP_SUCCESS) ? ret : -EACCES;
    }

    ~LDAPHelper() {
      if (ldap)
	ldap_unbind(ldap);
    }

  }; /* LDAPHelper */

#else

  class LDAPHelper
  {
  public:
    LDAPHelper(std::string _uri, std::string _binddn, std::string _bindpw,
	       std::string _searchdn, std::string _dnattr)
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
