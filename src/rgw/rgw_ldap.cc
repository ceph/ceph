// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_ldap.h"

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/safe_io.h"
#include <boost/algorithm/string.hpp>

#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

std::string parse_rgw_ldap_bindpw(CephContext* ctx)
{
  string ldap_bindpw;
  string ldap_secret = ctx->_conf->rgw_ldap_secret;

  if (ldap_secret.empty()) {
    ldout(ctx, 10)
      << __func__ << " LDAP auth no rgw_ldap_secret file found in conf"
      << dendl;
    } else {
      char bindpw[1024];
      memset(bindpw, 0, 1024);
      int pwlen = safe_read_file("" /* base */, ldap_secret.c_str(),
				 bindpw, 1023);
    if (pwlen) {
      ldap_bindpw = bindpw;
      boost::algorithm::trim(ldap_bindpw);
      if (ldap_bindpw.back() == '\n')
	ldap_bindpw.pop_back();
    }
  }

  return std::move(ldap_bindpw);
}

#if defined(HAVE_OPENLDAP)
namespace rgw {

  int LDAPHelper::auth(const std::string uid, const std::string pwd) {
    int ret;
    std::string filter;
    if (msad) {
      filter = "(&(objectClass=user)(sAMAccountName=";
      filter += uid;
      filter += "))";
    } else {
      /* openldap */
      if (searchfilter.empty()) {
        /* no search filter provided in config, we construct our own */
        filter = "(";
        filter += dnattr;
        filter += "=";
        filter += uid;
        filter += ")";
      } else {
        if (searchfilter.find("@USERNAME@") != std::string::npos) {
        /* we need to substitute the @USERNAME@ placeholder */
	  filter = searchfilter;
          filter.replace(searchfilter.find("@USERNAME@"), std::string("@USERNAME@").length(), uid);
        } else {
        /* no placeholder for username, so we need to append our own username filter to the custom searchfilter */
          filter = "(&(";
          filter += searchfilter;
          filter += ")(";
          filter += dnattr;
          filter += "=";
          filter += uid;
          filter += "))";
        }
      }
    }
    ldout(g_ceph_context, 12)
      << __func__ << " search filter: " << filter
      << dendl;
    char *attrs[] = { const_cast<char*>(dnattr.c_str()), nullptr };
    LDAPMessage *answer = nullptr, *entry = nullptr;
    bool once = true;

    lock_guard guard(mtx);

  retry_bind:
    ret = ldap_search_s(ldap, searchdn.c_str(), LDAP_SCOPE_SUBTREE,
			filter.c_str(), attrs, 0, &answer);
    if (ret == LDAP_SUCCESS) {
      entry = ldap_first_entry(ldap, answer);
      if (entry) {
	char *dn = ldap_get_dn(ldap, entry);
	ret = simple_bind(dn, pwd);
	if (ret != LDAP_SUCCESS) {
	  ldout(g_ceph_context, 10)
	    << __func__ << " simple_bind failed uid=" << uid
	    << dendl;
	}
	ldap_memfree(dn);
      } else {
	ldout(g_ceph_context, 12)
	  << __func__ << " ldap_search_s no user matching uid=" << uid
	  << dendl;
	ret = LDAP_NO_SUCH_ATTRIBUTE; // fixup result
      }
      ldap_msgfree(answer);
    } else {
      ldout(g_ceph_context, 5)
	<< __func__ << " ldap_search_s error uid=" << uid
	<< " ldap err=" << ret
	<< dendl;
      /* search should never fail--try to rebind */
      if (once) {
	rebind();
	once = false;
	goto retry_bind;
      }
    }
    return (ret == LDAP_SUCCESS) ? ret : -EACCES;
  } /* LDAPHelper::auth */
}

#endif /* defined(HAVE_OPENLDAP) */
