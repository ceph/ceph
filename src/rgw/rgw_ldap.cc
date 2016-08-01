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
