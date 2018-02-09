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

  return ldap_bindpw;
}

#if defined(HAVE_OPENLDAP)
namespace rgw {

  ldap::AuthResult ldap::Cred::auth_result()
  {
    unique_lock uniq(mtx);
    if (unlikely(result == AuthResult::AUTH_INIT)) {
      ++waiters;
      while (unlikely(result == AuthResult::AUTH_INIT)) {
	cv.wait(uniq);
      }
      --waiters;
    }
    if ((ceph::coarse_mono_clock::now() - time_added) > ldh->ttl_s) {
      result = AuthResult::AUTH_EXPIRED;
    }
    return result;
  } /* ldap::Cred::auth_result() */

  bool ldap::Cred::reclaim()
  {
    /* in the non-delete case, handle may still be in handle table */
    if (cache_hook.is_linked()) {
      /* in this case, we are being called from a context which holds
       * the partition lock */
      ldh->cache.remove(key.hk, this, Cache::FLAG_NONE);
    }
    return true; /* reclaimable? */
  }

  int LDAPHelper::_auth(const std::string& uid, const std::string& pwd) {
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
	    << "ldap err=" << ret
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
  } /* LDAPHelper::_auth */

  int LDAPHelper::auth(const std::string uid, const std::string pwd) {

    using namespace ldap;

    CKey k(uid, pwd);
    Cred::Cache::Latch latch;
    AuthResult auth_r{AuthResult::AUTH_FAIL};

    /* check cache */
  retry:
    Cred* cred =
      cache.find_latch(k.hk, k, latch, Cred::Cache::FLAG_LOCK);
    if (cred) {
      /* need initial ref from LRU (fast path) */
      if (! lru.ref(cred, cohort::lru::FLAG_INITIAL)) {
	latch.lock->unlock();
	goto retry; /* !LATCHED */
      }
      auth_r = cred->auth_result();
      switch(auth_r) {
      case AuthResult::AUTH_SUCCESS:
      {
	latch.lock->unlock();
	(void) lru.unref(cred, cohort::lru::FLAG_NONE);
	return LDAP_SUCCESS;
      }
      break;
      case AuthResult::AUTH_FAIL:
      {
	latch.lock->unlock();
	(void) lru.unref(cred, cohort::lru::FLAG_NONE);
	return -EACCES;
      }
      break;
      case AuthResult::AUTH_EXPIRED: /* refresh */
	cred->reset();
	break;
      default:
	break;
      };
    } else {
      Cred::Factory prototype(k, this);
      uint32_t iflags{cohort::lru::FLAG_INITIAL};
      cred = static_cast<Cred*>(
	lru.insert(&prototype,
		   cohort::lru::Edge::MRU,
		   iflags));
      if (likely(!!cred)) {
	/* lock fh (LATCHED) */
	if (likely(! (iflags & cohort::lru::FLAG_RECYCLE))) {
	  /* inserts at cached insert iterator, releasing latch */
	  cache.insert_latched(
	    cred, latch, Cred::Cache::FLAG_UNLOCK);
	} else {
	  /* recycle step invalidates Latch */
	  cache.insert(k.hk, cred, Cred::Cache::FLAG_NONE);
	  latch.lock->unlock(); /* !LATCHED */
	}
      } else {
	latch.lock->unlock();
	goto retry; /* !LATCHED */
      }
      lru.ref(cred, cohort::lru::FLAG_INITIAL); /* ref and adjust */
    }

    int ldap_r = _auth(uid, pwd);

    cred->result = (ldap_r == LDAP_SUCCESS)
      ? AuthResult::AUTH_SUCCESS
      : AuthResult::AUTH_FAIL;

    (void) lru.unref(cred, cohort::lru::FLAG_NONE);

    return ldap_r;
  } /* LDAPHelper::auth */

} /* rgw */

#endif /* defined(HAVE_OPENLDAP) */
