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
#include <condition_variable>
#include "xxhash.h"
#include "common/ceph_time.h"
#include <boost/intrusive/avl_set.hpp>
#include "common/cohort_lru.h"
#include "blake2/sse/blake2.h"

namespace rgw {

#if defined(HAVE_OPENLDAP)

  namespace bi = boost::intrusive;

  class LDAPHelper;

  namespace ldap {

    struct CKey
    {
      uint64_t hk; // 1st 8 bytes of the hmac, only used as part selector
      std::string name; // tracing and convenience only
      std::string hmac;

      static constexpr uint64_t seed = 1701;

      void hash(const std::string& pwd) {
	blake2bp_state s;
	(void) blake2bp_init(&s, BLAKE2B_OUTBYTES /* 64 */);
	blake2bp_update(&s, name.c_str(), name.length());
	blake2bp_update(&s, pwd.c_str(), pwd.length());
	hmac.resize(BLAKE2B_OUTBYTES);
	blake2bp_final(&s, hmac.data(), BLAKE2B_OUTBYTES);
	hk = *(reinterpret_cast<const uint64_t*>(hmac.c_str()));
      }

      CKey(const std::string& _name, const std::string& _pwd)
	: name(_name) {
	hash(_pwd);
      }

      CKey(std::string&& _name, std::string&& _pwd)
	: name(_name) {
	hash(_pwd);
      }
    };

    inline bool operator< (const CKey& lhs, const CKey& rhs)
    {
      return ((lhs.hk < rhs.hk) ||
	      ((lhs.hk == rhs.hk) &&
	       (lhs.hmac < rhs.hmac)));
    }

    inline bool operator> (const CKey& lhs, const CKey& rhs)
    {
      return rhs < lhs;
    }

    inline bool operator<=(const CKey& lhs, const CKey& rhs)
    {
      return !(lhs > rhs);
    }

    inline bool operator>=(const CKey& lhs, const CKey& rhs)
    {
      return !(lhs < rhs);
    }

    inline bool operator==(const CKey& lhs, const CKey& rhs)
    {
      return ((lhs.hk == rhs.hk) &&
	      (lhs.hmac == rhs.hmac));
    }

    enum class AuthResult
    {
      AUTH_SUCCESS = 0,
      AUTH_FAIL,
      AUTH_EXPIRED,
      AUTH_INIT
    };

    struct Cred : public cohort::lru::Object
    {
      CKey key;
      AuthResult result;
      LDAPHelper* ldh;

      std::mutex mtx;
      std::condition_variable cv;
      uint32_t waiters;

      ceph::coarse_mono_time time_added;

      using lock_guard = std::lock_guard<std::mutex>;
      using unique_lock = std::unique_lock<std::mutex>;
      using LRU = cohort::lru::LRU<std::mutex>;

      Cred(const CKey& k, LDAPHelper* _ldh)
	: key(k), result(AuthResult::AUTH_INIT), ldh(_ldh), waiters(0),
	  time_added(ceph::coarse_mono_clock::now())
	{}

      ~Cred() {}

      void reset() {
	result = AuthResult::AUTH_INIT;
	time_added = ceph::coarse_mono_clock::now();
      }

      AuthResult auth_result();

      void cond_notify() {
	unique_lock uniq(mtx);
	if (unlikely(waiters > 0)) {
	  cv.notify_all();
	}
      }

      bool reclaim() override;

      struct LT
      {
	// for internal ordering
	bool operator()(const Cred& lhs,
			const Cred& rhs) const
	  { return (lhs.key < rhs.key); }

	// for external search by CKey
	bool operator()(const CKey& k, const Cred& rhs) const
	  { return k < rhs.key; }

	bool operator()(const Cred& lhs, const CKey& k) const
	  { return lhs.key < k; }
      };

      struct EQ
      {
	bool operator()(const Cred& lhs,
			const Cred& rhs) const
	  { return (lhs.key == rhs.key); }

	bool operator()(const CKey& k, const Cred& rhs) const
	  { return k == rhs.key; }

	bool operator()(const Cred& lhs, const CKey& k) const
	  { return lhs.key == k; }
      };

      typedef bi::link_mode<bi::safe_link> link_mode; /* XXX safe_link */
      typedef bi::avl_set_member_hook<link_mode> tree_hook_type;

      tree_hook_type cache_hook;

      typedef bi::member_hook<
	Cred, tree_hook_type, &Cred::cache_hook> CacheHook;

      typedef bi::avltree<Cred, bi::compare<LT>, CacheHook> CacheAVL;

      typedef cohort::lru::TreeX<Cred, CacheAVL, LT, EQ, CKey,
				 std::mutex> Cache;

      class Factory : public cohort::lru::ObjectFactory
      {
      public:
	const CKey& k;
	LDAPHelper* ldh;
	uint32_t flags;

	Factory() = delete;

	Factory(const CKey& _k, LDAPHelper* _ldh)
	  : k(_k), ldh(_ldh)
	  {}

	void recycle (cohort::lru::Object* o) override {
	  /* re-use an existing object */
	  o->~Object(); // call lru::Object virtual dtor
	  // placement new!
	  new (o) Cred(k, ldh);
	}

	cohort::lru::Object* alloc() override {
	  return new Cred(k, ldh);
	}
      }; /* Factory */
      
    }; /* ldap::Cred */

  } /* namespace ldap */
  
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

    bool enable_cache;
    ceph::timespan ttl_s;
    ldap::Cred::LRU lru;
    ldap::Cred::Cache cache;

    friend class ldap::Cred;

  public:
    using lock_guard = std::lock_guard<std::mutex>;

    LDAPHelper(std::string _uri, std::string _binddn, std::string _bindpw,
	       std::string _searchdn, std::string _searchfilter,
	       std::string _dnattr,
	       bool enable_cache, uint32_t ttl_s,
	       uint64_t lru_nlanes, uint64_t lru_hiwat,
      	       uint64_t cache_npart, uint64_t cache_sz)
      : uri(std::move(_uri)), binddn(std::move(_binddn)),
	bindpw(std::move(_bindpw)), searchdn(_searchdn),
	searchfilter(_searchfilter), dnattr(_dnattr),
	ldap(nullptr),
	enable_cache(enable_cache), ttl_s(ttl_s), lru(lru_nlanes, lru_hiwat),
	cache(cache_npart, cache_sz)
      {
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

  private:
    int _auth(const std::string& uid, const std::string& pwd);

  }; /* LDAPHelper */

#else

  class LDAPHelper
  {
  public:
    LDAPHelper(std::string _uri, std::string _binddn, std::string _bindpw,
	       std::string _searchdn, std::string _searchfilter,
	       std::string _dnattr)
      {}

    int init() {
      return -ENOTSUP;
    }

    int bind() {
      return -ENOTSUP;
    }

    int _auth(const std::string& uid, const std::string& pwd) {
      return -EACCES;
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
