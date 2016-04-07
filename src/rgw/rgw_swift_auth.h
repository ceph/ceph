// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SWIFT_AUTH_H
#define CEPH_RGW_SWIFT_AUTH_H

#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_auth.h"

#define RGW_SWIFT_TOKEN_EXPIRATION (15 * 60)

/* TempURL: applier. */
class RGWTempURLAuthApplier : public RGWLocalAuthApplier {
protected:
  RGWTempURLAuthApplier(CephContext * const cct,
                        const RGWUserInfo& user_info)
    : RGWLocalAuthApplier(cct, user_info, RGWLocalAuthApplier::NO_SUBUSER) {
  };

public:
  virtual void modify_request_state(req_state * s) const override; /* in/out */

  class Factory;
};

class RGWTempURLAuthApplier::Factory : public RGWLocalAuthApplier::Factory {
  friend class RGWTempURLAuthEngine;
protected:
  /* We need to unhide other overloads through importing them to our scope.
   * C++ quirks, sorry. */
  using RGWLocalAuthApplier::Factory::create_loader;
  virtual aplptr_t create_loader(CephContext * const cct,
                                 const RGWUserInfo& user_info) const {
    return aplptr_t(new RGWTempURLAuthApplier(cct, user_info));
  }

public:
  using RGWLocalAuthApplier::Factory::Factory;
  virtual ~Factory() {};
};

/* TempURL: engine */
class RGWTempURLAuthEngine : public RGWAuthEngine {
protected:
  /* const */ RGWRados * const store;
  const req_state * const s;
  const RGWTempURLAuthApplier::Factory * const apl_factory;

  /* Helper methods. */
  void get_owner_info(RGWUserInfo& owner_info) const;
  bool is_expired(const std::string& expires) const;
  std::string generate_signature(const string& key,
                                 const string& method,
                                 const string& path,
                                 const string& expires) const;
public:
  RGWTempURLAuthEngine(const req_state * const s,
                       /*const*/ RGWRados * const store,
                       const RGWTempURLAuthApplier::Factory * const apl_factory)
    : RGWAuthEngine(s->cct),
      store(store),
      s(s),
      apl_factory(apl_factory) {
  }

  /* Interface implementations. */
  std::string get_name() const noexcept override {
    return "RGWTempURLAuthEngine";
  }

  bool is_applicable() const noexcept override;
  RGWAuthApplier::aplptr_t authenticate() const override;
};


/* AUTH_rgwtk */
class RGWSignedTokenAuthEngine : public RGWTokenBasedAuthEngine {
protected:
  /* const */ RGWRados * const store;
  const RGWLocalAuthApplier::Factory * apl_factory;
public:
  RGWSignedTokenAuthEngine(CephContext * const cct,
                           /* const */RGWRados * const store,
                           const Extractor& extr,
                           const RGWLocalAuthApplier::Factory * const apl_factory)
    : RGWTokenBasedAuthEngine(cct, extr),
      store(store),
      apl_factory(apl_factory) {
  }

  std::string get_name() const noexcept override {
    return "RGWSignedTokenAuthEngine";
  }

  bool is_applicable() const noexcept override;
  RGWAuthApplier::aplptr_t authenticate() const override;
};


/* External token */
class RGWExternalTokenAuthEngine : public RGWTokenBasedAuthEngine {
protected:
  /* const */ RGWRados * const store;
  const RGWLocalAuthApplier::Factory * const apl_factory;
public:
  RGWExternalTokenAuthEngine(CephContext * const cct,
                             /* const */RGWRados * const store,
                             const Extractor& extr,
                             const RGWLocalAuthApplier::Factory * const apl_factory)
    : RGWTokenBasedAuthEngine(cct, extr),
      store(store),
      apl_factory(apl_factory) {
  }

  std::string get_name() const noexcept override {
    return "RGWExternalTokenAuthEngine";
  }

  bool is_applicable() const noexcept override;
  RGWAuthApplier::aplptr_t authenticate() const override;
};


/* Extractor. */
class RGWReqStateTokenExtractor : public RGWTokenBasedAuthEngine::Extractor {
protected:
  const req_state * const s;
public:
  RGWReqStateTokenExtractor(const req_state * const s)
    : s(s) {
  }
  std::string get_token() const {
    return s->info.env->get("HTTP_X_AUTH_TOKEN", "");
  }
};


class RGW_SWIFT_Auth_Get : public RGWOp {
public:
  RGW_SWIFT_Auth_Get() {}
  ~RGW_SWIFT_Auth_Get() {}

  int verify_permission() { return 0; }
  void execute();
  virtual const string name() { return "swift_auth_get"; }
};

class RGWHandler_SWIFT_Auth : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_Auth() {}
  ~RGWHandler_SWIFT_Auth() {}
  RGWOp *op_get();

  int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  int authorize();
  int postauth_init() { return 0; }
  int read_permissions(RGWOp *op) { return 0; }

  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_SWIFT_Auth : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_Auth() {}
  virtual ~RGWRESTMgr_SWIFT_Auth() {}

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri, string *out_uri) {
    return this;
  }
  virtual RGWHandler_REST* get_handler(struct req_state *s) {
    return new RGWHandler_SWIFT_Auth;
  }
};


#endif
