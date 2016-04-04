// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_AUTH_H
#define CEPH_RGW_AUTH_H

#include "rgw_common.h"
#include "rgw_keystone.h"


/* Interface for classes applying changes to request state/RADOS store imposed
 * by a particular RGWAuthEngine.
 *
 * In contrast to RGWAuthEngine, implementations of this interface are allowed
 * to handle req_state or RGWRados in the read-write manner. */
class RGWAuthApplier {
  friend class RGWDecoratoringAuthApplier;
protected:
  CephContext * const cct;
public:
  typedef std::unique_ptr<RGWAuthApplier> aplptr_t;

  RGWAuthApplier(CephContext * const cct) : cct(cct) {}
  virtual ~RGWAuthApplier() {};

  /* Fill provided RGWUserInfo with information about the account that
   * RGWOp may operate on. Errors are handled solely through exceptions. */
  virtual void load_acct_info(RGWUserInfo& user_info) const = 0; /* out */

  /* Load information about identity that will be used by RGWOp to authorize
   * any operation that comes from an authenticated user. */
  virtual void load_user_info(rgw_user& auth_user,               /* out */
                              uint32_t& perm_mask,               /* out */
                              bool& admin_request) const = 0;    /* out */

  /* Apply any changes to request state. This method will be most useful for
   * TempURL of Swift API or AWSv4. */
  virtual void modify_request_state(req_state * s) const {}      /* in/out */
};


/* Abstract decorator over any implementation of RGWAuthApplier. */
class RGWDecoratoringAuthApplier : public RGWAuthApplier {
protected:
  aplptr_t decoratee;

public:
  RGWDecoratoringAuthApplier(aplptr_t&& decoratee)
    : RGWAuthApplier(decoratee->cct),
      decoratee(std::move(decoratee)) {
  }

  virtual void load_acct_info(RGWUserInfo& user_info) const override {  /* out */
    return decoratee->load_acct_info(user_info);
  }
  virtual void load_user_info(rgw_user& auth_user,                      /* out */
                              uint32_t& perm_mask,                      /* out */
                              bool& admin_request) const override {     /* out */
    return decoratee->load_user_info(auth_user, perm_mask, admin_request);
  }

  virtual void modify_request_state(req_state * s) const override {     /* in/out */
    return decoratee->modify_request_state(s);
  }
};


class RGWThirdPartyAccountAuthApplier : public RGWDecoratoringAuthApplier {
  /* const */RGWRados * const store;
  const rgw_user acct_user_override;
public:
  /* FIXME: comment this. */
  static const rgw_user UNKNOWN_ACCT;

  RGWThirdPartyAccountAuthApplier(aplptr_t&& decoratee,
                                  RGWRados * const store,
                                  const rgw_user acct_user_override)
    : RGWDecoratoringAuthApplier(std::move(decoratee)),
      store(store),
      acct_user_override(acct_user_override) {
  }

  virtual void load_acct_info(RGWUserInfo& user_info) const override;   /* out */
};


/* RGWRemoteAuthApplier - applier typical for auth engines which don't need
 * to ask the RADOS store about user credentials but instead obtain them from
 * an external source-of-truth like Keystone or LDAP.
 *
 * In such cases the applier must be able to create internal data structures
 * for users who log-in for first time. */
class RGWRemoteAuthApplier : public RGWAuthApplier {
public:
  class AuthInfo {
    friend class RGWRemoteAuthApplier;
  protected:
    const rgw_user acct_user;
    const rgw_user auth_user;
    const std::string display_name;
    const uint32_t perm_mask;
    const bool is_admin;

  public:
    AuthInfo(const rgw_user acct_user,
             const rgw_user auth_user,
             const std::string display_name,
             const uint32_t perm_mask,
             const bool is_admin)
    : acct_user(acct_user),
      auth_user(auth_user),
      display_name(display_name),
      perm_mask(perm_mask),
      is_admin(is_admin) {
    }

    /* Constructor for engines that aren't aware about user account. They know
     * only user's identity and its associated rights. Account will be deduced
     * for them. */
    AuthInfo(const rgw_user auth_user,
             const std::string display_name,
             const uint32_t perm_mask,
             const bool is_admin)
      : AuthInfo(rgw_user(), auth_user, display_name, perm_mask, is_admin) {
    }
  };

protected:
  /* Read-write is intensional here due to RGWUserInfo creation process.. */
  RGWRados * const store;
  const AuthInfo info;

  RGWRemoteAuthApplier(CephContext * const cct,
                       RGWRados * const store,
                       const AuthInfo info)
    : RGWAuthApplier(cct),
      store(store),
      info(info) {
  }

  virtual void create_account(const rgw_user acct_user,
                              RGWUserInfo& user_info) const;          /* out */

public:
  virtual void load_acct_info(RGWUserInfo& user_info) const override; /* out */
  virtual void load_user_info(rgw_user& auth_user,                    /* out */
                              uint32_t& perm_mask,                    /* out */
                              bool& admin_request) const override;    /* out */
  class Factory;
};

class RGWRemoteAuthApplier::Factory {
protected:
  RGWRados * const store;

public:
  Factory(RGWRados * const store) : store(store) {}
  virtual ~Factory() {}

  virtual aplptr_t create_loader(CephContext * const cct,
                                 const AuthInfo info) const {
    return aplptr_t(new RGWRemoteAuthApplier(cct, store, info));
  }
};


/* Local auth applier targets those auth engines that store user information
 * in the RADOS store. As a consequence, to perform the authentication, they
 * already have RGWUserInfo structure loaded.
 */
class RGWLocalAuthApplier : public RGWAuthApplier {
protected:
  const RGWUserInfo user_info;
  const std::string subuser;

  RGWLocalAuthApplier(CephContext * const cct,
                      const RGWUserInfo& user_info,
                      const std::string subuser)
    : RGWAuthApplier(cct),
      user_info(user_info),
      subuser(subuser) {
  }

  uint32_t get_perm_mask(const std::string& subuser_name,
                         const RGWUserInfo &uinfo) const;

public:
  static const std::string NO_SUBUSER;

  virtual void load_acct_info(RGWUserInfo& user_info) const;     /* out */
  virtual void load_user_info(rgw_user& auth_user,               /* out */
                              uint32_t& perm_mask,               /* out */
                              bool& admin_request) const;        /* out */
  class Factory;
};

class RGWLocalAuthApplier::Factory {
public:
  virtual ~Factory() {}

  virtual aplptr_t create_loader(CephContext * const cct,
                                 const RGWUserInfo& user_info,
                                 const std::string& subuser) const {
    return aplptr_t(new RGWLocalAuthApplier(cct, user_info, subuser));
  }
};


/* Abstract class for authentication backends (auth engines) in RadosGW.
 *
 * An engine is supposed only to:
 *  - authenticate (not authorize!) a given request basing on req_state,
 *  - provide an upper layer with RGWAuthApplier to commit all changes to
 *    data structures (like req_state) and to the RADOS store (creating
 *    an account, synchronizing user personal info).
 *    Auth engine MUST NOT make any changes to req_state nor RADOS store.
 *
 * Separation between authentication and global state modification has been
 * introduced because many auth engines are perfectly orthogonal to applier
 * and thus they can be decoupled. Additional motivation is clearly distinguish
 * all places which can modify underlying data structures. */
class RGWAuthEngine {
protected:
  CephContext * const cct;

  RGWAuthEngine(CephContext * const cct)
    : cct(cct) {
  }
  /* Make the engines non-copyable and non-moveable due to const-correctness
   * and aggregating applier factories less costly and error-prone. */
  RGWAuthEngine(const RGWAuthEngine&) = delete;
  RGWAuthEngine& operator=(const RGWAuthEngine&) = delete;

public:
  /* Fast, non-throwing method for screening whether a concrete engine may
   * be interested in handling a specific request. */
  virtual bool is_applicable() const noexcept = 0;

  /* Throwing method for identity verification. When the check is positive
   * an implementation should return RGWAuthApplier::aplptr_t containing
   * a non-null pointer to object conforming the RGWAuthApplier interface.
   * Otherwise, the authentication is treated as failed.
   * An error may be signalised by throwing an exception of int type with
   * errno value inside. Those value are always negative. */
  virtual RGWAuthApplier::aplptr_t authenticate() const = 0;

  virtual ~RGWAuthEngine() {};
};


/* Abstract base class for all token-based auth engines. */
class RGWTokenBasedAuthEngine : public RGWAuthEngine {
protected:
  const std::string token;

public:
  class Extractor {
  public:
    virtual ~Extractor() {};
    virtual std::string get_token() const = 0;
  };

  RGWTokenBasedAuthEngine(CephContext * const cct,
                          const Extractor& extr)
    : RGWAuthEngine(cct),
      token(extr.get_token()) {
  }

  bool is_applicable() const noexcept override {
    return !token.empty();
  }
};

/* Keystone. */
class RGWKeystoneAuthEngine : public RGWTokenBasedAuthEngine {
protected:
  const RGWRemoteAuthApplier::Factory * const apl_factory;

  /* Helper methods. */
  KeystoneToken decode_pki_token(const std::string token) const;
  KeystoneToken get_from_keystone(const std::string token) const;
  RGWRemoteAuthApplier::AuthInfo get_creds_info(const KeystoneToken& token,
                                                const std::vector<std::string>& admin_roles
                                               ) const noexcept;
public:
  RGWKeystoneAuthEngine(CephContext * const cct,
                        const Extractor& extr,
                        const RGWRemoteAuthApplier::Factory * const apl_factory)
    : RGWTokenBasedAuthEngine(cct, extr),
      apl_factory(apl_factory) {
  }

  bool is_applicable() const noexcept override;
  RGWAuthApplier::aplptr_t authenticate() const override;
};


/* Anonymous */
class RGWAnonymousAuthEngine : public RGWAuthEngine {
  const RGWLocalAuthApplier::Factory * const apl_factory;

public:
  RGWAnonymousAuthEngine(CephContext * const cct,
                         const RGWLocalAuthApplier::Factory * const apl_factory)
    : RGWAuthEngine(cct),
      apl_factory(apl_factory) {
  }

  bool is_applicable() const noexcept override {
    return true;
  }

  RGWAuthApplier::aplptr_t authenticate() const override;
};

#endif /* CEPH_RGW_AUTH_H */
