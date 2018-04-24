// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Daniel Oliveira <doliveira@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef LDAP_AUTH_MECHANISM_HPP
#define LDAP_AUTH_MECHANISM_HPP

/* Include order and names:
 * a) Immediate related header
 * b) C libraries (if any),
 * c) C++ libraries,
 * d) Other support libraries
 * e) Other project's support libraries
 *
 * Within each section the includes should
 * be ordered alphabetically.
 */

#include <algorithm>
#include <cstring>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "auth_mechanism.hpp"
#include "auth_options.hpp"
#include "ldap_utils.hpp"


/*
 * Bind operations are used to authenticate clients (and the users or
 * applications behind them) to the directory server, to establish an
 * authorization identity that will be used for subsequent operations processed
 * on that connection, and to specify the LDAP protocol version that the client
 * will use.
 *
 * Authentication consists of at least two parts:
 *  - identifying a) who or what is authenticating, and b) supplying some kind
 *    of proof of that identity (usually something only that user should know
 *    or have, like a static and/or password, a certificate, a hardware or
 *    software token, and/or biometric information). In many servers, there may
 *    be additional steps, like checking password policy state and other
 *    constraints that must be satisfied to allow the bind to succeed.
 *
 * LDAP bind requests provide the ability to use either simple or SASL
 * authentication. In simple authentication, the account to authenticate is
 * identified by the DN of the entry for that account, and the proof identity
 * comes in the form of a password. The password is transmitted without any form
 * of obfuscation, so it is strongly recommended that simple authentication be
 * used only over an encrypted connection (e.g., one that has been secured by
 * SSL/TLS, or with the StartTLS extended operation).
 *
 * An anonymous simple bind can be performed by providing empty strings as the
 * bind DN and password (technically, the LDAPv3 specification states that only
 * the password must be empty, but this has been responsible for many security
 * problems with LDAP clients in the past, and many servers require that if an
 * empty password is provided then an empty DN must also be given).
 *
 * SASL authentication uses the Simple Authentication and Security Layer, as
 * defined in RFC 4422. SASL is an extensible framework that makes it possible
 * to plug almost any kind of authentication into LDAP (or any of the other
 * protocols that use SASL). SASL authentication is performed with a SASL
 * mechanism name and an encoded set of credentials. Some SASL mechanisms may
 * require the client and server to exchange information multiple times (via
 * multiple bind requests and responses) in order to complete the authentication
 * process.
 *
 * An LDAP bind request includes three elements:
 *  - The LDAP protocol version that the client wants to use. This is an integer
 *    value, and version 3 is the most recent version. Some very old clients (or
 *    clients written with very old APIs) may still use LDAP version 2.
 *  - The DN of the user to authenticate. This should be empty for anonymous
 *  simple authentication, and is typically empty for SASL authentication
 *  because most SASL mechanisms identify the target account in the encoded
 *  credentials. It must be non-empty for non-anonymous simple authentication.
 *  - The credentials for the user to authenticate. For simple authentication,
 *    this is the password for the user specified by the bind DN (or an empty
 *    string for anonymous simple authentication). For SASL authentication, this
 *    is an encoded value that contains the SASL mechanism name and an optional
 *    set of encoded SASL credentials.
 *
 * Note that LDAPv3 does not require clients to perform a bind operation before
 * they can issue other types of requests to the server. If an LDAP client
 * issues some other kind of request without first performing a bind, then the
 * client will be considered unauthenticated. This is the same authentication
 * state that results from an anonymous simple bind (using an empty bind DN and
 * an empty password), and is also the authentication state that results from
 * an unsuccessful bind operation.
 *
 * When a simple bind operation completes, the server will return a basic
 * response that includes a result code, and optional matched DN, diagnostic
 * message, referrals, and/or response controls. A SASL bind response may also
 * include encoded server SASL credentials for use in subsequent processing.
 * For a SASL mechanism that requires multiple request/response cycles, all
 * responses except the last one will include a "SASL bind in progress" result
 * code to indicate that the authentication process has not yet completed.
 *
 * Some of the most common types of results for a bind operation include:
 *  - If the target user was successfully authenticated, then the server should
 *    return a "success" result.
 *  - If the client requests an LDAP protocol version that the server does not
 *    support, then the server should return a "protocol error" result.
 *  - If the client attempts to use a type of authentication that the server
 *    does not support, then it should fail with an "authentication method not
 *    supported" result.
 *  - If the client attempts to use a type of authentication that is not
 *    appropriate for the target user, then it should fail with an
 *    "inappropriate authentication" result.
 *  - If the client attempts to bind as a user that does not exist in the
 *    server, then it should fail with an "invalid credentials" result, although
 *    some servers may use a "no such object" result in this case.
 *  - If the client attempts to bind with incorrect credentials, then it should
 *    fail with an "invalid credentials" result.
 *
 *  For an LDAP Client we need to:
 *  - Initialize an LDAP session
 *  - Bind and Authenticate to an LDAP Server
 *  - Perform the LDAP operations
 *  - Close the Connection to the Server
 *
 *
 * LDAP URL Definition:
 * An LDAP URL begins with the protocol prefix "ldap" and is defined by
 * the following grammar.
 *
 *   <ldapurl> ::= "ldap://" [ <hostport> ] "/" <dn> [ "?" <attributes>
 *                       [ "?" <scope> "?" <filter> ] ]
 *
 *   <hostport> ::= <hostname> [ ":" <portnumber> ]
 *
 *   <dn> ::= a string as defined in RFC 1485
 *
 *   <attributes> ::= NULL | <attributelist>
 *
 *   <attributelist> ::= <attributetype>
 *                       | <attributetype> [ "," <attributelist> ]
 *
 *   <attributetype> ::= a string as defined in RFC 1777
 *
 *   <scope> ::= "base" | "one" | "sub"
 *
 *   <filter> ::= a string as defined in RFC 1558
 *
 * ie: ldap://host:port/baseDN[?attr[?scope[?filter]]]
 *
 *
 * Good References:
 *  - https://tools.ietf.org/html/rfc1959
 *  - https://tools.ietf.org/html/rfc4422
 *  - https://tools.ietf.org/html/rfc4510
 *  - https://tools.ietf.org/html/rfc4511
 *  - https://tools.ietf.org/html/rfc4513
 *  - https://tools.ietf.org/html/rfc4514
 *  - https://tools.ietf.org/html/rfc4516
 *  - https://tools.ietf.org/html/rfc4519
 *
 *
 * This is a C++ wrapper for LDAP C APIs.
 *
 * The C code is usually verbose, memory needs to be manually managed and it’s
 * really easy to forget the crucial bits. Therefore, the motivation for this.
 *
 * C++'s RAII (Resource Acquisition Is Initialization), semantics (destructors,
 * copy constructors, operator overloads, templates and move constructors,
 * resulting in code that is very easy to read, understand and not too hard
 * to reason about.
 *
 *
 *
 */

namespace ldap_client_auth {

class LDAPEntry;
class LDAPModify;


using vect_string_t           = std::vector<std::string>;
using LDAPEntries_t           = std::vector<LDAPEntry>;
using LDAPAttributes_t        = std::vector<std::string>;

using charPtr_t               = char*;
using LDAPPtr_t               = LDAP*;
using LDAPMsgPtr_t            = LDAPMessage*;
using LDAPBerValPtr_t         = berval*;
using LDAPBervalPtr_t         = BerValue*;
using LDAPBerElemPtr_t        = BerElement*;
using LDAPModPtr_t            = LDAPMod*;
using LDAPEntriesPtr_t        = LDAPEntries_t*;
using LDAPAttributesPtr_t     = LDAPAttributes_t*;

using LDAPModificationsPtr_t  = LDAPModify*;
using LDAPModifications_t     = std::vector<LDAPModificationsPtr_t>;


class LDAPMechanismBase //: public auth_mechanisms::AuthMechanism
{

  public:
    LDAPMechanismBase();
    explicit LDAPMechanismBase(const auth_mechanisms::AuthOptions&);
    LDAPMechanismBase(const std::string&,
                      const ldap_utils::LDAPSSLOption& =
                            ldap_utils::LDAPSSLOption::OFF);
    virtual ~LDAPMechanismBase() = 0;
    LDAPMechanismBase(const LDAPMechanismBase&) = delete;
    auth_mechanisms::AuthenticationStatus get_auth_status() const;

    //-- TODO.
    const std::string get_ldap_uri() const
      { return m_ldap_uri; }

    const std::string get_ldap_bind_dn() const
      { return m_ldap_bind_dn; }

    const auto get_ldap_status() const
      { return m_ldap_status; }

    int32_t ldap_connect(const std::string& = common_utils::EMPTY_STR,
                         const ldap_utils::LDAPSSLOption& =
                         ldap_utils::LDAPSSLOption::OFF);
    void ldap_disconnect();

    int32_t ldap_bind();
    int32_t ldap_bind(const std::string&, const std::string&);
    void ldap_unbind();

    bool ldap_ping() const;

    LDAPEntries_t ldap_search(const std::string&,
                              const std::string&,
                              const ldap_utils::LDAPSearchScope& =
                              ldap_utils::LDAPSearchScope::BASE,
                              const LDAPAttributesPtr_t = nullptr);

    int32_t ldap_add_entry(const LDAPEntry&);
    int32_t ldap_modify_entry(const LDAPEntry&);
    int32_t ldap_delete_entry(const std::string&);
    int32_t ldap_modify_entry_dn(const std::string&,
                                 const std::string&);

    int32_t ldap_compare_attribute(const std::string&,
                                   const std::string&) const;

    const std::string get_ldap_message() const {
      return m_ldap_connection_handler ?
             std::string(ldap_err2string(m_ldap_status)) :
             common_utils::EMPTY_STR;
    }


  private:


  protected:
    auth_mechanisms::AuthOptions m_authentication_options;
    ldap_utils::LDAPRequestType m_ldap_request_type{
        ldap_utils::LDAPRequestType::NONE};
    ldap_utils::LDAPSearchScope m_ldap_search_scope{
        ldap_utils::LDAPSearchScope::NONE};
    ldap_utils::LDAPSSLOption m_ssl_option{
        ldap_utils::LDAPSSLOption::OFF};
    LDAPPtr_t m_ldap_connection_handler{nullptr};
    bool m_is_referral{false};
    bool m_is_caching{false};
    int m_ldap_query_size_limit{0};
    int m_ldap_status{0};
    std::string m_ldap_uri{""};
    std::string m_ldap_bind_dn{""};
    std::string m_ldap_bind_passwd{""};
    u_short m_ldap_port{0};
    std::string ldap_build_uri(const std::string&,
                               const ldap_utils::LDAPSSLOption&);

}; //-- class LDAPMechanismBase


class LDAPBasicEncodeRuleValue : public berval
{

  public:
    LDAPBasicEncodeRuleValue() {
      bv_len = common_utils::ZERO;
      bv_val = nullptr;
    };
    LDAPBasicEncodeRuleValue(const std::string& ldap_value) :
        LDAPBasicEncodeRuleValue() {
      ldap_ber_set_values(ldap_value, ldap_value.size());
    }
    LDAPBasicEncodeRuleValue(const berval& ldap_ber) :
        LDAPBasicEncodeRuleValue() {
      ldap_ber_set_values(ldap_ber.bv_val, ldap_ber.bv_len);
    }
    ~LDAPBasicEncodeRuleValue() {
      ldap_ber_clear();
    }

    LDAPBasicEncodeRuleValue& operator= (const berval& ldap_ber) {
      if (&ldap_ber != this) {
        ldap_ber_set_values(ldap_ber.bv_val, ldap_ber.bv_len);
      }
      return *this;
    }

    void ldap_ber_clear() {
      if (bv_val) {
        delete[] bv_val;
      }
      bv_len = common_utils::ZERO;
      bv_val = nullptr;
    };

    void ldap_ber_set_values(const std::string& ldap_value,
                             size_t ldap_value_size) {
      if (ldap_value.empty() || !(ldap_value_size)) {
        return;
      }
      ldap_ber_clear();
      bv_len = ldap_value_size;
      bv_val = (new char[ldap_value_size]);
      std::memcpy(bv_val, ldap_value.c_str(), ldap_value_size);
    }
};


class LDAPModify : protected LDAPMod
{

  public:
    LDAPModify(const std::string&, const ldap_utils::LDAPModifyOperation&);
    ~LDAPModify();

    template<typename T>
    void ldap_modify_append(const T&);
    template<typename T>
    void ldap_modify_append(const T*);

    void ldap_modify_clear();
    bool is_it_type(const std::string&) const;
    bool is_it_operation(const ldap_utils::LDAPModifyOperation&) const;

    const charPtr_t* get_string_value() const;
    //const std::string get_string_value() const;
    const LDAPBerValPtr_t* get_binary_value() const;


  private:
    size_t m_ldap_modify_size;

    friend class LDAPMechanismBase;
    friend std::ostream& operator<< (std::ostream&, const LDAPModify&);

};


class LDAPEntry : protected LDAPModifications_t
{

  public:
    LDAPEntry(const std::string&);
    ~LDAPEntry();

    void ldap_entry_set_dn(const std::string& ldap_entry_dn)
      { m_ldap_entry_dn = ldap_entry_dn; };

    const std::string& ldap_get_entry_dn() const
      { return m_ldap_entry_dn; };

    template<typename T>
    void ldap_entry_append(const ldap_utils::LDAPModifyOperation&,
                           const std::string&,
                           const T&);
    template<typename T>
    T get_string_value(const std::string&) const;

    template<typename T>
    T get_binary_value(const std::string&) const;


  private:
    std::string m_ldap_entry_dn{""};
    iterator ldap_entry_push_back(LDAPModificationsPtr_t);

    friend class LDAPMechanismBase;
    friend std::ostream& operator<< (std::ostream&, const LDAPEntry&);

};

} //-- namespace ldap_client_auth

#endif    //-- LDAP_AUTH_MECHANISM_HPP

// ----------------------------- END-OF-FILE --------------------------------//
