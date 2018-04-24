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

#ifndef GSS_AUTH_MECHANISM_HPP
#define GSS_AUTH_MECHANISM_HPP

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
#include <string>

#include "auth_mechanism.hpp"
#include "gss_utils.hpp"


/*
 * We certainly could have used straight KRB5 APIs, but we wanted a more
 * portable option as regards network security, which is the hallmark of
 * the GSS(Generic Security Standard)-API. It does not actually provide
 * security services itself. Rather, it is a framework that provides
 * security services to callers in a generic way.
 *
 *  +---------------------------------+
 *  |        Application              |
 *  +---------------------------------+
 *  | Protocol (RPC, Etc. [Optional]) |
 *  +---------------------------------+
 *  |         GSS-API                 |
 *  +---------------------------------+
 *  |   Security Mechs (Krb v5, Etc)  |
 *  +---------------------------------+
 *
 * The GSS-API does two main things:
 *  1. It creates a security context in which data can be passed between
 *     applications. A context can be thought of as a sort of "state of trust"
 *     between two applications.
 *     Applications that share a context know who each other are and thus can
 *     permit data transfers between them as long as the context lasts.
 *
 *  2. It applies one or more types of protection, known as "security services",
 *     to the data to be transmitted.
 *
 *
 * GSS-API provides several types of portability for applications:
 * a. Mechanism independence. GSS-API provides a generic interface to the
 *    mechanisms for which it has been implemented. By specifying a default
 *    security mechanism, an application does not need to know which mechanism
 *    it is using (for example, Kerberos v5), or even what type of mechanism
 *    it uses. As an example, when an application forwards a user's credential
 *    to a server, it does not need to know if that credential has a Kerberos
 *    format or the format used by some other mechanism, nor how the
 *    credentials are stored by the mechanism and accessed by the application.
 *    (If necessary, an application can specify a particular mechanism to use)
 * b. Protocol independence. The GSS-API is independent of any communications
 *    protocol or protocol suite. It can be used with applications that use,
 *    for example, sockets, RCP, or TCP/IP.
 *    RPCSEC_GSS "RPCSEC_GSS Layer" is an additional layer that smoothly
 *    integrates GSS-API with RPC.
 * c. Platform independence. The GSS-API is completely oblivious to the type
 *    of operating system on which an application is running.
 * d. Quality of Protection independence. Quality of Protection (QOP) is the
 *    name given to the type of algorithm used in encrypting data or generating
 *    cryptographic tags; the GSS-API allows a programmer to ignore QOP, using
 *    a default provided by the GSS-API.
 *    (On the other hand, an application can specify the QOP if necessary.)
 *
 *
 * The basic security offered by the GSS-API is authentication. Authentication
 * is the verification of an identity: if you are authenticated, it means that
 * you are recognized to be who you say you are.
 *
 * The GSS-API provides for two additional security services, if supported by
 * the underlying mechanisms:
 * 1. Integrity. It's not always sufficient to know that an application sending
 *    you data is who it claims to be. The data itself could have become
 *    corrupted or compromised. The GSS-API provides for data to be accompanied
 *    by a cryptographic tag, known as an Message Integrity Code (MIC), to
 *    prove that the data that arrives at your doorstep is the same as the data
 *    that the sender transmitted. This verification of the data's validity is
 *    known as "integrity".
 * 2. Confidentiality. Both authentication and integrity, however, leave the
 *    data itself alone, so if it's somehow intercepted, others can read it.
 *    The GSS-API therefore allows data to be encrypted, if underlying
 *    mechanisms support it.
 *    This encryption of data is known as "confidentiality".
 *
 *
 * Mechanisms Available With GSS-API
 * ---------------------------------
 *  The current implementation of the GSS-API works only with the Kerberos v5
 *  security mechanism.
 *
 *
 * Although the GSS-API makes protecting data simple, it does not do certain
 * things, in order to maximize its generic nature. These include:
 * a. Provide security credentials for a user or application. These must be
 *    provided by the underlying security mechanism(s). The GSS-API does allow
 *    applications to acquire credentials, either automatically or explicitly.
 * b. Transfer data between applications. It is the application's
 *    responsibility to handle the transfer of all data between peers, whether
 *    it is security-related or "plain" data.
 * c. Distinguish between different types of transmitted data (for example,
 *    to know or determine that a data packet is plain data and not GSS-API
 *    related).
 * d. Indicate status due to remote (asynchronous) errors.
 * e. Automatically protect information sent between processes of a
 *    multiprocess program.
 * f. Allocate string buffers ("Strings and Similar Data") to be passed to
 *    GSS-API functions.
 * g. Deallocate GSS-API data spaces. These must be explicitly deallocated
 *    with functions such as gss_release_buffer() and gss_delete_name().
 *
 *
 * These are the basic steps in using the GSS-API
 * ----------------------------------------------
 * 1. Each application, sender and recipient, acquires credentials explicitly,
 *    if credentials have not been acquired automatically.
 * 2. The sender initiates a security context and the recipient accepts it.
 * 3. The sender applies security protection to the message (data) it wants to
 *    transmit. This means that it either encrypts the message or stamps it
 *    with an identification tag. The sender transmits the protected message.
 *    (The sender can choose not to apply either security protection, in which
 *    case the message has only the default GSS-API security service associated
 *    with it. That is authentication, in which the recipient knows that the
 *    sender is who it claims to be.)
 * 4. The recipient decrypts the message (if needed) and verifies it
 *    (if appropriate).
 * 5. (Optional) The recipient returns an identification tag to the sender for
 *    confirmation.
 * 6. Both applications destroy the shared security context. If necessary,
 *    they can also deallocate any "leftover" GSS-API data.
 *
 * Applications that use the GSS-API should include the file gssapi.h.
 *
 *
 * Good References:
 *  - https://tools.ietf.org/html/rfc1964
 *  - https://tools.ietf.org/html/rfc2743
 *  - https://tools.ietf.org/html/rfc2744
 *  - https://tools.ietf.org/html/rfc4178 (Simple and Protected GSS-API Negotiation
 *                                        (SPNEGO) mechanism. [Obsoletes 2478])
 *  - https://tools.ietf.org/html/rfc6649
 *  - https://web.mit.edu/kerberos/krb5-latest/doc/appdev/gssapi.html
 *  - https://docs.oracle.com/cd/E19683-01/816-1331/index.html
 *  - https://docs.oracle.com/cd/E19683-01/816-1331/6m7oo9sn5/index.html
 *  - https://docs.oracle.com/cd/E19683-01/816-1331/overview-44/index.html
 *  - http://www.gnu.org/software/gss/manual/gss.html#GSS_002dAPI-Overview
 *
 *
 * This is a C++ wrapper for GSS C APIs.
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

namespace gss_client_auth {

// Possible/valid gss authentication options.
enum class GSSAuthenticationOptions {
  SPNEGO,
  KRB5,
  GSS_OID,
};

enum class GSSAuthenticationRequest {
  GSS_CRYPTO_ERR    = 1,
  GSS_MUTUAL        = 0x100,
  GSS_TOKEN         = 0x200,
  GSS_REQUEST_MASK  = 0x0F00
};

static constexpr auto BUFF_IN_SIZE  = 8192;
static constexpr auto BUFF_OUT_SIZE = 8192;

/// All the similarities in between clients and servers will be captured
/// in this class. For example, both servers and clients need to produce
/// and process context-level GSS and message-level tokens
/// (INITIATE and MESSAGE commands respectively).

class GSSNameType;
class GSSSecCredential;
class GSSSecurityCtx;
class GSSDataBuffer;

class GSSMechanismBase //: public auth_mechanisms::AuthMechanism
{
  public:
      /*explicit*/ GSSMechanismBase(const std::string&);
      GSSMechanismBase() : m_gss_oid(GSS_C_NO_OID) { }
      /*explicit*/ GSSMechanismBase(gss_OID gss_oid) : m_gss_oid(gss_oid) { }

      /*
      GSSMechanismBase(const gss_OID& gss_oid) {
        m_gss_oid = gss_oid;
      }
      GSSMechanismBase& operator= (const GSSMechanismBase& gss_mechanism) {
        m_gss_oid = gss_mechanism.m_gss_oid;
        return *this;
      }
      */

      size_t get_size() const { return m_gss_oid->length; }
      std::string get_string() const;

      /*explicit*/ operator gss_OID() { return m_gss_oid; }
      /*explicit*/ operator gss_OID*() { return &m_gss_oid; }


  private:
      gss_OID m_gss_oid;

};  //-- class GSSMechanismBase


class GSSMechanismList
{
  public:
    GSSMechanismList();
    ~GSSMechanismList() { gss_mechanism_clear(); }

    void gss_mechanism_get();
    void gss_mechanism_clear();
    size_t gss_mechanism_getsize()  const { return m_gss_oid_set->count; }
    bool is_it_empty() const { return !gss_mechanism_getsize(); }

    void gss_mechanism_add(const GSSMechanismBase&);
    bool is_it_gss_mechanism_set(const GSSMechanismBase&) const;
    const GSSMechanismBase at(size_t idx) const;

    const GSSMechanismBase operator[] (size_t idx) const;
    GSSMechanismList& operator+= (const GSSMechanismBase&);
    operator gss_OID_set() { return m_gss_oid_set; }
    operator gss_OID_set*() { return &m_gss_oid_set; }

  private:
    gss_OID_set m_gss_oid_set;
};

class GSSNameType
{
  public:
    /* Taken from ./include/gssapi/gssapi.h
    */
    enum class GSSNameOption {
        NT_NO_OID,
        NT_USER_NAME,
        NT_MACHINE_UID_NAME,
        NT_STRING_UID_NAME,
        NT_HOSTBASED_SERVICE,
        NT_ANONYMOUS,
        NT_EXPORT_NAME
    };

    /* Update this if more name options are added.
    */
    static constexpr auto MAX_NAME_OPTIONS = 7;

    GSSNameType() : m_gss_type_name( GSS_C_NO_NAME ) { }
    GSSNameType(const GSSNameType&);
    /*explicit*/ GSSNameType(const gss_name_t&);
    /*explicit*/ GSSNameType(const GSSDataBuffer&,
                             GSSNameOption =
                                GSSNameOption::NT_HOSTBASED_SERVICE);
    /*explicit*/ GSSNameType(const char*,
                             GSSNameOption =
                                GSSNameOption::NT_HOSTBASED_SERVICE);
    /*explicit*/ GSSNameType(const std::string&,
                             GSSNameOption  =
                                GSSNameOption::NT_HOSTBASED_SERVICE);
    ~GSSNameType() { gss_name_type_clear(); }

    void gss_name_type_set(const GSSNameType&);
    void gss_name_type_set(const gss_name_t&);
    void gss_name_type_set(const GSSDataBuffer&,
                           GSSNameOption =
                              GSSNameOption::NT_HOSTBASED_SERVICE);
    void gss_name_type_set(const char*,
                           GSSNameOption =
                              GSSNameOption::NT_HOSTBASED_SERVICE);
    void gss_name_type_set(const std::string&,
                           GSSNameOption =
                              GSSNameOption::NT_HOSTBASED_SERVICE);

    GSSNameType& operator= (const GSSNameType& lhs)
    { gss_name_type_set(lhs); return *this; }
    GSSNameType& operator= (const GSSDataBuffer& lhs)
    { gss_name_type_set(lhs); return *this; }
    GSSNameType& operator= (const gss_name_t& lhs)
    { gss_name_type_set(lhs); return *this; }
    GSSNameType& operator= (const char* lhs)
    { gss_name_type_set(lhs); return *this; }
    GSSNameType& operator= (const std::string& lhs)
    { gss_name_type_set(lhs); return *this; }

    bool operator== (const GSSNameType&) const;
    bool operator!= (const GSSNameType&) const;
    bool operator== (const std::string&) const;
    bool operator!= (const std::string&) const;

    /*explicit*/ operator gss_name_t*() { return &m_gss_type_name; }
    /*explicit*/ operator gss_name_t() { return m_gss_type_name; }
    /*explicit*/ operator const gss_name_t() const { return m_gss_type_name; }

    void swap(gss_name_t&);
    void swap(GSSNameType&);

    void gss_name_type_clear();
    bool is_it_empty() const { return (m_gss_type_name == GSS_C_NO_NAME); }
    bool is_it_valid_type_name() const { return !is_it_empty(); }

    std::string get_string(GSSNameOption* = nullptr) const;
    GSSNameType standardize_type_name(const GSSMechanismBase&) const;
    GSSDataBuffer gss_name_export(const GSSMechanismBase&) const;
    void gss_name_import(const GSSDataBuffer&, GSSNameOption&);


  private:
    gss_name_t m_gss_type_name;
    static const std::array<gss_OID, MAX_NAME_OPTIONS> m_gss_oid_types;

};


class GSSSecCredential
{
  public:
    GSSSecCredential() : m_gss_credential(GSS_C_NO_CREDENTIAL) { }
    /*explicit*/ GSSSecCredential(gss_cred_id_t gss_credential) :
        m_gss_credential(gss_credential) { }
    /*explicit*/ GSSSecCredential(const GSSNameType&);
    ~GSSSecCredential() { gss_credential_clear(); }

    /*  No *copy* [constructor OR assignment] allowed.
    */
    GSSSecCredential(const GSSSecCredential&) = delete;
    GSSSecCredential& operator= (const GSSSecCredential&) = delete;

    /*explicit*/ operator gss_cred_id_t() { return m_gss_credential; }
    /*explicit*/ operator gss_cred_id_t*() { return &m_gss_credential; }

    void gss_credential_clear();


  private:
    gss_cred_id_t m_gss_credential;

};


class GSSSecurityCtx
{
  public:
    GSSSecurityCtx() : m_gss_ctx(GSS_C_NO_CONTEXT) { }
    /*explicit*/ GSSSecurityCtx(const GSSDataBuffer&);

    GSSSecurityCtx& operator= (const GSSSecurityCtx&);
    ~GSSSecurityCtx() { gss_ctx_clear(); }

    void gss_ctx_clear();
    bool is_it_valid_ctx() { return m_gss_ctx != GSS_C_NO_CONTEXT; }

    gss_ctx_id_t get_raw_ctx() { return m_gss_ctx; }
    gss_ctx_id_t get_raw_ctx() const { return m_gss_ctx; }

    operator gss_ctx_id_t() { return get_raw_ctx();  }
    operator const gss_ctx_id_t() const { return get_raw_ctx();  }
    operator gss_ctx_id_t*() { return &m_gss_ctx; }
    operator const gss_ctx_id_t*() const { return &m_gss_ctx; }

    class GSSSecurityCtxFlag
    {
      public:
        GSSSecurityCtxFlag() : m_gss_ctx_flags(0) { }
        /*explicit*/ GSSSecurityCtxFlag(OM_uint32 gss_ctx_flags) :
            m_gss_ctx_flags(gss_ctx_flags) { }

        void gss_ctx_flag_set(OM_uint32 gss_flag_bit,
                              bool is_it_enabled = true) {
          if (is_it_enabled) {
            m_gss_ctx_flags |= gss_flag_bit;
          }
          else {
            m_gss_ctx_flags &= ~gss_flag_bit;
          }
        }

        bool gss_ctx_flag_check(OM_uint32 gss_flag_bit) const
        { return (m_gss_ctx_flags & gss_flag_bit); }

        bool gss_ctx_deleg_flag() const
        { return gss_ctx_flag_check(GSS_C_DELEG_FLAG); }
        bool gss_ctx_mutual_flag() const
        { return gss_ctx_flag_check(GSS_C_MUTUAL_FLAG); }
        bool gss_ctx_replay_flag() const
        { return gss_ctx_flag_check(GSS_C_REPLAY_FLAG); }
        bool gss_ctx_sequence_flag() const
        { return gss_ctx_flag_check(GSS_C_SEQUENCE_FLAG); }
        bool gss_ctx_conf_flag() const
        { return gss_ctx_flag_check(GSS_C_CONF_FLAG); }
        bool gss_ctx_integ_flag() const
        { return gss_ctx_flag_check(GSS_C_INTEG_FLAG); }
        bool gss_ctx_anon_flag() const
        { return gss_ctx_flag_check(GSS_C_ANON_FLAG); }

        void gss_ctx_deleg_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_DELEG_FLAG, is_it_enabled); }
        void gss_ctx_mutual_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_MUTUAL_FLAG, is_it_enabled); }
        void gss_ctx_replay_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_REPLAY_FLAG, is_it_enabled); }
        void gss_ctx_sequence_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_SEQUENCE_FLAG, is_it_enabled); }
        void gss_ctx_conf_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_CONF_FLAG, is_it_enabled); }
        void gss_ctx_integ_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_INTEG_FLAG, is_it_enabled); }
        void gss_ctx_anon_flag(bool is_it_enabled)
        { gss_ctx_flag_set(GSS_C_ANON_FLAG, is_it_enabled); }

        operator OM_uint32() const { return m_gss_ctx_flags; }
        OM_uint32 get_gss_ctx_flags() const { return m_gss_ctx_flags; }

      private:
        OM_uint32 m_gss_ctx_flags;
    };

    bool gss_initiate_ctx(GSSDataBuffer&,
                          const GSSNameType&,
                          const GSSSecurityCtxFlag&,
                          const GSSSecCredential& = GSS_C_NO_CREDENTIAL,
                          GSSMechanismBase = GSS_C_NO_OID,
                          OM_uint32 = 0,
                          gss_channel_bindings_t = GSS_C_NO_CHANNEL_BINDINGS);

    bool gss_allow_ctx(GSSDataBuffer&,
                       const GSSSecCredential& = GSS_C_NO_CREDENTIAL,
                       gss_channel_bindings_t = GSS_C_NO_CHANNEL_BINDINGS);

    void gss_ctx_import(const GSSDataBuffer&);
    GSSDataBuffer gss_ctx_export();

    GSSDataBuffer get_gss_ctx_mic(const GSSDataBuffer&,
                                  gss_qop_t = GSS_C_QOP_DEFAULT) const;
    bool gss_ctx_mic_check(const GSSDataBuffer&,
                           const GSSDataBuffer&,
                           gss_qop_t* = nullptr) const;

    GSSDataBuffer gss_msg_wrap(const GSSDataBuffer&, bool,
                               gss_qop_t = GSS_C_QOP_DEFAULT) const;
    void gss_msg_wrap_in_place(GSSDataBuffer&, bool,
                               gss_qop_t = GSS_C_QOP_DEFAULT) const;
    static GSSDataBuffer gss_msg_wrap(const GSSSecurityCtx&,
                                      const GSSDataBuffer&, bool,
                                      gss_qop_t = GSS_C_QOP_DEFAULT);
    static void gss_msg_wrap_in_place(const GSSSecurityCtx&,
                                      GSSDataBuffer&, bool,
                                      gss_qop_t = GSS_C_QOP_DEFAULT);

    GSSDataBuffer gss_msg_unwrap(const GSSDataBuffer&,
                                 bool* = nullptr,
                                 gss_qop_t* = nullptr) const;
    void gss_msg_unwrap_in_place(GSSDataBuffer&, bool* = nullptr,
                                 gss_qop_t* = nullptr) const;
    static GSSDataBuffer gss_msg_unwrap(const GSSSecurityCtx&,
                                        const GSSDataBuffer&,
                                        bool* = nullptr,
                                        gss_qop_t* = nullptr);
    static void gss_msg_unwrap_in_place(const GSSSecurityCtx&,
                                        GSSDataBuffer&,
                                        bool* = nullptr,
                                        gss_qop_t* = nullptr);

    size_t gss_msg_wrap_size_limit(size_t, bool,
                                   gss_qop_t = GSS_C_QOP_DEFAULT) const;

  private:
    gss_ctx_id_t m_gss_ctx;
};


class GSSDataBuffer
{
  public:
    GSSDataBuffer() : m_gss_buffer() { }
    GSSDataBuffer(const GSSDataBuffer&);
    /*explicit*/ GSSDataBuffer(const gss_buffer_desc&);
    /*explicit*/ GSSDataBuffer(size_t);
    /*explicit*/ GSSDataBuffer(const char*);
    GSSDataBuffer(const void*, size_t);
    /*explicit*/ GSSDataBuffer(const std::string&);
    GSSDataBuffer(std::istream&, size_t);
    ~GSSDataBuffer() { gss_buff_clear(); }

    GSSDataBuffer& operator= (const GSSDataBuffer& lhs)
    { gss_buff_set(lhs); return (*this); }

    GSSDataBuffer& operator= (const gss_buffer_desc& lhs)
    { gss_buff_set(lhs); return (*this); }

    GSSDataBuffer& operator= (const char* lhs)
    { gss_buff_set(lhs); return (*this); }

    GSSDataBuffer& operator= (const std::string& lhs)
    { gss_buff_set(lhs); return (*this); }

    void gss_buff_set(const GSSDataBuffer&);
    void gss_buff_set(const gss_buffer_desc&);
    void gss_buff_set(const char*);
    void gss_buff_set(void*, size_t);
    void gss_buff_set(const std::string&);
    void gss_buff_set(std::istream&, size_t);

    void swap(GSSDataBuffer&);
    void swap(gss_buffer_desc&);

    void gss_buff_clear();
    size_t gss_buff_getsize() const { return m_gss_buffer.length; }
    bool is_it_empty() const { return !gss_buff_getsize(); }
    void gss_buff_resize(size_t);

    std::string get_string() const
    { return std::string((char*) m_gss_buffer.value, m_gss_buffer.length); }

    const char* get_bytes() const { return ((char*) m_gss_buffer.value); }
    const gss_buffer_desc_struct* get_raw_buffer() const
    { return &m_gss_buffer; }

    gss_buffer_desc_struct* get_raw_buffer() { return &m_gss_buffer; }

    /*explicit*/ operator const char*() const { return get_bytes(); }
    /*explicit*/ operator std::string() const { return get_string(); }

    /*explicit*/ operator const gss_buffer_desc_struct*() const
    { return &m_gss_buffer; }

    /*explicit*/ operator gss_buffer_desc_struct*()
    { return &m_gss_buffer; }

    /*explicit*/ operator void*()
    { return m_gss_buffer.value; }

    GSSDataBuffer& operator+ (const GSSDataBuffer&);

    template <typename T>
    GSSDataBuffer& operator+ (const T& other)
    { return (*this + GSSDataBuffer((void*) &other, sizeof(T))); }

    template <typename T>
    GSSDataBuffer& operator+ (const T* other)
    { return (*this + GSSDataBuffer((void*) other, sizeof(T))); }

    GSSDataBuffer& operator+= (const GSSDataBuffer& other);

    template <typename T>
    GSSDataBuffer& operator+= (const T& other)
    { return (*this += GSSDataBuffer((void*) &other, sizeof(T))); }

    template <typename T>
    GSSDataBuffer& operator+= (const T* other)
    { return (*this += GSSDataBuffer((void*) other, sizeof(T))); }

    /*
    template <typename T>
    T get_value() {
      assert(sizeof(T) <= m_gss_buffer.length);
      return (*(T*) m_gss_buffer.value);
    }

    template <typename T>
    T& get_value(T& value) {
      assert(sizeof(T) <= m_gss_buffer.length);
      value = (*(T*) m_gss_buffer.value);
      return value;
    }
    */

  private:
    gss_buffer_desc m_gss_buffer;
};

}   //-- namespace gss_client_auth


/*  GSSMechanismBase Operators.
*/
std::ostream& operator<< (std::ostream&,
                          const gss_client_auth::GSSMechanismBase&);


/*  GSSNameType Operators.
*/
bool operator== (const std::string&, const gss_client_auth::GSSNameType&);
bool operator!= (const std::string&, const gss_client_auth::GSSNameType&);
std::ostream& operator<< (std::ostream&,
                          const gss_client_auth::GSSNameType&);


/*  GSSDataBuffer Operators.
*/
bool operator== (const gss_client_auth::GSSDataBuffer&,
                 const gss_client_auth::GSSDataBuffer&);
bool operator== (const gss_client_auth::GSSDataBuffer&,
                 const gss_buffer_desc&);
bool operator== (const gss_buffer_desc&,
                 const gss_client_auth::GSSDataBuffer&);

bool operator!= (const gss_client_auth::GSSDataBuffer&,
                 const gss_client_auth::GSSDataBuffer&);
bool operator!= (const gss_client_auth::GSSDataBuffer&,
                 const gss_buffer_desc&);
bool operator!= (const gss_buffer_desc&,
                 const gss_client_auth::GSSDataBuffer&);

bool operator< (const gss_client_auth::GSSDataBuffer&,
                const gss_client_auth::GSSDataBuffer&);
bool operator< (const gss_client_auth::GSSDataBuffer&,
                const gss_buffer_desc&);
bool operator< (const gss_buffer_desc&,
                const gss_client_auth::GSSDataBuffer&);

bool operator> (const gss_client_auth::GSSDataBuffer&,
                const gss_client_auth::GSSDataBuffer&);
bool operator> (const gss_client_auth::GSSDataBuffer&,
                const gss_buffer_desc&);
bool operator> (const gss_buffer_desc&,
                const gss_client_auth::GSSDataBuffer&);

bool operator<= (const gss_client_auth::GSSDataBuffer&,
                 const gss_client_auth::GSSDataBuffer&);
bool operator<= (const gss_client_auth::GSSDataBuffer&,
                 const gss_buffer_desc&);
bool operator<= (const gss_buffer_desc&,
                 const gss_client_auth::GSSDataBuffer&);

bool operator>= (const gss_client_auth::GSSDataBuffer&,
                 const gss_client_auth::GSSDataBuffer&);
bool operator>= (const gss_client_auth::GSSDataBuffer&,
                 const gss_buffer_desc&);

std::ostream& operator<< (std::ostream&,
                          const gss_client_auth::GSSDataBuffer&);
std::istream& operator>> (std::istream& ,
                          gss_client_auth::GSSDataBuffer&);


#endif    //-- GSS_AUTH_MECHANISM_HPP

// ----------------------------- END-OF-FILE --------------------------------//
