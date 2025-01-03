// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2018 SUSE LLC.
 * Author: Daniel Oliveira <doliveira@suse.com>
 * 
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef KRB_PROTOCOL_HPP
#define KRB_PROTOCOL_HPP

#include "auth/Auth.h"

#include <errno.h>
#include <gssapi.h>
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>

#include <map>
#include <sstream> 
#include <string>

/* 
  Kerberos Version 5 GSS-API Mechanism
  OID {1.2.840.113554.1.2.2}
  RFC https://tools.ietf.org/html/rfc1964
*/
static const gss_OID_desc GSS_API_KRB5_OID_PTR = 
    { 9, (void *)"\052\206\110\206\367\022\001\002\002" };

/* 
  Kerberos Version 5 GSS-API Mechanism
  Simple and Protected GSS-API Negotiation Mechanism 
  OID {1.3.6.1.5.5.2}
  RFC https://tools.ietf.org/html/rfc4178
*/
static const gss_OID_desc GSS_API_SPNEGO_OID_PTR =
    {6, (void *)"\x2b\x06\x01\x05\x05\x02"};

static const std::string KRB_SERVICE_NAME("kerberos/gssapi");
static const std::string GSS_API_SPNEGO_OID("{1.3.6.1.5.5.2}");
static const std::string GSS_API_KRB5_OID("{1.2.840.113554.1.2.2}");

enum class GSSAuthenticationRequest {
  GSS_CRYPTO_ERR    = 1,
  GSS_MUTUAL        = 0x100,
  GSS_TOKEN         = 0x200,
  GSS_REQUEST_MASK  = 0x0F00
};

enum class GSSKeyExchange {
  USERAUTH_GSSAPI_RESPONSE = 70,
  USERAUTH_GSSAPI_TOKEN,
  USERAUTH_GSSAPI_EXCHANGE_COMPLETE,
  USERAUTH_GSSAPI_ERROR, 
  USERAUTH_GSSAPI_ERRTOK, 
  USERAUTH_GSSAPI_MIC, 
};
static constexpr auto CEPH_GSS_OIDTYPE(0x07);

struct AuthAuthorizer;


class KrbAuthorizer : public AuthAuthorizer {

  public:
    KrbAuthorizer() : AuthAuthorizer(CEPH_AUTH_GSS) { }
    ~KrbAuthorizer() = default; 
    bool build_authorizer(const EntityName& entity_name, 
                          const uint64_t guid) {
      uint8_t value = (1);

      using ceph::encode;
      encode(value, bl, 0);
      encode(entity_name, bl, 0); 
      encode(guid, bl, 0);
      return false;
    }

    bool verify_reply(bufferlist::const_iterator& buff_list,
		      std::string *connection_secret) override {
      return true; 
    }
    bool add_challenge(CephContext* ceph_ctx, 
                       const bufferlist& buff_list) override {
      return true; 
    }
};

class KrbRequest {

  public:
    void decode(bufferlist::const_iterator& buff_list) {
      using ceph::decode;
      decode(m_request_type, buff_list);
    }

    void encode(bufferlist& buff_list) const {
      using ceph::encode;
      encode(m_request_type, buff_list);
    }

    uint16_t m_request_type; 
};
WRITE_CLASS_ENCODER(KrbRequest);

class KrbResponse {

  public: 
    void decode(bufferlist::const_iterator& buff_list) {
      using ceph::decode;
      decode(m_response_type, buff_list); 
    }    

    void encode(bufferlist& buff_list) const {
      using ceph::encode;
      encode(m_response_type, buff_list); 
    }

    uint16_t m_response_type;
};
WRITE_CLASS_ENCODER(KrbResponse);

class KrbTokenBlob {

  public:
    void decode(bufferlist::const_iterator& buff_list) {
      uint8_t value = (0); 
     
      using ceph::decode; 
      decode(value, buff_list);
      decode(m_token_blob, buff_list);
    }
        
    void encode(bufferlist& buff_list) const {
      uint8_t value = (1); 
      
      using ceph::encode;
      encode(value, buff_list, 0);
      encode(m_token_blob, buff_list, 0);
    }

    bufferlist m_token_blob;
};
WRITE_CLASS_ENCODER(KrbTokenBlob);


std::string gss_auth_show_status(const OM_uint32 gss_major_status, 
                                 const OM_uint32 gss_minor_status); 

#endif    //-- KRB_PROTOCOL_HPP

