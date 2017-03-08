// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_AUTHGSSAPIPROTOCOL_H
#define CEPH_AUTHGSSAPIPROTOCOL_H

/* authenticate requests */
#define GSSAPI_MUTUAL_AUTH               0x0100
#define GSSAPI_TOKEN                     0x0200

#define GSSAPI_REQUEST_TYPE_MASK            0x0F00
#define GSSAPI_CRYPT_ERR			1

#include "auth/Auth.h"
#include <errno.h>
#include <sstream>

#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>


string auth_gssapi_display_status(
  OM_uint32 major,
  OM_uint32 minor);

class CephContext;

/*
 * Authentication
 */

// request/reply headers, for subsequent exchanges.

struct GssapiRequestHeader {
  uint16_t request_type;

  void encode(bufferlist& bl) const {
    ::encode(request_type, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(request_type, bl);
  }
};
WRITE_CLASS_ENCODER(GssapiRequestHeader)

struct GssapiResponseHeader {
  uint16_t response_type;

  void encode(bufferlist& bl) const {
    ::encode(response_type, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(response_type, bl);
  }
};
WRITE_CLASS_ENCODER(GssapiResponseHeader)

struct GssapiTokenBlob {
  bufferlist blob;

  GssapiTokenBlob() {}

  void encode(bufferlist& bl) const {
     __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(blob, bl);
  }

  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(blob, bl);
  }
};
WRITE_CLASS_ENCODER(GssapiTokenBlob)


struct GssapiAuthorizer : public AuthAuthorizer {
  GssapiAuthorizer() : AuthAuthorizer(CEPH_AUTH_GSSAPI) { }
  bool build_authorizer(const EntityName &ename, uint64_t global_id) {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(ename, bl);
    ::encode(global_id, bl);
    return 0;
  }
  bool verify_reply(bufferlist::iterator& reply) { return true; }
};

#endif
