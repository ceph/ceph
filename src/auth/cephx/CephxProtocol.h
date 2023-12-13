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

#ifndef CEPH_CEPHXPROTOCOL_H
#define CEPH_CEPHXPROTOCOL_H

/*
  Ceph X protocol

  See doc/dev/cephx.rst

*/

/* authenticate requests */
#define CEPHX_GET_AUTH_SESSION_KEY      0x0100
#define CEPHX_GET_PRINCIPAL_SESSION_KEY 0x0200
#define CEPHX_GET_ROTATING_KEY          0x0400

#define CEPHX_REQUEST_TYPE_MASK            0x0F00
#define CEPHX_CRYPT_ERR			1

#include "auth/Auth.h"
#include <errno.h>
#include <sstream>

#include "include/common_fwd.h"
/*
 * Authentication
 */

// initial server -> client challenge
struct CephXServerChallenge {
  uint64_t server_challenge;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(server_challenge, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(server_challenge, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("server_challenge", server_challenge);
  }
  static void generate_test_instances(std::list<CephXServerChallenge*>& ls) {
    ls.push_back(new CephXServerChallenge);
    ls.back()->server_challenge = 1;
  }
};
WRITE_CLASS_ENCODER(CephXServerChallenge)


// request/reply headers, for subsequent exchanges.

struct CephXRequestHeader {
  __u16 request_type;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(request_type, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(request_type, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("request_type", request_type);
  }
  static void generate_test_instances(std::list<CephXRequestHeader*>& ls) {
    ls.push_back(new CephXRequestHeader);
    ls.back()->request_type = 1;
  }
};
WRITE_CLASS_ENCODER(CephXRequestHeader)

struct CephXResponseHeader {
  uint16_t request_type;
  int32_t status;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(request_type, bl);
    encode(status, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(request_type, bl);
    decode(status, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("request_type", request_type);
    f->dump_int("status", status);
  }
  static void generate_test_instances(std::list<CephXResponseHeader*>& ls) {
    ls.push_back(new CephXResponseHeader);
    ls.back()->request_type = 1;
    ls.back()->status = 0;
  }
};
WRITE_CLASS_ENCODER(CephXResponseHeader)

struct CephXTicketBlob {
  uint64_t secret_id;
  ceph::buffer::list blob;

  CephXTicketBlob() : secret_id(0) {}

  void encode(ceph::buffer::list& bl) const {
     using ceph::encode;
     __u8 struct_v = 1;
     encode(struct_v, bl);
     encode(secret_id, bl);
     encode(blob, bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
     using ceph::decode;
     __u8 struct_v;
     decode(struct_v, bl);
     decode(secret_id, bl);
     decode(blob, bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("secret_id", secret_id);
    f->dump_unsigned("blob_len", blob.length());
  }

  static void generate_test_instances(std::list<CephXTicketBlob*>& ls) {
    ls.push_back(new CephXTicketBlob);
    ls.back()->secret_id = 123;
    ls.back()->blob.append(std::string_view("this is a blob"));
  }
};
WRITE_CLASS_ENCODER(CephXTicketBlob)

// client -> server response to challenge
struct CephXAuthenticate {
  uint64_t client_challenge;
  uint64_t key;
  CephXTicketBlob old_ticket;
  uint32_t other_keys = 0;  // replaces CephXServiceTicketRequest

  bool old_ticket_may_be_omitted;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 3;
    encode(struct_v, bl);
    encode(client_challenge, bl);
    encode(key, bl);
    encode(old_ticket, bl);
    encode(other_keys, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(client_challenge, bl);
    decode(key, bl);
    decode(old_ticket, bl);
    if (struct_v >= 2) {
      decode(other_keys, bl);
    }

    // v2 and v3 encodings are the same, but:
    // - some clients that send v1 or v2 don't populate old_ticket
    //   on reconnects (but do on renewals)
    // - any client that sends v3 or later is expected to populate
    //   old_ticket both on reconnects and renewals
    old_ticket_may_be_omitted = struct_v < 3;
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("client_challenge", client_challenge);
    f->dump_unsigned("key", key);
    f->open_object_section("old_ticket");
    old_ticket.dump(f);
    f->close_section();
    f->dump_unsigned("other_keys", other_keys);
  }
  static void generate_test_instances(std::list<CephXAuthenticate*>& ls) {
    ls.push_back(new CephXAuthenticate);
    ls.back()->client_challenge = 0;
    ls.back()->key = 0;
    ls.push_back(new CephXAuthenticate);
    ls.back()->client_challenge = 1;
    ls.back()->key = 2;
    ls.back()->old_ticket.secret_id = 3;
    ls.back()->old_ticket.blob.append(std::string_view("this is a blob"));
    ls.back()->other_keys = 4;
  }
};
WRITE_CLASS_ENCODER(CephXAuthenticate)

struct CephXChallengeBlob {
  uint64_t server_challenge, client_challenge;
  
  void encode(ceph::buffer::list& bl) const {
     using ceph::encode;
    encode(server_challenge, bl);
    encode(client_challenge, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(server_challenge, bl);
    decode(client_challenge, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("server_challenge", server_challenge);
    f->dump_unsigned("client_challenge", client_challenge);
  }
  static void generate_test_instances(std::list<CephXChallengeBlob*>& ls) {
    ls.push_back(new CephXChallengeBlob);
    ls.back()->server_challenge = 123;
    ls.back()->client_challenge = 456;
  }
};
WRITE_CLASS_ENCODER(CephXChallengeBlob)

void cephx_calc_client_server_challenge(CephContext *cct, 
					CryptoKey& secret, uint64_t server_challenge, uint64_t client_challenge,
					uint64_t *key, std::string &error);


/*
 * getting service tickets
 */
struct CephXSessionAuthInfo {
  uint32_t service_id;
  uint64_t secret_id;
  AuthTicket ticket;
  CryptoKey session_key;
  CryptoKey service_secret;
  utime_t validity;
};


extern bool cephx_build_service_ticket_blob(CephContext *cct,
					    CephXSessionAuthInfo& ticket_info, CephXTicketBlob& blob);

extern void cephx_build_service_ticket_request(CephContext *cct, 
					       uint32_t keys,
					       ceph::buffer::list& request);

extern bool cephx_build_service_ticket_reply(CephContext *cct,
					     CryptoKey& principal_secret,
					     std::vector<CephXSessionAuthInfo> ticket_info,
                                             bool should_encrypt_ticket,
                                             CryptoKey& ticket_enc_key,
					     ceph::buffer::list& reply);

struct CephXServiceTicketRequest {
  uint32_t keys;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(keys, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(keys, bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("keys", keys);
  }

  static void generate_test_instances(std::list<CephXServiceTicketRequest*>& ls) {
    ls.push_back(new CephXServiceTicketRequest);
    ls.back()->keys = 123;
  }
};
WRITE_CLASS_ENCODER(CephXServiceTicketRequest)


/*
 * Authorize
 */

struct CephXAuthorizeReply {
  uint64_t nonce_plus_one;
  std::string connection_secret;
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 1;
    if (connection_secret.size()) {
      struct_v = 2;
    }
    encode(struct_v, bl);
    encode(nonce_plus_one, bl);
    if (struct_v >= 2) {
      struct_v = 2;
      encode(connection_secret, bl);
    }
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(nonce_plus_one, bl);
    if (struct_v >= 2) {
      decode(connection_secret, bl);
    }
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("nonce_plus_one", nonce_plus_one);
    f->dump_string("connection_secret", connection_secret);
  }
  static void generate_test_instances(std::list<CephXAuthorizeReply*>& ls) {
    ls.push_back(new CephXAuthorizeReply);
    ls.back()->nonce_plus_one = 0;
    ls.push_back(new CephXAuthorizeReply);
    ls.back()->nonce_plus_one = 123;
    ls.back()->connection_secret = "secret";
  }
};
WRITE_CLASS_ENCODER(CephXAuthorizeReply)


struct CephXAuthorizer : public AuthAuthorizer {
private:
  CephContext *cct;
public:
  uint64_t nonce;
  ceph::buffer::list base_bl;

  explicit CephXAuthorizer(CephContext *cct_)
    : AuthAuthorizer(CEPH_AUTH_CEPHX), cct(cct_), nonce(0) {}

  bool build_authorizer();
  bool verify_reply(ceph::buffer::list::const_iterator& reply,
		    std::string *connection_secret) override;
  bool add_challenge(CephContext *cct, const ceph::buffer::list& challenge) override;
};



/*
 * TicketHandler
 */
struct CephXTicketHandler {
  uint32_t service_id;
  CryptoKey session_key;
  CephXTicketBlob ticket;        // opaque to us
  utime_t renew_after, expires;
  bool have_key_flag;

  CephXTicketHandler(CephContext *cct_, uint32_t service_id_)
    : service_id(service_id_), have_key_flag(false), cct(cct_) { }

  // to build our ServiceTicket
  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 ceph::buffer::list::const_iterator& indata);
  // to access the service
  CephXAuthorizer *build_authorizer(uint64_t global_id) const;

  bool have_key();
  bool need_key() const;

  void invalidate_ticket() {
    have_key_flag = false;
  }
private:
  CephContext *cct;
};

struct CephXTicketManager {
  typedef std::map<uint32_t, CephXTicketHandler> tickets_map_t;
  tickets_map_t tickets_map;
  uint64_t global_id;

  explicit CephXTicketManager(CephContext *cct_) : global_id(0), cct(cct_) {}

  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 ceph::buffer::list::const_iterator& indata);

  CephXTicketHandler& get_handler(uint32_t type) {
    tickets_map_t::iterator i = tickets_map.find(type);
    if (i != tickets_map.end())
      return i->second;
    CephXTicketHandler newTicketHandler(cct, type);
    std::pair < tickets_map_t::iterator, bool > res =
	tickets_map.insert(std::make_pair(type, newTicketHandler));
    ceph_assert(res.second);
    return res.first->second;
  }
  CephXAuthorizer *build_authorizer(uint32_t service_id) const;
  bool have_key(uint32_t service_id);
  bool need_key(uint32_t service_id) const;
  void set_have_need_key(uint32_t service_id, uint32_t& have, uint32_t& need);
  void validate_tickets(uint32_t mask, uint32_t& have, uint32_t& need);
  void invalidate_ticket(uint32_t service_id);

private:
  CephContext *cct;
};


/* A */
struct CephXServiceTicket {
  CryptoKey session_key;
  utime_t validity;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(session_key, bl);
    encode(validity, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(session_key, bl);
    decode(validity, bl);
  }
  void dump(ceph::Formatter *f) const {
    session_key.dump(f);
    validity.dump(f);
  }
  static void generate_test_instances(std::list<CephXServiceTicket*>& ls) {
    ls.push_back(new CephXServiceTicket);
    ls.push_back(new CephXServiceTicket);
    ls.back()->session_key.set_secret(
      CEPH_CRYPTO_AES, bufferptr("1234567890123456", 16), utime_t(123, 456));
    ls.back()->validity = utime_t(123, 456);
  }
};
WRITE_CLASS_ENCODER(CephXServiceTicket)

/* B */
struct CephXServiceTicketInfo {
  AuthTicket ticket;
  CryptoKey session_key;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(ticket, bl);
    encode(session_key, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(ticket, bl);
    decode(session_key, bl);
  }
  void dump(ceph::Formatter *f) const {
    ticket.dump(f);
    session_key.dump(f);
  }
  static void generate_test_instances(std::list<CephXServiceTicketInfo*>& ls) {
    ls.push_back(new CephXServiceTicketInfo);
    ls.push_back(new CephXServiceTicketInfo);
    ls.back()->ticket.global_id = 1234;
    ls.back()->ticket.init_timestamps(utime_t(123, 456), utime_t(123, 456));
    ls.back()->session_key.set_secret(
      CEPH_CRYPTO_AES, bufferptr("1234567890123456", 16), utime_t(123, 456));
  }
};
WRITE_CLASS_ENCODER(CephXServiceTicketInfo)

struct CephXAuthorizeChallenge : public AuthAuthorizerChallenge {
  uint64_t server_challenge = 0;
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(server_challenge, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(server_challenge, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("server_challenge", server_challenge);
  }
  static void generate_test_instances(std::list<CephXAuthorizeChallenge*>& ls) {
    ls.push_back(new CephXAuthorizeChallenge);
    ls.back()->server_challenge = 1234;
  }
};
WRITE_CLASS_ENCODER(CephXAuthorizeChallenge)

struct CephXAuthorize {
  uint64_t nonce = 0;
  bool have_challenge = false;
  uint64_t server_challenge_plus_one = 0;
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 struct_v = 2;
    encode(struct_v, bl);
    encode(nonce, bl);
    encode(have_challenge, bl);
    encode(server_challenge_plus_one, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(nonce, bl);
    if (struct_v >= 2) {
      decode(have_challenge, bl);
      decode(server_challenge_plus_one, bl);
    }
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("nonce", nonce);
    f->dump_unsigned("have_challenge", have_challenge);
    f->dump_unsigned("server_challenge_plus_one", server_challenge_plus_one);
  }
  static void generate_test_instances(std::list<CephXAuthorize*>& ls) {
    ls.push_back(new CephXAuthorize);
    ls.push_back(new CephXAuthorize);
    ls.back()->nonce = 1234;
    ls.back()->have_challenge = true;
    ls.back()->server_challenge_plus_one = 1234;
  }
};
WRITE_CLASS_ENCODER(CephXAuthorize)

/*
 * Decode an extract ticket
 */
bool cephx_decode_ticket(CephContext *cct, KeyStore *keys,
			 uint32_t service_id,
			 const CephXTicketBlob& ticket_blob,
			 CephXServiceTicketInfo& ticket_info);

/*
 * Verify authorizer and generate reply authorizer
 */
extern bool cephx_verify_authorizer(
  CephContext *cct,
  const KeyStore& keys,
  ceph::buffer::list::const_iterator& indata,
  size_t connection_secret_required_len,
  CephXServiceTicketInfo& ticket_info,
  std::unique_ptr<AuthAuthorizerChallenge> *challenge,
  std::string *connection_secret,
  ceph::buffer::list *reply_bl);






/*
 * encode+encrypt macros
 */
static constexpr uint64_t AUTH_ENC_MAGIC = 0xff009cad8826aa55ull;

template <typename T>
void decode_decrypt_enc_bl(CephContext *cct, T& t, CryptoKey key,
			   const ceph::buffer::list& bl_enc,
			   std::string &error)
{
  uint64_t magic;
  ceph::buffer::list bl;

  if (key.decrypt(cct, bl_enc, bl, &error) < 0)
    return;

  auto iter2 = bl.cbegin();
  __u8 struct_v;
  using ceph::decode;
  decode(struct_v, iter2);
  decode(magic, iter2);
  if (magic != AUTH_ENC_MAGIC) {
    std::ostringstream oss;
    oss << "bad magic in decode_decrypt, " << magic << " != " << AUTH_ENC_MAGIC;
    error = oss.str();
    return;
  }

  decode(t, iter2);
}

template <typename T>
void encode_encrypt_enc_bl(CephContext *cct, const T& t, const CryptoKey& key,
			   ceph::buffer::list& out, std::string &error)
{
  ceph::buffer::list bl;
  __u8 struct_v = 1;
  using ceph::encode;
  encode(struct_v, bl);
  uint64_t magic = AUTH_ENC_MAGIC;
  encode(magic, bl);
  encode(t, bl);

  key.encrypt(cct, bl, out, &error);
}

template <typename T>
int decode_decrypt(CephContext *cct, T& t, const CryptoKey& key,
		    ceph::buffer::list::const_iterator& iter, std::string &error)
{
  ceph::buffer::list bl_enc;
  using ceph::decode;
  try {
    decode(bl_enc, iter);
    decode_decrypt_enc_bl(cct, t, key, bl_enc, error);
  }
  catch (ceph::buffer::error &e) {
    error = "error decoding block for decryption";
  }
  if (!error.empty())
    return CEPHX_CRYPT_ERR;
  return 0;
}

template <typename T>
int encode_encrypt(CephContext *cct, const T& t, const CryptoKey& key,
		    ceph::buffer::list& out, std::string &error)
{
  using ceph::encode;
  ceph::buffer::list bl_enc;
  encode_encrypt_enc_bl(cct, t, key, bl_enc, error);
  if (!error.empty()){
    return CEPHX_CRYPT_ERR;
  }
  encode(bl_enc, out);
  return 0;
}

#endif
