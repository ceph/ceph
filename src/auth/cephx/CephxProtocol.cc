// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "CephxProtocol.h"
#include "common/Clock.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx: "



void cephx_calc_client_server_challenge(CephContext *cct, CryptoKey& secret, uint64_t server_challenge, 
		  uint64_t client_challenge, uint64_t *key, std::string &error)
{
  CephXChallengeBlob b;
  b.server_challenge = server_challenge;
  b.client_challenge = client_challenge;

  bufferlist enc;
  if (encode_encrypt(cct, b, secret, enc, error))
    return;

  uint64_t k = 0;
  const uint64_t *p = (const uint64_t *)enc.c_str();
  for (int pos = 0; pos + sizeof(k) <= enc.length(); pos+=sizeof(k), p++)
    k ^= mswab(*p);
  *key = k;
}


/*
 * Authentication
 */

bool cephx_build_service_ticket_blob(CephContext *cct, CephXSessionAuthInfo& info,
				     CephXTicketBlob& blob)
{
  CephXServiceTicketInfo ticket_info;
  ticket_info.session_key = info.session_key;
  ticket_info.ticket = info.ticket;
  ticket_info.ticket.caps = info.ticket.caps;

  ldout(cct, 10) << "build_service_ticket service "
		 << ceph_entity_type_name(info.service_id)
		 << " secret_id " << info.secret_id
		 << " ticket_info.ticket.name="
		 << ticket_info.ticket.name.to_str()
		 << " ticket.global_id " << info.ticket.global_id << dendl;
  blob.secret_id = info.secret_id;
  std::string error;
  if (!info.service_secret.get_secret().length())
    error = "invalid key";  // Bad key?
  else
    encode_encrypt_enc_bl(cct, ticket_info, info.service_secret, blob.blob, error);
  if (!error.empty()) {
    ldout(cct, -1) << "cephx_build_service_ticket_blob failed with error "
	  << error << dendl;
    return false;
  }
  return true;
}

/*
 * AUTH SERVER: authenticate
 *
 * Authenticate principal, respond with AuthServiceTicketInfo
 *
 * {session key, validity}^principal_secret
 * {principal_ticket, session key}^service_secret  ... "enc_ticket"
 */
bool cephx_build_service_ticket_reply(CephContext *cct,
                     CryptoKey& principal_secret,
                     vector<CephXSessionAuthInfo> ticket_info_vec,
                     bool should_encrypt_ticket,
                     CryptoKey& ticket_enc_key,
                     bufferlist& reply)
{
  __u8 service_ticket_reply_v = 1;
  encode(service_ticket_reply_v, reply);

  uint32_t num = ticket_info_vec.size();
  encode(num, reply);
  ldout(cct, 10) << "build_service_ticket_reply encoding " << num
	   << " tickets with secret " << principal_secret << dendl;

  for (vector<CephXSessionAuthInfo>::iterator ticket_iter = ticket_info_vec.begin(); 
       ticket_iter != ticket_info_vec.end();
       ++ticket_iter) {
    CephXSessionAuthInfo& info = *ticket_iter;
    encode(info.service_id, reply);

    __u8 service_ticket_v = 1;
    encode(service_ticket_v, reply);

    CephXServiceTicket msg_a;
    msg_a.session_key = info.session_key;
    msg_a.validity = info.validity;
    std::string error;
    if (encode_encrypt(cct, msg_a, principal_secret, reply, error)) {
      ldout(cct, -1) << "error encoding encrypted: " << error << dendl;
      return false;
    }

    bufferlist service_ticket_bl;
    CephXTicketBlob blob;
    if (!cephx_build_service_ticket_blob(cct, info, blob)) {
      return false;
    }
    encode(blob, service_ticket_bl);

    ldout(cct, 30) << "service_ticket_blob is ";
    service_ticket_bl.hexdump(*_dout);
    *_dout << dendl;

    encode((__u8)should_encrypt_ticket, reply);
    if (should_encrypt_ticket) {
      if (encode_encrypt(cct, service_ticket_bl, ticket_enc_key, reply, error)) {
	ldout(cct, -1) << "error encoding encrypted ticket: " << error << dendl;
        return false;
      }
    } else {
      encode(service_ticket_bl, reply);
    }
  }
  return true;
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool CephXTicketHandler::verify_service_ticket_reply(
  CryptoKey& secret,
  bufferlist::const_iterator& indata)
{
  __u8 service_ticket_v;
  decode(service_ticket_v, indata);

  CephXServiceTicket msg_a;
  std::string error;
  if (decode_decrypt(cct, msg_a, secret, indata, error)) {
    ldout(cct, 0) << "verify_service_ticket_reply: failed decode_decrypt, error is: " << error << dendl;
    return false;
  }
  
  __u8 ticket_enc;
  decode(ticket_enc, indata);

  bufferlist service_ticket_bl;
  if (ticket_enc) {
    ldout(cct, 10) << " got encrypted ticket" << dendl;
    std::string error;
    if (decode_decrypt(cct, service_ticket_bl, session_key, indata, error)) {
      ldout(cct, 10) << "verify_service_ticket_reply: decode_decrypt failed "
	    << "with " << error << dendl;
      return false;
    }
  } else {
    decode(service_ticket_bl, indata);
  }
  auto iter = service_ticket_bl.cbegin();
  decode(ticket, iter);
  ldout(cct, 10) << " ticket.secret_id=" <<  ticket.secret_id << dendl;

  ldout(cct, 10) << "verify_service_ticket_reply service " << ceph_entity_type_name(service_id)
	   << " secret_id " << ticket.secret_id
	   << " session_key " << msg_a.session_key
           << " validity=" << msg_a.validity << dendl;
  session_key = msg_a.session_key;
  if (!msg_a.validity.is_zero()) {
    expires = ceph_clock_now();
    expires += msg_a.validity;
    renew_after = expires;
    renew_after -= ((double)msg_a.validity.sec() / 4);
    ldout(cct, 10) << "ticket expires=" << expires << " renew_after=" << renew_after << dendl;
  }
  
  have_key_flag = true;
  return true;
}

bool CephXTicketHandler::have_key()
{
  if (have_key_flag) {
    have_key_flag = ceph_clock_now() < expires;
  }

  return have_key_flag;
}

bool CephXTicketHandler::need_key() const
{
  if (have_key_flag) {
    return (!expires.is_zero()) && (ceph_clock_now() >= renew_after);
  }

  return true;
}

bool CephXTicketManager::have_key(uint32_t service_id)
{
  map<uint32_t, CephXTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end())
    return false;
  return iter->second.have_key();
}

bool CephXTicketManager::need_key(uint32_t service_id) const
{
  map<uint32_t, CephXTicketHandler>::const_iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end())
    return true;
  return iter->second.need_key();
}

void CephXTicketManager::set_have_need_key(uint32_t service_id, uint32_t& have, uint32_t& need)
{
  map<uint32_t, CephXTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end()) {
    have &= ~service_id;
    need |= service_id;
    ldout(cct, 10) << "set_have_need_key no handler for service "
		   << ceph_entity_type_name(service_id) << dendl;
    return;
  }

  //ldout(cct, 10) << "set_have_need_key service " << ceph_entity_type_name(service_id)
  //<< " (" << service_id << ")"
  //<< " need=" << iter->second.need_key() << " have=" << iter->second.have_key() << dendl;
  if (iter->second.need_key())
    need |= service_id;
  else
    need &= ~service_id;

  if (iter->second.have_key())
    have |= service_id;
  else
    have &= ~service_id;
}

void CephXTicketManager::invalidate_ticket(uint32_t service_id)
{
  map<uint32_t, CephXTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter != tickets_map.end())
    iter->second.invalidate_ticket();
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool CephXTicketManager::verify_service_ticket_reply(CryptoKey& secret,
						     bufferlist::const_iterator& indata)
{
  __u8 service_ticket_reply_v;
  decode(service_ticket_reply_v, indata);

  uint32_t num;
  decode(num, indata);
  ldout(cct, 10) << "verify_service_ticket_reply got " << num << " keys" << dendl;

  for (int i=0; i<(int)num; i++) {
    uint32_t type;
    decode(type, indata);
    ldout(cct, 10) << "got key for service_id " << ceph_entity_type_name(type) << dendl;
    CephXTicketHandler& handler = get_handler(type);
    if (!handler.verify_service_ticket_reply(secret, indata)) {
      return false;
    }
    handler.service_id = type;
  }

  return true;
}

/*
 * PRINCIPAL: build authorizer to access the service.
 *
 * ticket, {timestamp}^session_key
 */
CephXAuthorizer *CephXTicketHandler::build_authorizer(uint64_t global_id) const
{
  CephXAuthorizer *a = new CephXAuthorizer(cct);
  a->session_key = session_key;
  cct->random()->get_bytes((char*)&a->nonce, sizeof(a->nonce));

  __u8 authorizer_v = 1; // see AUTH_MODE_* in Auth.h
  encode(authorizer_v, a->bl);
  encode(global_id, a->bl);
  encode(service_id, a->bl);

  encode(ticket, a->bl);
  a->base_bl = a->bl;

  CephXAuthorize msg;
  msg.nonce = a->nonce;

  std::string error;
  if (encode_encrypt(cct, msg, session_key, a->bl, error)) {
    ldout(cct, 0) << "failed to encrypt authorizer: " << error << dendl;
    delete a;
    return 0;
  }
  return a;
}

/*
 * PRINCIPAL: build authorizer to access the service.
 *
 * ticket, {timestamp}^session_key
 */
CephXAuthorizer *CephXTicketManager::build_authorizer(uint32_t service_id) const
{
  map<uint32_t, CephXTicketHandler>::const_iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end()) {
    ldout(cct, 0) << "no TicketHandler for service "
		  << ceph_entity_type_name(service_id) << dendl;
    return NULL;
  }

  const CephXTicketHandler& handler = iter->second;
  return handler.build_authorizer(global_id);
}

void CephXTicketManager::validate_tickets(uint32_t mask, uint32_t& have, uint32_t& need)
{
  uint32_t i;
  need = 0;
  for (i = 1; i<=mask; i<<=1) {
    if (mask & i) {
      set_have_need_key(i, have, need);
    }
  }
  ldout(cct, 10) << "validate_tickets want " << mask << " have " << have
		 << " need " << need << dendl;
}

bool cephx_decode_ticket(CephContext *cct, KeyStore *keys, uint32_t service_id,
	      CephXTicketBlob& ticket_blob, CephXServiceTicketInfo& ticket_info)
{
  uint64_t secret_id = ticket_blob.secret_id;
  CryptoKey service_secret;

  if (!ticket_blob.blob.length()) {
    return false;
  }

  if (secret_id == (uint64_t)-1) {
    if (!keys->get_secret(cct->_conf->name, service_secret)) {
      ldout(cct, 0) << "ceph_decode_ticket could not get general service secret for service_id="
	      << ceph_entity_type_name(service_id) << " secret_id=" << secret_id << dendl;
      return false;
    }
  } else {
    if (!keys->get_service_secret(service_id, secret_id, service_secret)) {
      ldout(cct, 0) << "ceph_decode_ticket could not get service secret for service_id=" 
	      << ceph_entity_type_name(service_id) << " secret_id=" << secret_id << dendl;
      return false;
    }
  }

  std::string error;
  decode_decrypt_enc_bl(cct, ticket_info, service_secret, ticket_blob.blob, error);
  if (!error.empty()) {
    ldout(cct, 0) << "ceph_decode_ticket could not decrypt ticket info. error:" 
	<< error << dendl;
    return false;
  }

  return true;
}

/*
 * SERVICE: verify authorizer and generate reply authorizer
 *
 * {timestamp + 1}^session_key
 */
bool cephx_verify_authorizer(CephContext *cct, KeyStore *keys,
			     bufferlist::const_iterator& indata,
			     size_t connection_secret_required_len,
			     CephXServiceTicketInfo& ticket_info,
			     std::unique_ptr<AuthAuthorizerChallenge> *challenge,
			     std::string *connection_secret,
			     bufferlist *reply_bl)
{
  __u8 authorizer_v;
  uint32_t service_id;
  uint64_t global_id;
  CryptoKey service_secret;
  // ticket blob
  CephXTicketBlob ticket;

  try {
    decode(authorizer_v, indata);
    decode(global_id, indata);
    decode(service_id, indata);
    decode(ticket, indata);
  } catch (buffer::end_of_buffer &e) {
    // Unable to decode!
    return false;
  }
  ldout(cct, 10) << "verify_authorizer decrypted service "
	   << ceph_entity_type_name(service_id)
	   << " secret_id=" << ticket.secret_id << dendl;

  if (ticket.secret_id == (uint64_t)-1) {
    EntityName name;
    name.set_type(service_id);
    if (!keys->get_secret(name, service_secret)) {
      ldout(cct, 0) << "verify_authorizer could not get general service secret for service "
	      << ceph_entity_type_name(service_id) << " secret_id=" << ticket.secret_id << dendl;
      return false;
    }
  } else {
    if (!keys->get_service_secret(service_id, ticket.secret_id, service_secret)) {
      ldout(cct, 0) << "verify_authorizer could not get service secret for service "
	      << ceph_entity_type_name(service_id) << " secret_id=" << ticket.secret_id << dendl;
      if (cct->_conf->auth_debug && ticket.secret_id == 0)
	ceph_abort_msg("got secret_id=0");
      return false;
    }
  }
  std::string error;
  if (!service_secret.get_secret().length())
    error = "invalid key";  // Bad key?
  else
    decode_decrypt_enc_bl(cct, ticket_info, service_secret, ticket.blob, error);
  if (!error.empty()) {
    ldout(cct, 0) << "verify_authorizer could not decrypt ticket info: error: "
      << error << dendl;
    return false;
  }

  if (ticket_info.ticket.global_id != global_id) {
    ldout(cct, 0) << "verify_authorizer global_id mismatch: declared id=" << global_id
	    << " ticket_id=" << ticket_info.ticket.global_id << dendl;
    return false;
  }

  ldout(cct, 10) << "verify_authorizer global_id=" << global_id << dendl;

  // CephXAuthorize
  CephXAuthorize auth_msg;
  if (decode_decrypt(cct, auth_msg, ticket_info.session_key, indata, error)) {
    ldout(cct, 0) << "verify_authorizercould not decrypt authorize request with error: "
      << error << dendl;
    return false;
  }

  if (challenge) {
    auto *c = static_cast<CephXAuthorizeChallenge*>(challenge->get());
    if (!auth_msg.have_challenge || !c) {
      c = new CephXAuthorizeChallenge;
      challenge->reset(c);
      cct->random()->get_bytes((char*)&c->server_challenge, sizeof(c->server_challenge));
      ldout(cct,10) << __func__ << " adding server_challenge " << c->server_challenge
		    << dendl;

      encode_encrypt_enc_bl(cct, *c, ticket_info.session_key, *reply_bl, error);
      if (!error.empty()) {
	ldout(cct, 10) << "verify_authorizer: encode_encrypt error: " << error << dendl;
	return false;
      }
      return false;
    }
    ldout(cct, 10) << __func__ << " got server_challenge+1 "
		   << auth_msg.server_challenge_plus_one
		   << " expecting " << c->server_challenge + 1 << dendl;
    if (c->server_challenge + 1 != auth_msg.server_challenge_plus_one) {
      return false;
    }
  }

  /*
   * Reply authorizer:
   *  {timestamp + 1}^session_key
   */
  CephXAuthorizeReply reply;
  // reply.trans_id = auth_msg.trans_id;
  reply.nonce_plus_one = auth_msg.nonce + 1;
#ifndef WITH_SEASTAR
  if (connection_secret) {
    // generate a connection secret
    connection_secret->resize(connection_secret_required_len);
    if (connection_secret_required_len) {
      cct->random()->get_bytes(connection_secret->data(),
			       connection_secret_required_len);
    }
    reply.connection_secret = *connection_secret;
  }
#endif
  if (encode_encrypt(cct, reply, ticket_info.session_key, *reply_bl, error)) {
    ldout(cct, 10) << "verify_authorizer: encode_encrypt error: " << error << dendl;
    return false;
  }

  ldout(cct, 10) << "verify_authorizer ok nonce " << hex << auth_msg.nonce << dec
	   << " reply_bl.length()=" << reply_bl->length() <<  dendl;
  return true;
}

bool CephXAuthorizer::verify_reply(bufferlist::const_iterator& indata,
				   std::string *connection_secret)
{
  CephXAuthorizeReply reply;

  std::string error;
  if (decode_decrypt(cct, reply, session_key, indata, error)) {
      ldout(cct, 0) << "verify_reply couldn't decrypt with error: " << error << dendl;
      return false;
  }

  uint64_t expect = nonce + 1;
  if (expect != reply.nonce_plus_one) {
    ldout(cct, 0) << "verify_authorizer_reply bad nonce got " << reply.nonce_plus_one << " expected " << expect
	    << " sent " << nonce << dendl;
    return false;
  }

  if (connection_secret &&
      reply.connection_secret.size()) {
    *connection_secret = reply.connection_secret;
  }
  return true;
}

bool CephXAuthorizer::add_challenge(CephContext *cct,
				    const bufferlist& challenge)
{
  bl = base_bl;

  CephXAuthorize msg;
  msg.nonce = nonce;

  auto p = challenge.begin();
  if (!p.end()) {
    std::string error;
    CephXAuthorizeChallenge ch;
    decode_decrypt_enc_bl(cct, ch, session_key, challenge, error);
    if (!error.empty()) {
      ldout(cct, 0) << "failed to decrypt challenge (" << challenge.length() << " bytes): "
		    << error << dendl;
      return false;
    }
    msg.have_challenge = true;
    msg.server_challenge_plus_one = ch.server_challenge + 1;
  }

  std::string error;
  if (encode_encrypt(cct, msg, session_key, bl, error)) {
    ldout(cct, 0) << __func__ << " failed to encrypt authorizer: " << error << dendl;
    return false;
  }
  return true;
}
