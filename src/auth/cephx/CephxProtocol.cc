
#include "CephxProtocol.h"
#include "common/Clock.h"

#include "common/config.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx: "



int cephx_calc_client_server_challenge(CryptoKey& secret, uint64_t server_challenge, uint64_t client_challenge, uint64_t *key)
{
  CephXChallengeBlob b;
  b.server_challenge = server_challenge;
  b.client_challenge = client_challenge;

  bufferlist enc;
  int ret = encode_encrypt(b, secret, enc);
  if (ret < 0)
    return ret;

  uint64_t k = 0;
  const uint64_t *p = (const uint64_t *)enc.c_str();
  for (int pos = 0; pos + sizeof(k) <= enc.length(); pos+=sizeof(k), p++)
    k ^= *p;
  *key = k;
  return 0;
}


/*
 * Authentication
 */

bool cephx_build_service_ticket_blob(CephXSessionAuthInfo& info, CephXTicketBlob& blob)
{
  CephXServiceTicketInfo ticket_info;
  ticket_info.session_key = info.session_key;
  ticket_info.ticket = info.ticket;
  ticket_info.ticket.caps = info.ticket.caps;

  dout(10) << "build_service_ticket service " << ceph_entity_type_name(info.service_id)
	   << " secret_id " << info.secret_id
	   << " ticket_info.ticket.name=" << ticket_info.ticket.name.to_str() << dendl;
  blob.secret_id = info.secret_id;
  if (encode_encrypt_enc_bl(ticket_info, info.service_secret, blob.blob) < 0)
    return false;
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
bool cephx_build_service_ticket_reply(
                     CryptoKey& principal_secret,
                     vector<CephXSessionAuthInfo> ticket_info_vec,
                     bool should_encrypt_ticket,
                     CryptoKey& ticket_enc_key,
                     bufferlist& reply)
{
  __u8 service_ticket_reply_v = 1;
  ::encode(service_ticket_reply_v, reply);

  uint32_t num = ticket_info_vec.size();
  ::encode(num, reply);
  dout(10) << "build_service_ticket_reply encoding " << num
	   << " tickets with secret " << principal_secret << dendl;

  for (vector<CephXSessionAuthInfo>::iterator ticket_iter = ticket_info_vec.begin(); 
       ticket_iter != ticket_info_vec.end();
       ++ticket_iter) {
    CephXSessionAuthInfo& info = *ticket_iter;
    ::encode(info.service_id, reply);

    __u8 service_ticket_v = 1;
    ::encode(service_ticket_v, reply);

    CephXServiceTicket msg_a;
    msg_a.session_key = info.session_key;
    msg_a.validity = info.validity;
    if (encode_encrypt(msg_a, principal_secret, reply) < 0)
      return false;

    bufferlist service_ticket_bl;
    CephXTicketBlob blob;
    if (!cephx_build_service_ticket_blob(info, blob))
      return false;
    ::encode(blob, service_ticket_bl);

    dout(20) << "service_ticket_blob is ";
    service_ticket_bl.hexdump(*_dout);
    *_dout << dendl;

    ::encode((__u8)should_encrypt_ticket, reply);
    if (should_encrypt_ticket) {
      if (encode_encrypt(service_ticket_bl, ticket_enc_key, reply) < 0)
        return false;
    } else {
      ::encode(service_ticket_bl, reply);
    }
  }
  return true;
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool CephXTicketHandler::verify_service_ticket_reply(CryptoKey& secret,
						     bufferlist::iterator& indata)
{
  __u8 service_ticket_v;
  ::decode(service_ticket_v, indata);

  CephXServiceTicket msg_a;
  if (decode_decrypt(msg_a, secret, indata) < 0) {
    dout(0) << "verify_service_ticket_reply failed decode_decrypt with secret " << secret << dendl;
    return false;
  }
  
  __u8 ticket_enc;
  ::decode(ticket_enc, indata);

  bufferlist service_ticket_bl;
  if (ticket_enc) {
    dout(10) << " got encrypted ticket" << dendl;
    if (decode_decrypt(service_ticket_bl, session_key, indata) < 0)
      return false;
  } else {
    ::decode(service_ticket_bl, indata);
  }
  bufferlist::iterator iter = service_ticket_bl.begin();
  ::decode(ticket, iter);
  dout(10) << " ticket.secret_id=" <<  ticket.secret_id << dendl;

  dout(10) << "verify_service_ticket_reply service " << ceph_entity_type_name(service_id)
	   << " secret_id " << ticket.secret_id
	   << " session_key " << msg_a.session_key
           << " validity=" << msg_a.validity << dendl;
  session_key = msg_a.session_key;
  if (!msg_a.validity.is_zero()) {
    expires = g_clock.now();
    expires += msg_a.validity;
    renew_after = expires;
    renew_after -= ((double)msg_a.validity.sec() / 4);
    dout(10) << "ticket expires=" << expires << " renew_after=" << renew_after << dendl;
  }
  
  have_key_flag = true;
  return true;
}

bool CephXTicketHandler::have_key()
{
  if (have_key_flag) {
    //dout(20) << "have_key: g_clock.now()=" << g_clock.now() << " renew_after=" << renew_after << " expires=" << expires << dendl;
    have_key_flag = g_clock.now() < expires;
  }

  return have_key_flag;
}

bool CephXTicketHandler::need_key()
{
  if (have_key_flag) {
    //dout(20) << "need_key: g_clock.now()=" << g_clock.now() << " renew_after=" << renew_after << " expires=" << expires << dendl;
    return (!expires.is_zero()) && (g_clock.now() >= renew_after);
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

bool CephXTicketManager::need_key(uint32_t service_id)
{
  map<uint32_t, CephXTicketHandler>::iterator iter = tickets_map.find(service_id);
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
    dout(10) << "set_have_need_key no handler for service " << ceph_entity_type_name(service_id) << dendl;
    return;
  }

  //dout(10) << "set_have_need_key service " << ceph_entity_type_name(service_id) << " (" << service_id << ")"
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
						     bufferlist::iterator& indata)
{
  __u8 service_ticket_reply_v;
  ::decode(service_ticket_reply_v, indata);

  uint32_t num;
  ::decode(num, indata);
  dout(10) << "verify_service_ticket_reply got " << num << " keys" << dendl;

  for (int i=0; i<(int)num; i++) {
    uint32_t type;
    ::decode(type, indata);
    dout(10) << "got key for service_id " << ceph_entity_type_name(type) << dendl;
    CephXTicketHandler& handler = tickets_map[type];
    handler.service_id = type;
    if (!handler.verify_service_ticket_reply(secret, indata)) {
      return false;
    }
    handler.service_id = type;
  }

  if (!indata.end())
    return false;

  return true;
}

/*
 * PRINCIPAL: build authorizer to access the service.
 *
 * ticket, {timestamp}^session_key
 */
CephXAuthorizer *CephXTicketHandler::build_authorizer(uint64_t global_id)
{
  CephXAuthorizer *a = new CephXAuthorizer;
  a->session_key = session_key;
  a->nonce = ((uint64_t)rand() << 32) + rand();

  __u8 authorizer_v = 1;
  ::encode(authorizer_v, a->bl);

  ::encode(global_id, a->bl);
  ::encode(service_id, a->bl);

  ::encode(ticket, a->bl);

  CephXAuthorize msg;
  msg.nonce = a->nonce;

  if (encode_encrypt(msg, session_key, a->bl) < 0) {
    dout(0) << "failed to encrypt authorizer" << dendl;
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
CephXAuthorizer *CephXTicketManager::build_authorizer(uint32_t service_id)
{
  map<uint32_t, CephXTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end()) {
    dout(0) << "no TicketHandler for service " << ceph_entity_type_name(service_id) << dendl;
    return NULL;
  }

  CephXTicketHandler& handler = iter->second;
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
  dout(10) << "validate_tickets want " << mask << " have " << have << " need " << need << dendl;
}

bool cephx_decode_ticket(KeyStore *keys, uint32_t service_id, CephXTicketBlob& ticket_blob, CephXServiceTicketInfo& ticket_info)
{
  uint64_t secret_id = ticket_blob.secret_id;
  CryptoKey service_secret;

  if (!ticket_blob.blob.length()) {
    return false;
  }

  if (secret_id == (uint64_t)-1) {
    if (!keys->get_secret(*g_conf.entity_name, service_secret)) {
      dout(0) << "ceph_decode_ticket could not get general service secret for service_id="
	      << ceph_entity_type_name(service_id) << " secret_id=" << secret_id << dendl;
      return false;
    }
  } else {
    if (!keys->get_service_secret(service_id, secret_id, service_secret)) {
      dout(0) << "ceph_decode_ticket could not get service secret for service_id=" 
	      << ceph_entity_type_name(service_id) << " secret_id=" << secret_id << dendl;
      return false;
    }
  }

  if (decode_decrypt_enc_bl(ticket_info, service_secret, ticket_blob.blob) < 0) {
    dout(0) << "ceph_decode_ticket could not decrypt ticket info" << dendl;
    return false;
  }

  return true;
}

/*
 * SERVICE: verify authorizer and generate reply authorizer
 *
 * {timestamp + 1}^session_key
 */
bool cephx_verify_authorizer(KeyStore *keys,
			     bufferlist::iterator& indata,
			     CephXServiceTicketInfo& ticket_info, bufferlist& reply_bl)
{
  __u8 authorizer_v;
  ::decode(authorizer_v, indata);

  uint32_t service_id;
  uint64_t global_id;
  CryptoKey service_secret;

  ::decode(global_id, indata);
  ::decode(service_id, indata);

  // ticket blob
  CephXTicketBlob ticket;
  ::decode(ticket, indata);
  dout(10) << "verify_authorizer decrypted service " << ceph_entity_type_name(service_id)
	   << " secret_id=" << ticket.secret_id << dendl;

  if (ticket.secret_id == (uint64_t)-1) {
    EntityName name;
    name.entity_type = service_id;
    if (!keys->get_secret(name, service_secret)) {
      dout(0) << "verify_authorizer could not get general service secret for service "
	      << ceph_entity_type_name(service_id) << " secret_id=" << ticket.secret_id << dendl;
      return false;
    }
  } else {
    if (!keys->get_service_secret(service_id, ticket.secret_id, service_secret)) {
      dout(0) << "verify_authorizer could not get service secret for service "
	      << ceph_entity_type_name(service_id) << " secret_id=" << ticket.secret_id << dendl;
      return false;
    }
  }
  if (decode_decrypt_enc_bl(ticket_info, service_secret, ticket.blob) < 0) {
    dout(0) << "verify_authorizer could not decrypt ticket info" << dendl;
    return false;
  }

  if (ticket_info.ticket.global_id != global_id) {
    dout(0) << "verify_authorizer global_id mismatch: declared id=" << global_id
	    << " ticket_id=" << ticket_info.ticket.global_id << dendl;
    return false;
  }

  dout(10) << "verify_authorizer global_id=" << global_id << dendl;

  // CephXAuthorize
  CephXAuthorize auth_msg;
  if (decode_decrypt(auth_msg, ticket_info.session_key, indata) < 0) {
    dout(0) << "verify_authorizercould not decrypt authorize request" << dendl;
    return false;
  }

  /*
   * Reply authorizer:
   *  {timestamp + 1}^session_key
   */
  CephXAuthorizeReply reply;
  // reply.trans_id = auth_msg.trans_id;
  reply.nonce_plus_one = auth_msg.nonce + 1;
  if (encode_encrypt(reply, ticket_info.session_key, reply_bl) < 0)
    return false;

  dout(10) << "verify_authorizer ok nonce " << hex << auth_msg.nonce << dec
	   << " reply_bl.length()=" << reply_bl.length() <<  dendl;
  return true;
}

bool CephXAuthorizer::verify_reply(bufferlist::iterator& indata)
{
  CephXAuthorizeReply reply;

  try {
    if (decode_decrypt(reply, session_key, indata) < 0) {
      dout(0) << "verify_authorizer_reply coudln't decrypt with " << session_key << dendl;
      return false;
    }
  } catch (const buffer::error &e) {
    dout(0) << "verify_authorizer_reply exception in decode_decrypt with " << session_key << dendl;
    return false;
  }

  uint64_t expect = nonce + 1;
  if (expect != reply.nonce_plus_one) {
    dout(0) << "verify_authorizer_reply bad nonce got " << reply.nonce_plus_one << " expected " << expect
	    << " sent " << nonce << dendl;
    return false;
  }
  return true;
}

