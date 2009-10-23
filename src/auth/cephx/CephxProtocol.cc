
#include "CephxProtocol.h"
#include "common/Clock.h"

#include "config.h"




/*
 * Authentication
 */

bool cephx_build_service_ticket(SessionAuthInfo& info, bufferlist& reply)
{
  CephXServiceTicketInfo ticket_info;
  ticket_info.session_key = info.session_key;
  ticket_info.ticket = info.ticket;
  ticket_info.ticket.caps = info.ticket.caps;
  ::encode(info.secret_id, reply);
  dout(0) << "encoded info.secret_id=" << info.secret_id <<  " ticket_info.ticket.name=" << ticket_info.ticket.name.to_str() << dendl;
  if (info.service_secret.get_secret().length())
    hexdump("service_secret", info.service_secret.get_secret().c_str(), info.service_secret.get_secret().length());
  if (encode_encrypt(ticket_info, info.service_secret, reply) < 0)
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
                     vector<SessionAuthInfo> ticket_info_vec,
                     bufferlist& reply)
{
  vector<SessionAuthInfo>::iterator ticket_iter = ticket_info_vec.begin(); 

  uint32_t num = ticket_info_vec.size();
  ::encode(num, reply);
  dout(0) << "encoding " << num << " tickets with secret " << principal_secret << dendl;

  while (ticket_iter != ticket_info_vec.end()) {
    SessionAuthInfo& info = *ticket_iter;

    ::encode(info.service_id, reply);

    CephXServiceTicket msg_a;

    bufferptr& s1 = principal_secret.get_secret();
    if (s1.length()) {
      hexdump("encoding, using key", s1.c_str(), s1.length());
    }

    msg_a.session_key = info.session_key;
    if (encode_encrypt(msg_a, principal_secret, reply) < 0)
      return false;

    if (!cephx_build_service_ticket(info, reply))
      return false; 

    ++ticket_iter;
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
  CephXServiceTicket msg_a;

  if (decode_decrypt(msg_a, secret, indata) < 0) {
    dout(0) << "failed service ticket reply decode with secret " << secret << dendl;
    return false;
  }
  ::decode(ticket, indata);
  dout(0) << "verify_service_ticket_reply service " << ceph_entity_type_name(service_id)
	  << " secret_id " << ticket.secret_id
	  << " session_key " << msg_a.session_key << dendl;  
  session_key = msg_a.session_key;
  has_key_flag = true;
  return true;
}

bool CephXTicketManager::has_key(uint32_t service_id)
{
  map<uint32_t, CephXTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end())
    return false;
  return iter->second.has_key();
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool CephXTicketManager::verify_service_ticket_reply(CryptoKey& secret,
						     bufferlist::iterator& indata)
{
  uint32_t num;
  ::decode(num, indata);
  dout(0) << "verify_service_ticket_reply got " << num << " keys" << dendl;

  for (int i=0; i<(int)num; i++) {
    uint32_t type;
    ::decode(type, indata);
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
CephXAuthorizer *CephXTicketHandler::build_authorizer()
{
  CephXAuthorizer *a = new CephXAuthorizer;
  a->session_key = session_key;
  a->timestamp = g_clock.now();

  dout(0) << "build_authorizer: service_id=" << service_id << dendl;

  ::encode(service_id, a->bl);
  ::encode(ticket, a->bl);

  CephXAuthorize msg;
  msg.now = a->timestamp;
  if (encode_encrypt(msg, session_key, a->bl) < 0) {
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
  if (iter == tickets_map.end())
    return false;

  CephXTicketHandler& handler = iter->second;
  return handler.build_authorizer();
}

/*
 * SERVICE: verify authorizer and generate reply authorizer
 *
 * {timestamp + 1}^session_key
 */
bool cephx_verify_authorizer(KeyStore& keys, bufferlist::iterator& indata,
                       CephXServiceTicketInfo& ticket_info, bufferlist& reply_bl)
{
  uint32_t service_id;
  uint64_t secret_id;
  CryptoKey service_secret;

  ::decode(service_id, indata);

  ::decode(secret_id, indata);
  dout(0) << "decrypted service_id=" << service_id << " secret_id=" << secret_id << dendl;
  if (secret_id == (uint64_t)-1) {
    EntityName name;
    name.entity_type = service_id;
    if (!keys.get_secret(name, service_secret)) {
      dout(0) << "could not get general service secret for service_id=" << service_id << " secret_id=" << secret_id << dendl;
      return false;
    }
  } else {
    if (!keys.get_service_secret(service_id, secret_id, service_secret)) {
      dout(0) << "could not get service secret for service_id=" << service_id << " secret_id=" << secret_id << dendl;
      return false;
    }
  }
  if (service_secret.get_secret().length())
    hexdump("service_secret", service_secret.get_secret().c_str(), service_secret.get_secret().length());
  if (decode_decrypt(ticket_info, service_secret, indata) < 0) {
    dout(0) << "could not decrypt ticket info" << dendl;
    return false;
  }
  dout(0) << "decoded ticket_info.ticket.name=" << ticket_info.ticket.name.to_str() << dendl;

  CephXAuthorize auth_msg;
  if (decode_decrypt(auth_msg, ticket_info.session_key, indata) < 0) {
    dout(0) << "could not decrypt authorize request" << dendl;
    return false;
  }

  /*
   * Reply authorizer:
   *  {timestamp + 1}^session_key
   */
  CephXAuthorizeReply reply;
  // reply.trans_id = auth_msg.trans_id;
  reply.timestamp = auth_msg.now;
  reply.timestamp.sec_ref() += 1;
  if (encode_encrypt(reply, ticket_info.session_key, reply_bl) < 0)
    return false;

  dout(0) << "verify_authorizer: ok reply_bl.length()=" << reply_bl.length() <<  dendl;

  return true;
}

bool CephXAuthorizer::verify_reply(bufferlist::iterator& indata)
{
  CephXAuthorizeReply reply;

  if (decode_decrypt(reply, session_key, indata) < 0) {
    dout(0) << " coudln't decrypt auth reply" << dendl;
    return false;
  }

  utime_t expect = timestamp;
  expect.sec_ref() += 1;
  if (expect != reply.timestamp) {
    dout(0) << " bad ts got " << reply.timestamp << " expect " << expect << " sent " << timestamp << dendl;
    return false;
  }

  return true;
}

