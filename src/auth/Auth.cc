
#include "Auth.h"
#include "common/Clock.h"

#include "config.h"


/*
 * Authentication
 */

static void encode_tgt(PrincipalTicket& ticket, CryptoKey& key, bufferlist& bl)
{
  ::encode(ticket, bl);
  ::encode(key, bl);
}

static void decode_tgt(PrincipalTicket& ticket, CryptoKey& key, bufferlist& bl)
{
  bufferlist::iterator iter = bl.begin();
  ::decode(ticket, iter);
  ::decode(key, iter);
}


/*
 * PRINCIPAL: request authentication
 *
 * principal_name, principal_addr.  "please authenticate me."
 */
void build_get_tgt_request(EntityName& principal_name, entity_addr_t principal_addr,
				bufferlist& request)
{
  ::encode(principal_name, request);
  ::encode(principal_addr, request);
}

/*
 * AUTH SERVER: authenticate
 *
 * Authenticate principal, respond with TGT
 *
 * {session key, validity, nonce}^principal_secret
 * {principal_ticket, session key}^service_secret  ... "enc_ticket"
 */
bool build_get_tgt_reply(PrincipalTicket& principal_ticket,
                     CryptoKey& session_key,
                     CryptoKey& principal_secret,
                     CryptoKey& service_secret,
                     bufferlist& reply)
{
  AuthMsg_A msg_a;

  msg_a.session_key = session_key;
  if (msg_a.encode_encrypt(principal_secret, reply) < 0)
    return false;

  TGT tgt;
  tgt.session_key = session_key;
  tgt.ticket = principal_ticket;
  if (tgt.encode_encrypt(service_secret, reply) < 0)
    return false;
  return true;
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool ServiceTicket::verify_service_ticket_reply(CryptoKey& principal_secret,
					      bufferlist::iterator& indata)
{
  dout(0) << "1" << dendl;

  AuthMsg_A msg_a;
  if (msg_a.decode_decrypt(principal_secret, indata) < 0)
    return false;

  ::decode(enc_ticket, indata);

  if (!indata.end())
    return false;

  has_key_flag = true;

  return true;
}

/*
 * PRINCIPAL: build request to retrieve a service ticket
 *
 * TGT, D = {principal_addr, timestamp}^principal/auth session key
 */
bool ServiceTicket::get_session_keys(uint32_t keys, entity_addr_t& principal_addr, bufferlist& bl)
{
  AuthMsg_D msg;
  msg.timestamp = g_clock.now();
  msg.principal_addr = principal_addr;

  if (msg.encode_encrypt(session_key, bl) < 0)
    return false;

  ::encode(enc_ticket, bl);

  return true;
}

bool verify_get_session_keys_request(CryptoKey& service_secret,
                                     CryptoKey& session_key, uint32_t& keys, bufferlist::iterator& indata)
{
  AuthMsg_D msg;
  if (msg.decode_decrypt(session_key, indata) < 0)
    return false;

  dout(0) << "decoded now=" << msg.timestamp << " addr=" << msg.principal_addr << dendl;

  TGT tgt;
  if (tgt.decode_decrypt(service_secret, indata) < 0)
    return false;

  /* FIXME: validate that request makes sense */

  return true;
}
#if 0
bool build_get_tgt_reply(PrincipalTicket& principal_ticket, CryptoKey& principal_secret,
			      CryptoKey& session_key, CryptoKey& service_secret,
			      bufferlist& reply)
{
  bufferlist info, enc_info;
  ::encode(session_key, info);
  ::encode(principal_ticket.renew_after, info);
  ::encode(principal_ticket.expires, info);
  ::encode(principal_ticket.nonce, info);
  dout(0) << "encoded expires=" << principal_ticket.expires << dendl;
  if (principal_secret.encrypt(info, enc_info) < 0) {
    dout(0) << "error encrypting principal ticket" << dendl;
    return false;
  }
  ::encode(enc_info, reply);

  /*
     Build TGT
   */
  bufferlist ticket, tgt;
  encode_tgt(principal_ticket, session_key, ticket);

  if (service_secret.encrypt(ticket, tgt) < 0) {
    dout(0) << "error ecryptng result" << dendl;
    return false;
  }
  ::encode(tgt, reply);  

  dout(0) << "enc_info.length()=" << enc_info.length() << dendl;
  dout(0) << "tgt.length()=" << tgt.length() << dendl;

  return true;
}
#endif
/*
 * AUTH SERVER: build ticket for service reply
 *
 * a->p : E= {service ticket}^svcsecret
 *        F= {principal/service session key, validity}^principal/auth session key
 *
 */
bool build_ticket_reply(ServiceTicket service_ticket,
                        CryptoKey session_key,
                        CryptoKey auth_session_key,
                        CryptoKey& service_secret,
			bufferlist& reply)
{
  AuthMsg_E e;

  e.ticket = service_ticket;
  if (e.encode_encrypt(service_secret, reply) < 0)
    return false;


   AuthMsg_F f;
   f.session_key = session_key;
   f.validity = 0; /* FIXME */
   if (f.encode_encrypt(auth_session_key, reply) < 0)
     return false;
  
  return true;
}

/*
 * AUTH SERVER: verify a request to retrieve a service ticket, build response
 *
 * TGT, {principal_addr, timestamp}^principal/auth session key
 */
bool build_get_session_keys_response(ServiceTicket& ticket, CryptoKey& service_secret,
                                     bufferlist::iterator& indata, bufferlist& out)
{
  /* FIXME: verify session key */

  return true;
}

/*
 * PRINCIPAL: build authenticator to access the service.
 *
 * enc_ticket, {timestamp, nonce}^session_key
 */
utime_t ServiceTicket::build_authenticator(bufferlist& bl)
{
  utime_t now = g_clock.now();
  
  ::encode(enc_ticket, bl);
  
  bufferlist info, enc_info;
  ::encode(now, info);
  ::encode(nonce, info);
  session_key.encrypt(info, enc_info);
  ::encode(enc_info, bl);
  return now;
}

/*
 * SERVICE: verify authenticator and generate reply authenticator
 *
 * {timestamp + 1}^session_key
 */
bool verify_authenticator(CryptoKey& service_secret, bufferlist::iterator& indata,
			  bufferlist& enc_reply)
{
  bufferlist enc_ticket, enc_info;
  ::decode(enc_ticket, indata);
  ::decode(enc_info, indata);

  // decrypt ticket
  PrincipalTicket ticket;
  CryptoKey session_key;
  {
    bufferlist bl;
    if (service_secret.decrypt(enc_ticket, bl) < 0)
      return false;
    dout(0) << "verify_authenticator: decrypted ticket" << dendl;
    bufferlist::iterator p = bl.begin();
    ::decode(ticket, p);
    ::decode(session_key, p);
  }
  
  // decrypt info with session key
  utime_t timestamp;
  string nonce;
  {
    bufferlist info;
    if (session_key.decrypt(enc_info, info) < 0)
      return false;
    dout(0) << "verify_authenticator: decrypted session key" << dendl;
    bufferlist::iterator p = info.begin();
    ::decode(timestamp, p);
    ::decode(nonce, p);
  }

  // it's authentic if the nonces match
  if (nonce != ticket.nonce)
    return false;
  dout(0) << "verify_authenticator: nonce ok" << dendl;
  
  /*
   * Reply authenticator:
   *  {timestamp + 1}^session_key
   */
  bufferlist reply;
  timestamp += 1;
  ::encode(timestamp, reply);
  if (session_key.encrypt(reply, enc_reply) < 0)
    return false;

  dout(0) << "verify_authenticator: ok" << dendl;

  return true;
}

/*
 * PRINCIPAL: verify reply is authentic
 */
bool ServiceTicket::verify_reply_authenticator(utime_t then, bufferlist& enc_reply)
{
  bufferlist reply;
  if (session_key.decrypt(enc_reply, reply) < 0)
    return false;
  
  bufferlist::iterator p = reply.begin();
  utime_t later;
  ::decode(later, p);
  dout(0) << "later=" << later << " then=" << then << dendl;
  if (then + 1 == later) {
    return true;
  }

  return false;
}



