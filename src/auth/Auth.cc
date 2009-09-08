
#include "Auth.h"
#include "common/Clock.h"

#include "config.h"


/*
 * Authentication
 */

/*
 * PRINCIPAL: request authentication
 *
 * principal_name, principal_addr.  "please authenticate me."
 */
void build_authenticate_request(EntityName& principal_name, entity_addr_t& principal_addr,
                                uint32_t keys,
                                bool encrypt,
                                CryptoKey& session_key,
                                AuthBlob& ticket_info,
				bufferlist& request)
{
  AuthServiceTicketRequest ticket_req;

  ticket_req.addr =  principal_addr;
  ticket_req.timestamp = g_clock.now();
  ticket_req.keys = keys;
  if (!encrypt) {
    ::encode(ticket_req, request);
  } else {
    ticket_req.encode_encrypt(session_key, request);
  }
  ::encode(ticket_info, request);
}

/*
 * AUTH SERVER: authenticate
 *
 * Authenticate principal, respond with AuthServiceTicketInfo
 *
 * {session key, validity, nonce}^principal_secret
 * {principal_ticket, session key}^service_secret  ... "enc_ticket"
 */
bool build_authenticate_reply(AuthTicket& ticket,
                     CryptoKey& session_key,
                     CryptoKey& principal_secret,
                     CryptoKey& service_secret,
                     bufferlist& reply)
{
  AuthServiceTicket msg_a;

  msg_a.session_key = session_key;
  if (msg_a.encode_encrypt(principal_secret, reply) < 0)
    return false;

  AuthServiceTicketInfo ticket_info;
  ticket_info.session_key = session_key;
  ticket_info.ticket = ticket;
  if (ticket_info.encode_encrypt(service_secret, reply) < 0)
    return false;
  return true;
}

bool verify_service_ticket_request(bool encrypted,
                                   CryptoKey& service_secret,
                                   CryptoKey& session_key,
                                   uint32_t& keys,
                                   bufferlist::iterator& indata)
{
  AuthServiceTicketRequest msg;

  if (encrypted) {
    dout(0) << "verify encrypted service ticket request" << dendl;
    if (msg.decode_decrypt(session_key, indata) < 0)
      return false;

    dout(0) << "decoded timestamp=" << msg.timestamp << " addr=" << msg.addr << " (was encrypted)" << dendl;

    AuthServiceTicketInfo ticket_info;
    if (ticket_info.decode_decrypt(service_secret, indata) < 0)
      return false;
  } else {
    ::decode(msg, indata);

    dout(0) << "decoded timestamp=" << msg.timestamp << " addr=" << msg.addr << dendl;
  }

  /* FIXME: validate that request makes sense */

  keys = msg.keys;
  dout(0) << "requested keys=" << keys << dendl;

  return true;
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool AuthTicketHandler::verify_service_ticket_reply(CryptoKey& secret,
					      bufferlist::iterator& indata)
{
  AuthServiceTicket msg_a;
  if (msg_a.decode_decrypt(secret, indata) < 0)
    return false;

  ::decode(ticket, indata);

  if (!indata.end())
    return false;

  has_key_flag = true;

  return true;
}

/*
 * PRINCIPAL: build authenticator to access the service.
 *
 * ticket, {timestamp, nonce}^session_key
 */
utime_t AuthTicketHandler::build_authenticator(bufferlist& bl)
{
  utime_t now = g_clock.now();

  ::encode(ticket, bl);

  AuthAuthenticate msg;
  msg.now = now;
  msg.nonce = nonce;
  msg.encode_encrypt(session_key, bl);

  return now;
}

/*
 * SERVICE: verify authenticator and generate reply authenticator
 *
 * {timestamp + 1}^session_key
 */
bool verify_authenticator(CryptoKey& service_secret, bufferlist::iterator& indata,
			  bufferlist& reply_bl)
{
  AuthTicket ticket;
  ticket.decode_decrypt(service_secret, indata);

  AuthAuthenticate auth_msg;
  auth_msg.decode_decrypt(ticket.session_key, indata);

  bufferlist enc_ticket, enc_info;
  ::decode(enc_ticket, indata);
  ::decode(enc_info, indata);

  // it's authentic if the nonces match
  if (auth_msg.nonce != ticket.nonce)
    return false;
  dout(0) << "verify_authenticator: nonce ok" << dendl;
  
  /*
   * Reply authenticator:
   *  {timestamp + 1}^session_key
   */
  AuthAuthenticateReply reply;
  reply.timestamp = auth_msg.now;
  reply.timestamp += 1;
  reply.encode_encrypt(ticket.session_key, reply_bl);

  dout(0) << "verify_authenticator: ok" << dendl;

  return true;
}

/*
 * PRINCIPAL: verify reply is authentic
 */
bool AuthTicketHandler::verify_reply_authenticator(utime_t then, bufferlist& enc_reply)
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



