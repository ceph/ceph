
#include "Auth.h"
#include "common/Clock.h"

/*
 * Authentication
 */

/*
 * client_name, client_addr.  "please authenticate me."
 */
void build_authenticate_request(EntityName& client_name, entity_addr_t client_addr,
				bufferlist& request)
{
  ::encode(client_name, request);
  ::encode(client_addr, request);
}

/*
 * {session key, validity, nonce}^client_secret
 * {client_ticket, session key}^service_secret  ... "enc_ticket"
 */
void build_authenticate_reply(ClientTicket& client_ticket, CryptoKey& client_secret,
			      CryptoKey& session_key, CryptoKey& service_secret,
			      bufferlist& reply)
{
  bufferlist info, enc_info;
  ::encode(session_key, info);
  ::encode(client_ticket.renew_after, info);
  ::encode(client_ticket.expires, info);
  ::encode(client_ticket.nonce, info);
  client_secret.encrypt(info, enc_info);
  ::encode(enc_info, reply);

  bufferlist ticket, enc_ticket;
  ::encode(client_ticket, ticket);
  ::encode(session_key, ticket);
  service_secret.encrypt(ticket, enc_ticket);
  ::encode(enc_ticket, reply);  
}

/*
 * verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool ServiceTicket::verify_authenticate_reply(CryptoKey& client_secret,
					      bufferlist& reply)
{
  bufferlist enc_info, info;
  bufferlist::iterator p = reply.begin();
  ::decode(enc_info, p);
  ::decode(enc_ticket, p);
  
  client_secret.decrypt(enc_info, info);
  bufferlist::iterator q = info.begin();
  try {
    ::decode(session_key, q);
    ::decode(renew_after, q);
    ::decode(expires, p);
    ::decode(nonce, p);
  }
  catch (buffer::error *e) {
    delete e;
    return false;
  }
  if (!p.end())
    return false;
  
  // yay!
  enc_ticket = enc_ticket;
  return true;
}

/*
 * Build authenticator to access the service.
 *
 * enc_ticket
 * {nonce, timestamp}^client/mon session key.  do foo (assign id)
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
 * Verify authenticator and generate reply authenticator
 */
bool verify_authenticator(CryptoKey& service_secret, bufferlist& bl,
			  bufferlist& enc_reply)
{
  bufferlist::iterator p = bl.begin();
  bufferlist enc_ticket, enc_info;
  ::decode(enc_ticket, p);
  ::decode(enc_info, p);

  // decrypt ticket
  ClientTicket ticket;
  CryptoKey session_key;
  {
    bufferlist bl;
    service_secret.decrypt(enc_ticket, bl);
    bufferlist::iterator p = bl.begin();
    ::decode(ticket, p);
    ::decode(session_key, p);
  }
  
  // decrypt info with session key
  utime_t timestamp;
  string nonce;
  {
    bufferlist info;
    session_key.decrypt(enc_info, info);
    bufferlist::iterator p = info.begin();
    ::decode(timestamp, p);
    ::decode(nonce, p);
  }

  // it's authentic if the nonces match
  if (nonce != ticket.nonce)
    return false;
  
  /*
   * Reply authenticator:
   *  {timestamp + 1}^session_key
   */
  bufferlist reply;
  timestamp += 1;
  ::encode(timestamp, reply);
  session_key.encrypt(reply, enc_reply);

  return true;
}


/*
 * Verify reply is authentic
 */
bool ServiceTicket::verify_reply_authenticator(utime_t then, bufferlist& enc_reply)
{
  bufferlist reply;
  session_key.decrypt(enc_reply, reply);
  
  bufferlist::iterator p = reply.begin();
  utime_t later;
  ::decode(later, p);
  if (then + 1 == later)
    return true;
  return false;
}



