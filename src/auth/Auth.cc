
#include "Auth.h"
#include "common/Clock.h"

#include "config.h"

/*
 * Authentication
 */

/*
 * CLIENT: request authentication
 *
 * client_name, client_addr.  "please authenticate me."
 */
void build_authenticate_request(EntityName& client_name, entity_addr_t client_addr,
				bufferlist& request)
{
  ::encode(client_name, request);
  ::encode(client_addr, request);
}

/*
 * AUTH SERVER: authenticate
 *
 * {session key, validity, nonce}^client_secret
 * {client_ticket, session key}^service_secret  ... "enc_ticket"
 */
bool build_authenticate_reply(ClientTicket& client_ticket, CryptoKey& client_secret,
			      CryptoKey& session_key, CryptoKey& service_secret,
			      bufferlist& reply)
{
  bufferlist info, enc_info;
  ::encode(session_key, info);
  ::encode(client_ticket.renew_after, info);
  ::encode(client_ticket.expires, info);
  ::encode(client_ticket.nonce, info);
  dout(0) << "encoded expires=" << client_ticket.expires << dendl;
  if (client_secret.encrypt(info, enc_info) < 0) {
    dout(0) << "error encrypting client ticket" << dendl;
    return false;
  }
  ::encode(enc_info, reply);

  bufferlist ticket, enc_ticket;
  ::encode(client_ticket, ticket);
  ::encode(session_key, ticket);
  if (service_secret.encrypt(ticket, enc_ticket) < 0) {
    dout(0) << "error ecryptng result" << dendl;
    return false;
  }
  ::encode(enc_ticket, reply);  

  dout(0) << "enc_info.length()=" << enc_info.length() << dendl;
  dout(0) << "enc_ticket.length()=" << enc_ticket.length() << dendl;

  return true;
}

/*
 * CLIENT: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool ServiceTicket::verify_authenticate_reply(CryptoKey& client_secret,
					      bufferlist::iterator& indata)
{
  dout(0) << "1" << dendl;
  bufferlist enc_info, info;
  ::decode(enc_info, indata);
  ::decode(enc_ticket, indata);
  
  if (client_secret.decrypt(enc_info, info) < 0) {
    dout(0) << "error decrypting data" << dendl;
    return false;
  }

  dout(0) << "enc_info.length()=" << enc_info.length() << dendl;
  dout(0) << "info.length()=" << info.length() << dendl;
  bufferlist::iterator q = info.begin();
  try {
    ::decode(session_key, q);
    ::decode(renew_after, q);
    ::decode(expires, q);
    ::decode(nonce, q);
    dout(0) << "decoded expires=" << expires << dendl;
  }
  catch (buffer::error *e) {
    delete e;
    return false;
  }
  if (!indata.end())
    return false;

  has_key_flag = true;

  return true;
}

/*
 * CLIENT: build authenticator to access the service.
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
    if (service_secret.decrypt(enc_ticket, bl) < 0)
      return false;
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
  if (session_key.encrypt(reply, enc_reply) < 0)
    return false;

  return true;
}


/*
 * CLIENT: verify reply is authentic
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



