
#include "Auth.h"
#include "common/Clock.h"

#include "config.h"

static void hexdump(string msg, const char *s, int len)
{
  int buf_len = len*4;
  char buf[buf_len];
  int pos = 0;
  for (int i=0; i<len && pos<buf_len - 8; i++) {
    if (i && !(i%8))
      pos += snprintf(&buf[pos], buf_len-pos, " ");
    if (i && !(i%16))
      pos += snprintf(&buf[pos], buf_len-pos, "\n");
    pos += snprintf(&buf[pos], buf_len-pos, "%.2x ", (int)(unsigned char)s[i]);
  }
  dout(0) << msg << ":\n" << buf << dendl;
}




/*
 * Authentication
 */

/*
 * PRINCIPAL: request authentication
 *
 * principal_name, principal_addr.  "please authenticate me."
 */
void build_authenticate_request(EntityName& principal_name, entity_addr_t& principal_addr,
				bufferlist& request)
{
  AuthAuthenticateRequest req(principal_name, principal_addr, g_clock.now());
  ::encode(req, request);
}

void build_service_ticket_request(EntityName& principal_name, entity_addr_t& principal_addr,
                                uint32_t keys,
                                CryptoKey& session_key,
                                AuthBlob& ticket_info,
				bufferlist& request)
{
  AuthServiceTicketRequest ticket_req;

  ticket_req.addr =  principal_addr;
  ticket_req.timestamp = g_clock.now();
  ticket_req.keys = keys;

  bufferptr& s1 = session_key.get_secret();
  hexdump("encoding, session key", s1.c_str(), s1.length());
  encode_encrypt(ticket_req, session_key, request);

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
bool build_service_ticket_reply(
                     CryptoKey& principal_secret,
                     vector<SessionAuthInfo> ticket_info,
                     bufferlist& reply)
{
  vector<SessionAuthInfo>::iterator ticket_iter = ticket_info.begin(); 

  uint32_t num = ticket_info.size();
  ::encode(num, reply);

  while (ticket_iter != ticket_info.end()) {
    SessionAuthInfo& info = *ticket_iter;

    ::encode(info.service_id, reply);

    AuthServiceTicket msg_a;

    msg_a.session_key = info.session_key;
    if (encode_encrypt(msg_a, principal_secret, reply) < 0)
      return false;

    AuthServiceTicketInfo ticket_info;
    ticket_info.session_key = info.session_key;
    ticket_info.ticket = info.ticket;
    if (encode_encrypt(ticket_info, info.service_secret, reply) < 0)
      return false;

    ++ticket_iter;
  }
  return true;
}

bool verify_authenticate_request(CryptoKey& service_secret,
				 bufferlist::iterator& indata)
{
  AuthAuthenticateRequest msg;
  ::decode(msg, indata);
  dout(0) << "decoded timestamp=" << msg.timestamp << " addr=" << msg.addr << dendl;

  /* FIXME: validate that request makes sense */
  return true;
}

bool verify_service_ticket_request(CryptoKey& service_secret,
                                   CryptoKey& session_key,
                                   uint32_t& keys,
                                   bufferlist::iterator& indata)
{
  AuthServiceTicketRequest msg;

  bufferptr& s1 = session_key.get_secret();
  hexdump("decoding, session key", s1.c_str(), s1.length());
  
  dout(0) << "verify encrypted service ticket request" << dendl;
  if (decode_decrypt(msg, session_key, indata) < 0)
    return false;
  
  dout(0) << "decoded timestamp=" << msg.timestamp << " addr=" << msg.addr << " (was encrypted)" << dendl;
  
  AuthServiceTicketInfo ticket_info;
  if (decode_decrypt(ticket_info, service_secret, indata) < 0)
      return false;

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

  bufferptr& s1 = secret.get_secret();
  hexdump("decoding, session key", s1.c_str(), s1.length());
  if (decode_decrypt(msg_a, secret, indata) < 0)
    return false;
  /* FIXME: decode into relevant ticket */
  ::decode(ticket, indata);

  bufferptr& s = msg_a.session_key.get_secret();
  hexdump("decoded ticket.session key", s.c_str(), s.length());
  session_key = msg_a.session_key;
  has_key_flag = true;

  return true;
}

bool AuthTicketsManager::has_key(uint32_t service_id)
{
  map<uint32_t, AuthTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end())
    return false;
  return iter->second.has_key();
}

/*
 * PRINCIPAL: verify our attempt to authenticate succeeded.  fill out
 * this ServiceTicket with the result.
 */
bool AuthTicketsManager::verify_service_ticket_reply(CryptoKey& secret,
					      bufferlist::iterator& indata)
{
  uint32_t num;
  ::decode(num, indata);
  dout(0) << "received " << num << " keys" << dendl;

  for (int i=0; i<(int)num; i++) {
    uint32_t type;
    ::decode(type, indata);
    dout(0) << "received key type=" << type << dendl;
    if (!tickets_map[type].verify_service_ticket_reply(secret, indata))
      return false;
  }

  if (!indata.end())
    return false;

  return true;
}

/*
 * PRINCIPAL: build authenticator to access the service.
 *
 * ticket, {timestamp, nonce}^session_key
 */
bool AuthTicketHandler::build_authorizer(bufferlist& bl, AuthContext& ctx)
{
  ctx.timestamp = g_clock.now();

  ::encode(ticket, bl);

  AuthAuthorize msg;
  msg.trans_id = ctx.id;
  msg.now = ctx.timestamp;
  msg.nonce = nonce;
  encode_encrypt(msg, session_key, bl);

  return true;
}

/*
 * PRINCIPAL: build authenticator to access the service.
 *
 * ticket, {timestamp, nonce}^session_key
 */
bool AuthTicketsManager::build_authorizer(uint32_t service_id, bufferlist& bl, AuthContext& ctx)
{
  map<uint32_t, AuthTicketHandler>::iterator iter = tickets_map.find(service_id);
  if (iter == tickets_map.end())
    return false;

  AuthTicketHandler& handler = iter->second;
  return handler.build_authorizer(bl, ctx);
}

/*
 * SERVICE: verify authenticator and generate reply authenticator
 *
 * {timestamp + 1}^session_key
 */
bool verify_authorizer(CryptoKey& service_secret, bufferlist::iterator& indata,
			  bufferlist& reply_bl)
{
  AuthServiceTicketInfo ticket_info;
  decode_decrypt(ticket_info, service_secret, indata);

  AuthAuthorize auth_msg;
  decode_decrypt(auth_msg, ticket_info.session_key, indata);

  // it's authentic if the nonces match
  if (auth_msg.nonce != ticket_info.ticket.nonce)
    return false;
  dout(0) << "verify_authenticator: nonce ok" << dendl;
  
  /*
   * Reply authenticator:
   *  {timestamp + 1}^session_key
   */
  AuthAuthorizeReply reply;
  reply.trans_id = auth_msg.trans_id;
  reply.timestamp = auth_msg.now;
  reply.timestamp += 1;
  encode_encrypt(reply, ticket_info.session_key, reply_bl);

  dout(0) << "verify_authenticator: ok" << dendl;

  return true;
}

bool AuthTicketHandler::decode_reply_authorizer(bufferlist::iterator& indata, AuthAuthorizeReply& reply)
{
  if (decode_decrypt(reply, session_key, indata) < 0)
    return false;

  return true;
}

/*
 * PRINCIPAL: verify reply is authentic
 */
bool AuthTicketHandler::verify_reply_authorizer(AuthContext& ctx, AuthAuthorizeReply& reply)
{
  if (ctx.timestamp + 1 == reply.timestamp) {
    return true;
  }

  return false;
}



