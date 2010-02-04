
#include "../KeyRing.h"

#include "CephxProtocol.h"
#include "CephxAuthorizeHandler.h"


bool CephxAuthorizeHandler::verify_authorizer(KeyRing *keys, RotatingKeyRing *rkeys,
					      bufferlist& authorizer_data, bufferlist& authorizer_reply,
                                              EntityName& entity_name, uint64_t& global_id, AuthCapsInfo& caps_info)
{
  bufferlist::iterator iter = authorizer_data.begin();

  if (!authorizer_data.length()) {
    dout(0) << "verify authorizer, authorizer_data.length()=0" << dendl;
    return false;
  }

  CephXServiceTicketInfo auth_ticket_info;

  bool isvalid = cephx_verify_authorizer(keys, rkeys, iter, auth_ticket_info, authorizer_reply);
  dout(0) << "CephxAuthorizeHandler::verify_authorizer isvalid=" << isvalid << dendl;

  if (isvalid) {
    caps_info = auth_ticket_info.ticket.caps;
    entity_name = auth_ticket_info.ticket.name;
    global_id = auth_ticket_info.ticket.global_id;
  }

  return isvalid;
}

