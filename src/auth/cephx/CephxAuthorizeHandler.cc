
#include "../KeyRing.h"

#include "CephxProtocol.h"
#include "CephxAuthorizeHandler.h"

#define dout_subsys ceph_subsys_auth


bool CephxAuthorizeHandler::verify_authorizer(CephContext *cct, KeyStore *keys,
					      bufferlist& authorizer_data, bufferlist& authorizer_reply,
                                              EntityName& entity_name, uint64_t& global_id, AuthCapsInfo& caps_info, uint64_t *auid)
{
  bufferlist::iterator iter = authorizer_data.begin();

  if (!authorizer_data.length()) {
    ldout(cct, 1) << "verify authorizer, authorizer_data.length()=0" << dendl;
    return false;
  }

  CephXServiceTicketInfo auth_ticket_info;

  bool isvalid = cephx_verify_authorizer(cct, keys, iter, auth_ticket_info, authorizer_reply);
  ldout(cct, 1) << "CephxAuthorizeHandler::verify_authorizer isvalid=" << isvalid << dendl;

  if (isvalid) {
    caps_info = auth_ticket_info.ticket.caps;
    entity_name = auth_ticket_info.ticket.name;
    global_id = auth_ticket_info.ticket.global_id;
    if (auid) *auid = auth_ticket_info.ticket.auid;
  }

  return isvalid;
}

