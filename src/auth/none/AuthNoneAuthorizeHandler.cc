

#include "AuthNoneAuthorizeHandler.h"


bool AuthNoneAuthorizeHandler::verify_authorizer(KeyStore *keys,
						 bufferlist& authorizer_data, bufferlist& authorizer_reply,
						 EntityName& entity_name, uint64_t& global_id, AuthCapsInfo& caps_info,
__u64 *auid)
{
  bufferlist::iterator iter = authorizer_data.begin();

  try {
    __u8 struct_v = 1;
    ::decode(struct_v, iter);
    ::decode(entity_name, iter);
    ::decode(global_id, iter);
  } catch (buffer::error *err) {
    dout(0) << "AuthNoneAuthorizeHandle::verify_authorizer() failed to decode" << dendl;
    return false;
  }

  caps_info.allow_all = true;

  return true;
}

