

#include "AuthNoneAuthorizeHandler.h"


bool AuthNoneAuthorizeHandler::verify_authorizer(bufferlist& authorizer_data, bufferlist& authorizer_reply,
                                              EntityName& entity_name, AuthCapsInfo& caps_info)
{
  bufferlist::iterator iter = authorizer_data.begin();

  try {
    ::decode(entity_name, iter);
  } catch (buffer::error *err) {
    return false;
  }

  caps_info.allow_all = true;

  return true;
}

