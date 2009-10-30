#include "Auth.h"
#include "AuthAuthorizeHandler.h"
#include "cephx/CephxAuthorizeHandler.h"
#include "none/AuthNoneAuthorizeHandler.h"


AuthAuthorizeHandler *get_authorize_handler(int protocol)
{
  switch (protocol) {
    case CEPH_AUTH_NONE:
      return new AuthNoneAuthorizeHandler();
    case CEPH_AUTH_CEPHX:
      return new CephxAuthorizeHandler();
    default:
      return NULL;
  }
}
