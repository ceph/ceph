#include "Auth.h"
#include "AuthAuthorizeHandler.h"
#include "cephx/CephxAuthorizeHandler.h"
#include "none/AuthNoneAuthorizeHandler.h"
#include "AuthSupported.h"

static bool _initialized = false;
static Mutex _lock("auth_service_handler_init");
static map<int, AuthAuthorizeHandler *> authorizers;

static void _init_authorizers(void)
{
  if (is_supported_auth(CEPH_AUTH_NONE)) {
    authorizers[CEPH_AUTH_NONE] = new AuthNoneAuthorizeHandler(); 
  }
  if (is_supported_auth(CEPH_AUTH_CEPHX)) {
    authorizers[CEPH_AUTH_CEPHX] = new CephxAuthorizeHandler(); 
  }
  _initialized = true;
}

AuthAuthorizeHandler *get_authorize_handler(int protocol)
{
  Mutex::Locker l(_lock);
  if (!_initialized) {
   _init_authorizers();
  }

  map<int, AuthAuthorizeHandler *>::iterator iter = authorizers.find(protocol);
  if (iter != authorizers.end())
    return iter->second;

  return NULL;
}
