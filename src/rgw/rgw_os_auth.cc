#include "rgw_os_auth.h"
#include "rgw_rest.h"

static RGW_OS_Auth_Get rgw_os_auth_get;

void RGW_OS_Auth_Get::execute()
{
  int ret = -EPERM;

  RGW_LOG(0) << "RGW_OS_Auth_Get::execute()" << std::endl;

  const char *key = FCGX_GetParam("HTTP_X_AUTH_KEY", s->fcgx->envp);
  const char *user = FCGX_GetParam("HTTP_X_AUTH_USER", s->fcgx->envp);

  if (key && user) {
  }
  dump_errno(s, ret);
  end_header(s);

}

bool RGWHandler_OS_Auth::authorize(struct req_state *s)
{
  return true;
}

RGWOp *RGWHandler_OS_Auth::get_op()
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = &rgw_os_auth_get;
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(s);
  }
  return op;
}

