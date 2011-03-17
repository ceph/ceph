#include "rgw_os_auth.h"
#include "rgw_rest.h"

static RGW_OS_Auth_Get rgw_os_auth_get;

void RGW_OS_Auth_Get::execute()
{
  RGW_LOG(0) << "RGW_OS_Auth_Get::execute()" << std::endl;
  dump_errno(s, -EPERM);
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

