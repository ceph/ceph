#include "svc_role_rados.h"

RGWSI_MetaBackend_Handler* RGWSI_Role_RADOS::get_be_handler()
{
  return be_handler;
}
