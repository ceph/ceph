
#include "svc_rados.h"
#include "svc_config_key_rados.h"

int RGWSI_ConfigKey_RADOS::get(const string& key, bufferlist *result)
{
  string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";

  bufferlist inbl;
  auto handle = svc.rados->handle();
  return handle.mon_command(cmd, inbl, result, nullptr);
}
