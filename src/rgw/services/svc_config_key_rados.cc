
#include "svc_rados.h"
#include "svc_config_key_rados.h"

int RGWSI_ConfigKey_RADOS::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  maybe_insecure_mon_conn = !svc.rados->check_secure_mon_conn(dpp);

  return 0;
}

void RGWSI_ConfigKey_RADOS::warn_if_insecure()
{
  if (!maybe_insecure_mon_conn ||
      warned_insecure.test_and_set()) {
    return;
  }

  string s = "rgw is configured to optionally allow insecure connections to the monitors (auth_supported, ms_mon_client_mode), ssl certificates stored at the monitor configuration could leak";

  svc.rados->clog_warn(s);

  lderr(ctx()) << __func__ << "(): WARNING: " << s << dendl;
}

int RGWSI_ConfigKey_RADOS::get(const string& key, bool secure, bufferlist *result)
{
  string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";

  bufferlist inbl;
  auto handle = svc.rados->handle();
  int ret = handle.mon_command(cmd, inbl, result, nullptr);
  if (ret < 0) {
    return ret;
  }

  if (secure) {
    warn_if_insecure();
  }

  return 0;
}
