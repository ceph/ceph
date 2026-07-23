// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "svc_config_key_rados.h"

#include "driver/rados/rgw_tools.h"

using std::string;

RGWSI_ConfigKey_RADOS::~RGWSI_ConfigKey_RADOS(){}

int RGWSI_ConfigKey_RADOS::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  maybe_insecure_mon_conn = !rgw_check_secure_mon_conn(dpp);

  return 0;
}

void RGWSI_ConfigKey_RADOS::warn_if_insecure()
{
  if (!maybe_insecure_mon_conn ||
      warned_insecure.test_and_set()) {
    return;
  }

  string s = ("rgw is configured to optionally allow insecure connections to "
	      "the monitors (auth_supported, ms_mon_client_mode), secrets "
	      "stored at the monitor configuration could leak");

  rgw_clog_warn(rados, s);

  lderr(ctx()) << __func__ << "(): WARNING: " << s << dendl;
}

int RGWSI_ConfigKey_RADOS::get(const string& key, bool secure,
			       bufferlist *result)
{
  string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";

  int ret = rados->mon_command(std::move(cmd), {}, result, nullptr);
  if (ret < 0) {
    return ret;
  }

  if (secure) {
    warn_if_insecure();
  }

  return 0;
}

int RGWSI_ConfigKey_RADOS::set(const string& key, const bufferlist& value,
			       bool secure)
{
  if (secure) {
    // the secret crosses the monitor connection during the write
    warn_if_insecure();
  }

  // the value travels in the input buffer, not the command string
  string cmd =
    "{"
      "\"prefix\": \"config-key set\", "
      "\"key\": \"" + key + "\""
    "}";

  bufferlist inbl = value;
  return rados->mon_command(std::move(cmd), std::move(inbl), nullptr, nullptr);
}

int RGWSI_ConfigKey_RADOS::rm(const string& key)
{
  string cmd =
    "{"
      "\"prefix\": \"config-key rm\", "
      "\"key\": \"" + key + "\""
    "}";

  return rados->mon_command(std::move(cmd), {}, nullptr, nullptr);
}
