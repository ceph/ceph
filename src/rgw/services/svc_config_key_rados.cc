// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_config_key_rados.h"

#include "rgw_tools.h"

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
	      "the monitors (auth_supported, ms_mon_client_mode), ssl "
	      "certificates stored at the monitor configuration could leak");

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

  bufferlist inbl;
  int ret = rados->mon_command(cmd, inbl, result, nullptr);
  if (ret < 0) {
    return ret;
  }

  if (secure) {
    warn_if_insecure();
  }

  return 0;
}
