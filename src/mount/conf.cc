// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <vector>
#include <cstring>
#include <map>

#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/config.h"
#include "auth/KeyRing.h"
#include "mount.ceph.h"
#include "mon/MonClient.h"

extern "C" void mount_ceph_get_config_info(const char *config_file,
					   const char *name,
					   struct ceph_config_info *cci)
{
  int err;
  KeyRing keyring;
  CryptoKey secret;
  std::string secret_str;
  std::string monaddrs;
  vector<const char *> args = { "--name", name };
  bool first = true;

  if (config_file) {
    args.push_back("--conf");
    args.push_back(config_file);
  }

  /* Create CephContext */
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DAEMON_ACTIONS|CINIT_FLAG_NO_MON_CONFIG);
  auto& conf = cct->_conf;

  conf.parse_env(cct->get_module_type()); // environment variables override
  conf.apply_changes(nullptr);

  MonClient monc = MonClient(cct.get());
  err = monc.build_initial_monmap();
  if (err)
    goto scrape_keyring;

  for (const auto& mon : monc.monmap.addr_mons) {
    auto& eaddr = mon.first;

    // For now, kernel client only accepts legacy addrs
    if (!eaddr.is_legacy())
      continue;

    std::string addr;
    addr += eaddr.ip_only_to_str();
    addr += ":";
    addr += std::to_string(eaddr.get_port());
    /* If this will overrun cci_mons, stop here */
    if (monaddrs.length() + 1 + addr.length() + 1 > sizeof(cci->cci_mons))
      break;

    if (first)
      first = false;
    else
      monaddrs += ",";

    monaddrs += addr;
  }

  if (monaddrs.length())
    strcpy(cci->cci_mons, monaddrs.c_str());
  else
    mount_ceph_debug("Could not discover monitor addresses");

scrape_keyring:
  err = keyring.from_ceph_context(cct.get());
  if (err) {
    mount_ceph_debug("keyring.from_ceph_context failed: %d\n", err);
    return;
  }

  if (!keyring.get_secret(conf->name, secret)) {
    mount_ceph_debug("keyring.get_secret failed\n");
    return;
  }

  secret.encode_base64(secret_str);

  if (secret_str.length() + 1 > sizeof(cci->cci_secret)) {
    mount_ceph_debug("secret is too long\n");
    return;
  }
  strcpy(cci->cci_secret, secret_str.c_str());
}
