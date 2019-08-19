// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/config.h"
#include "auth/KeyRing.h"
#include "mount.ceph.h"

#include <iostream>

extern "C" int get_keyring_secret(const char *name, char *dst, size_t len)
{
  int err;
  KeyRing keyring;
  CryptoKey secret;
  string secret_str;
  vector<const char *> args = { "--name", name };

  /* Create CephContext */
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);
  auto& conf = cct->_conf;

  conf.parse_env(cct->get_module_type()); // environment variables coverride
  conf.apply_changes(nullptr);

  err = keyring.from_ceph_context(cct.get());
  if (err) {
    mount_ceph_debug("keyring.from_ceph_context failed: %d\n", err);
    goto out;
  }

  if (!keyring.get_secret(conf->name, secret)) {
    mount_ceph_debug("keyring.get_secret failed\n");
    err = -ENOENT;
    goto out;
  }

  secret.encode_base64(secret_str);

  if (secret_str.length() + 1 > len) {
    err = -E2BIG;
    goto out;
  }

  strcpy(dst, secret_str.c_str());
out:
  return err;
}
