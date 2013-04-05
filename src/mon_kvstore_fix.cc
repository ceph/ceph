// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2013 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <iostream>
#include <string>
#include <sstream>
#include <list>
#include <map>
#include <set>
#include <errno.h>
#include <memory>
#include <tr1/memory>
#include <sstream>

#include "include/types.h"
#include "include/buffer.h"
#include "include/stringify.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include "common/config.h"
#include "common/errno.h"

#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/MonitorStore.h"

typedef std::tr1::shared_ptr<MonitorDBStore> MonitorDBStoreRef;
typedef std::tr1::shared_ptr<MonitorStore> MonitorStoreRef;

struct ServiceVersions {
  version_t first_committed;
  version_t last_committed;
  version_t latest_full;

  version_t first_found;
  version_t last_found;

  version_t first_full_found;
  version_t last_full_found;
};

list<string> services;

int check_old_service_versions(MonitorStoreRef store,
                               map<string,ServiceVersions> &versions)
{
  generic_dout(0) << __func__ << dendl;
  for (list<string>::iterator it = services.begin();
       it != services.end(); ++it) {
    string name = *it;
    ServiceVersions &svc = versions[name];

    svc.first_committed = store->get_int(name.c_str(), "first_committed");
    svc.last_committed = store->get_int(name.c_str(), "last_committed");

    for (version_t v = svc.first_committed; v <= svc.last_committed; ++v) {
      if (!store->exists_bl_sn(name.c_str(), v)) {
        derr << " service " << name << " ver " << v << " does not exist"
             << dendl;
        return -EINVAL;
      }
    }
    generic_dout(0) << " service " << name << " is okay" << dendl;
  }

  return 0;
}

int check_new_service_versions(MonitorDBStoreRef db,
                               map<string,ServiceVersions> &versions)
{
  generic_dout(0) << __func__ << dendl;
  for (list<string>::iterator it = services.begin();
       it != services.end(); ++it) {
    string name = *it;
    ServiceVersions &svc = versions[name];

    svc.first_committed = db->get(name, "first_committed");
    svc.last_committed = db->get(name, "last_committed");

    for (version_t v = svc.first_committed; v <= svc.last_committed; ++v) {
      if (!db->exists(name.c_str(), v)) {
        derr << " service " << name << " ver " << v << " does not exist"
             << dendl;
        return -EINVAL;
      }
    }
    generic_dout(0) << " service " << name << " is okay" << dendl;
  }

  return 0;
}

int fix_osdmap_full(MonitorStoreRef store,
                    MonitorDBStoreRef db,
                    ServiceVersions &osdm_old, ServiceVersions &osdm_new)
{
  if (osdm_old.last_committed < osdm_new.first_committed) {
    derr << "osdmap versions are incompatible; nothing we can do :("
         << dendl;
    return -EINVAL;
  }

  generic_dout(0) << "check old-format osdmap full versions" << dendl;
  generic_dout(0) << " old-format available versions:"
                  << " [" << osdm_old.first_committed << ","
                  << osdm_old.last_committed << "]" << dendl;
  // check for all the osdmap_full versions
  for (version_t v = osdm_old.first_committed;
       v <= osdm_old.last_committed; ++v) {
    if (!store->exists_bl_sn("osdmap_full", v)) {
      derr << "osdmap_full ver " << v << " does not exist!" << dendl;
      return -ENOENT;
    }
  }

  // move versions to kvstore if they don't exist there; as soon as we find an
  // already existing full version on the kv store, stop and check if we have
  // any gaps in the osdmap's full versions on the kv store.
  generic_dout(0) << "move old-format osdmap full versions to kv store" << dendl;
  for (version_t v = osdm_old.first_committed;
       v <= osdm_old.last_committed; ++v) {

    string full_ver = "full_" + stringify(v);
    if (db->exists("osdmap", full_ver)) {
      generic_dout(0) << " osdmap full ver " << v
              << " already exists on kv store -- stop!"
              << dendl;
      break;
    }

    bufferlist bl;
    store->get_bl_sn(bl, "osdmap_full", v);

    MonitorDBStore::Transaction t;
    t.put("osdmap", full_ver, bl);
    db->apply_transaction(t);
  }

  // check for gaps in kv store's osdmap's full versions
  generic_dout(0) << "check for gaps in kv store's osdmap's full versions" << dendl;
  int err = 0;
  for (version_t v = osdm_old.first_committed;
       v <= osdm_new.last_committed; ++v) {
    string full_ver = "full_" + stringify(v);
    if (!db->exists("osdmap", full_ver)) {
      generic_dout(0) << " missing full ver " << v << dendl;
      err = -ENOENT;
    }
  }
  if (err == -ENOENT) {
    derr << "there were some gaps in the full version history!" << dendl;
    return -ENOENT;
  }

  return 0;
}

bool check_gv_store(MonitorStoreRef store)
{
  generic_dout(5) << __func__ << dendl;
  if (!store->exists_bl_ss("feature_set", 0))
    return false;

  bufferlist features_bl;
  store->get_bl_ss_safe(features_bl, "feature_set", 0);
  if (!features_bl.length()) {
    generic_dout(5) << __func__ << " on-disk features length is zero" << dendl;
    return false;
  }
  CompatSet features;
  bufferlist::iterator p = features_bl.begin();
  features.decode(p);
  return (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_GV));
}

void usage(const char *pname)
{
  std::cerr << "usage: " << pname
            << " <old-format-store-path> <new-format-store-path>\n"
            << std::endl;
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  const char *our_name = argv[0];
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
	      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (args.size() < 2) {
    usage(our_name);
    return 1;
  }
  string store_path(args[0]);
  string db_path(args[1]);

  MonitorDBStoreRef db;
  MonitorStoreRef store;

  int ret = 0;

  MonitorStore *store_ptr = new MonitorStore(store_path);
  int err = store_ptr->mount();
  if (err < 0) {
    if (err == -ENOENT) {
      derr << "unable to mount old-format store at '"
           << store_path << "': "
           << cpp_strerror(err)
           << dendl;
    } else {
      derr << "it appears that a monitor is still running: "
           << cpp_strerror(err)
           << dendl;
    }
    return -err;
  }
  store.reset(store_ptr);

  MonitorDBStore *db_ptr = new MonitorDBStore(db_path);

  ostringstream err_ss;
  err = db_ptr->open(err_ss);
  if (err < 0) {
    derr << "unable to open new-format store at '" << db_path << "': "
         << err_ss.str() << dendl;
    ret = -err;
    goto cleanup_store;
  }
  db.reset(db_ptr);

  // TODO: check partial conversion, or no conversion at all!

  if (!check_gv_store(store)) {
    derr << "old-format store at '" << store_path
         << "' doesn't support Global Versions" << dendl;
    ret = EINVAL;
    goto cleanup_db;
  }

  services.push_back("auth");
  services.push_back("logm");
  services.push_back("mdsmap");
  services.push_back("monmap");
  services.push_back("osdmap");
  services.push_back("pgmap");

  do {
    map<string,ServiceVersions> old_vers;
    map<string,ServiceVersions> new_vers;

    err = check_old_service_versions(store, old_vers);
    if (err < 0) {
      derr << "something went wrong while checking old-format services versions"
        << dendl;
      break;
    }

    err = check_new_service_versions(db, new_vers);
    if (err < 0) {
      derr << "something went wrong while checking new-format services versions"
        << dendl;
      break;
    }

    err = fix_osdmap_full(store, db, old_vers["osdmap"], new_vers["osdmap"]);
    if (err < 0) {
      derr << "something went wrong while fixing the store" << dendl;
    }
  } while (false);

  if (err < 0)
    ret = -err;

cleanup_db:
cleanup_store:
  store->umount();
  return ret;
}

