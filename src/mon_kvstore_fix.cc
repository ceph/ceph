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
#include "osd/OSDMap.h"

typedef std::tr1::shared_ptr<MonitorDBStore> MonitorDBStoreRef;
typedef std::tr1::shared_ptr<MonitorStore> MonitorStoreRef;

struct ServiceVersions {
  version_t first_committed;
  version_t last_committed;
  version_t latest_full;

  version_t first_found;
  version_t last_found;

  list<version_t> found_full;
  list<version_t> missing_full;
};

class MemMonitorDBStore : public MonitorDBStore
{
  map<string,bufferlist> mem_store;
  string mem_prefix;

public:
  MemMonitorDBStore(string &path, string prefix)
    : MonitorDBStore(path),
      mem_prefix(prefix)
  { }

  virtual int get(const string &prefix, const string &key, bufferlist &bl) {
    generic_dout(30) << "MemMonitorDBStore::get("
                    << prefix << "," << key << ")" << dendl;
    if (prefix == mem_prefix) {
      if (mem_store.count(key)) {
        bl.append(mem_store[key]);
        return 0;
      }
    }
    return MonitorDBStore::get(prefix, key, bl);
  }

  virtual bool exists(const string &prefix, const string &key) {
    if (prefix == mem_prefix) {
      if (mem_store.count(key) > 0)
        return true;
    }
    return MonitorDBStore::exists(prefix, key);
  }

  virtual bool exists(const string &prefix, const version_t ver) {
    return exists(prefix, stringify(ver));
  }

  virtual int apply_transaction(MonitorDBStore::Transaction &t) {
    generic_dout(30) << "MemMonitorDBStore::apply_transaction()"
                     << " " << t.ops.size() << " ops" << dendl;

    list<Op> not_our_ops;

    for (list<Op>::iterator it = t.ops.begin(); it != t.ops.end(); ++it) {
      Op &op = *it;

      if (op.prefix != mem_prefix) {
        not_our_ops.push_back(op);
        continue;
      }

      mem_store[op.key] = op.bl;
    }

    if (not_our_ops.size() == 0)
      return 0;

    generic_dout(30) << "MemMonitorDBStore::apply_transaction()"
                     << " " << not_our_ops.size() << " ops delegated" << dendl;
    t.ops = not_our_ops;
    return MonitorDBStore::apply_transaction(t);
  }
};

int calc_progress(version_t first, version_t last, version_t curr)
{
  version_t delta = last - first;
  version_t count = curr - first;

  return ((int) (((float)count/delta)*100));
}

list<pair<version_t,version_t> > gen_intervals(list<version_t> &lst)
{
  list<pair<version_t,version_t> > intervals;

  for (list<version_t>::iterator it = lst.begin(); it != lst.end(); ++it) {
    version_t v = *it;
    if (intervals.size() == 0) {
      intervals.push_back(make_pair(v, 0));
      continue;
    }

    pair<version_t,version_t> &last = intervals.back();

    if (last.second == 0) {
      if (v == last.first + 1) {
        last.second = v;
        continue;
      } else {
        last.second = last.first;
        intervals.push_back(make_pair(v, 0));
        continue;
      }
    }

    if (v == last.second + 1) {
      last.second = v;
      continue;
    } else {
      intervals.push_back(make_pair(v, 0));
      continue;
    }
  }

  pair<version_t,version_t> &last = intervals.back();
  if (last.second == 0)
    last.second = last.first;

  return intervals;
}

string print_interval(pair<version_t,version_t> p)
{
  ostringstream s;
  if (p.first == p.second)
    s << p.first;
  else
    s << "[" << p.first << "," << p.second << "]";
  return s.str();
}

string dump_intervals(list<pair<version_t,version_t> > &lst)
{
  ostringstream os;
  for (list<pair<version_t,version_t> >::iterator it = lst.begin();
       it != lst.end(); ++it) {
    os << " ";
    os << print_interval(*it);
  }
  return os.str();
}

string dump_intervals(list<version_t> &vers)
{
  list<pair<version_t,version_t> > gaps = gen_intervals(vers);
  return dump_intervals(gaps);
}


int check_old_service_versions(MonitorStoreRef store,
                               ServiceVersions &svc)
{
  generic_dout(0) << __func__ << dendl;

  svc.first_committed = store->get_int("osdmap", "first_committed");
  svc.last_committed = store->get_int("osdmap", "last_committed");

  int last_progress = -1;
  for (version_t v = svc.first_committed; v <= svc.last_committed; ++v) {
    int done = calc_progress(svc.first_committed, svc.last_committed, v);
    if (done > last_progress && !(done % 10)) {
      generic_dout(0) << " " << done << "\% done" << dendl;
      last_progress = done;
    }

    if (!store->exists_bl_sn("osdmap", v)) {
      derr << " osdmap       ver " << v << " does not exist" << dendl;
      return -EINVAL;
    }

    if (!store->exists_bl_sn("osdmap_full", v)) {
      generic_dout(20) << " osdmap full  ver " << v
                       << " does not exist" << dendl;
      svc.missing_full.push_back(v);
    } else {
      if (svc.missing_full.size() > 0
          && svc.missing_full.back() < v
          && svc.found_full.back() < svc.missing_full.back()) {
        generic_dout(20) << " osdmap full  gap from "
                         << svc.missing_full.back()
                         << " to " << v << dendl;
      }
      svc.found_full.push_back(v);
    }
  }

  return 0;
}

int check_new_service_versions(MonitorDBStoreRef db,
                               ServiceVersions &svc)
{
  generic_dout(0) << __func__ << dendl;

  svc.first_committed = db->get("osdmap", "first_committed");
  svc.last_committed = db->get("osdmap", "last_committed");

  int last_progress = -1;
  for (version_t v = svc.first_committed; v <= svc.last_committed; ++v) {
    int done = calc_progress(svc.first_committed, svc.last_committed, v);
    if (done > last_progress && !(done % 10)) {
      generic_dout(0) << " " << done << "\% done" << dendl;
      last_progress = done;
    }

    if (!db->exists("osdmap", v)) {
      derr << " osdmap       ver " << v << " does not exist" << dendl;
      return -EINVAL;
    }

    string full_key = "full_" + stringify(v);
    if (!db->exists("osdmap", full_key)) {
      generic_dout(20) << " osdmap full  ver " << v
                       << " does not exist" << dendl;
      svc.missing_full.push_back(v);
    } else {
      if (svc.missing_full.size() > 0
          && svc.missing_full.back() < v
          && svc.found_full.back() < svc.missing_full.back()) {
        generic_dout(20) << " osdmap full  gap from "
                         << svc.missing_full.back()
                         << " to " << v << dendl;
      }
      svc.found_full.push_back(v);
    }
  }

  return 0;
}

int check_fix_viability(ServiceVersions &o, ServiceVersions &n)
{
  generic_dout(0) << __func__ << dendl;

  if (o.last_committed < n.first_committed) {
    derr << " old store versions and new store versions do not intersect!"
         << dendl;
    return -1;
  }

  if (o.found_full.size() == 0) {
    derr << " old store doesn't have full versions!" << dendl;
    return -1;
  }

  return 0;
}

int check_osdmap_gaps(MonitorDBStoreRef db,
                      version_t first, version_t last,
                      list<version_t> &missing)
{
  generic_dout(0) << __func__ << dendl;
  assert(missing.size() == 0);
  assert(first <= last);

  for (version_t v = first; v <= last; ++v) {
    string full_ver = "full_" + stringify(v);
    if (!db->exists("osdmap", full_ver)) {
      generic_dout(0) << " missing full ver " << v << dendl;
      missing.push_back(v);
    }
  }
  if (missing.size() > 0) {
    derr << " there were some gaps in the full version history!" << dendl;
    return -ENOENT;
  }
  return 0;
}


int copy_full_maps(MonitorStoreRef store,
                   MonitorDBStoreRef db,
                   pair<version_t,version_t> &interval)
{
  generic_dout(0) << " copying " << print_interval(interval) << dendl;

  version_t first = interval.first;
  version_t last = interval.second;

  int count = 0;
  int last_progress = -1;

  for (version_t v = first; v <= last; ++v) {
    int done = calc_progress(first, last, v);
    if (done > last_progress && !(done % 10)) {
      generic_dout(1) << " " << done << "\% done" << dendl;
      last_progress = done;
    }

    string full_ver = "full_" + stringify(v);

    // this should have been guaranteed before!
    assert(!db->exists("osdmap", full_ver));
    assert(store->exists_bl_sn("osdmap", v));

    bufferlist bl;
    store->get_bl_sn_safe(bl, "osdmap_full", v);

    MonitorDBStore::Transaction t;
    t.put("osdmap", full_ver, bl);
    db->apply_transaction(t);

    count ++;
  }
  generic_dout(0) << " " << count << " version" << (count > 1 ? "s" : "")
                  << " copied" << dendl;

  return 0;
}

int check_rebuilt_map(MonitorDBStoreRef db, OSDMap &osdmap)
{
  version_t epoch = osdmap.get_epoch();
  generic_dout(0) << "   check rebuilt map e" << epoch << dendl;

  version_t next_ver = epoch+1;
  string next_full_ver = "full_" + next_ver;

  if (!db->exists("osdmap", next_ver)) {
    generic_dout(5) << "      last version!" << dendl;
    return 0;
  } else if (!db->exists("osdmap", next_full_ver)) {
    generic_dout(5) << "      there are more versions but not yet rebuilt"
                    << dendl;
    return 0;
  }

  bufferlist next_full_bl;
  bufferlist next_inc_bl;

  db->get("osdmap", next_full_ver, next_full_bl);
  db->get("osdmap", next_full_ver, next_inc_bl);

  OSDMap::Incremental inc(next_inc_bl);
  osdmap.apply_incremental(inc);

  bufferlist resulting_bl;
  osdmap.encode(resulting_bl);

  if (!resulting_bl.contents_equal(next_full_bl)) {
    derr << " unable to guarantee maps correctness!" << dendl;
    derr << " contents from full map e" << epoch << " + next incremental"
         << " don't match full map e" << next_ver << dendl;
    return -EINVAL;
  }

  return 0;
}

int rebuild_full_maps(MonitorStoreRef store,
                      MonitorDBStoreRef db,
                      pair<version_t,version_t> &interval)
{
  generic_dout(0) << " rebuilding " << print_interval(interval)
                  << " from incrementals" << dendl;

  version_t first = interval.first;
  version_t last = interval.second;

  version_t last_full = first - 1;
  string full_ver = "full_" + stringify(last_full);

  bufferlist bl;

  if (db->exists("osdmap", full_ver)) {
    generic_dout(1) << "   reading last full map from kv store" << dendl;
    db->get("osdmap", full_ver, bl);
  } else if (store->exists_bl_sn("osdmap_full", last_full)) {
    generic_dout(1) << "   reading last full map from old-format store"
                    << dendl;
    store->get_bl_sn_safe(bl, "osdmap_full", last_full);
  } else {
    derr << "unable to find the last full map e" << last_full << dendl;
    return -ENOENT;
  }

  assert(bl.length() > 0);

  OSDMap osdmap;
  osdmap.decode(bl);

  int count = 0;
  int last_progress = -1;

  for (version_t v = first; v <= last; ++v) {
    int done = calc_progress(first, last, v);
    if (done > last_progress && !(done % 10)) {
      generic_dout(1) << " " << done << "\% done" << dendl;
      last_progress = done;
    }

    full_ver = "full_" + stringify(v);
    assert(!db->exists("osdmap", full_ver));

    generic_dout(5) << "   read incremental e" << v << dendl;
    assert(db->exists("osdmap", v));
    bufferlist inc_bl;
    db->get("osdmap", v, inc_bl);

    generic_dout(5) << "   applying incremental e" << v << dendl;
    OSDMap::Incremental inc(inc_bl);
    osdmap.apply_incremental(inc);

    generic_dout(5) << "   write out new full map e" << v << dendl;
    bufferlist full_bl;
    osdmap.encode(full_bl);
    MonitorDBStore::Transaction t;
    t.put("osdmap", full_ver, full_bl);
    db->apply_transaction(t);
  }
  generic_dout(0) << " " << count << " version" << (count > 1 ? "s" : "")
                  << " rebuilt" << dendl;

  int err = check_rebuilt_map(db, osdmap);
  if (err < 0) {
    derr << "error checking rebuilt maps -- unable to guarantee correctness"
         << dendl;
    return err;
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
                  << osdm_old.last_committed << "]"
                  << " full " << dump_intervals(osdm_old.found_full);
  if (osdm_old.missing_full.size() > 0) {
    *_dout << " missing " << dump_intervals(osdm_old.missing_full);
  }
  *_dout << dendl;
  generic_dout(0) << " new-format available versions:"
                  << " [" << osdm_new.first_committed << ","
                  << osdm_new.last_committed << "]"
                  << " full " << dump_intervals(osdm_new.found_full);
  if (osdm_new.missing_full.size() > 0) {
    *_dout << " missing " << dump_intervals(osdm_new.missing_full);
  }
  *_dout << dendl;

  list<pair<version_t,version_t> > missing_full;
  missing_full = gen_intervals(osdm_new.missing_full);

  list<pair<version_t,version_t> > to_copy;
  list<pair<version_t,version_t> > to_rebuild;

  generic_dout(0) << "generate intervals to copy and to rebuild" << dendl;

  for (list<pair<version_t,version_t> >::iterator it = missing_full.begin();
       it != missing_full.end(); ++it) {

    version_t first = (*it).first;
    version_t last = (*it).second;

    assert(first >= osdm_old.first_committed);

    if ((first >= osdm_old.first_committed)
        && (last <= osdm_old.last_committed)) {
      to_copy.push_back(*it);
    } else if (first > osdm_old.last_committed) {
      to_rebuild.push_back(*it);
    } else if (last > osdm_old.last_committed) {
      to_copy.push_back(make_pair(first, osdm_old.last_committed));
      to_rebuild.push_back(make_pair(osdm_old.last_committed+1, last));
    } else {
      // We already guaranteed that we are only dealing with a kv store
      // having versions >= than those on the old-format store.
      assert(0 == "this should never happen!");
    }
  }

  generic_dout(0) << " to copy:    " << dump_intervals(to_copy) << dendl;
  generic_dout(0) << " to rebuild: " << dump_intervals(to_rebuild) << dendl;

  generic_dout(0) << "copying from old-format store to kv store" << dendl;
  for (list<pair<version_t,version_t> >::iterator it = to_copy.begin();
       it != to_copy.end(); ++it) {

    int err = copy_full_maps(store, db, (*it));
    if (err < 0) {
      derr << "error copying versions for interval " << print_interval(*it)
           << ": " << cpp_strerror(err) << dendl;
      return err;
    }
  }

  generic_dout(0) << "rebuilding missing maps from incrementals" << dendl;
  for (list<pair<version_t,version_t> >::iterator it = to_rebuild.begin();
       it != to_rebuild.end(); ++it) {

    int err = rebuild_full_maps(store, db, (*it));
    if (err < 0) {
      derr << "error rebuilding maps for interval " << print_interval(*it)
           << ": " << cpp_strerror(err) << dendl;
      return err;
    }
  }

  // sanity check gaps in kv store's osdmap's full versions
  generic_dout(0) << "sanity check gaps in kv store's osdmap's full versions"
                  << dendl;
  list<version_t> missing;
  int err = check_osdmap_gaps(db,
                              osdm_new.first_committed,
                              osdm_new.last_committed,
                              missing);
  if (err < 0) {
    assert(missing.size() > 0);
    derr << " there are still gaps in the osdmap's full version history!"
         << dendl;
    return err;
  }
  generic_dout(0) << " store should be fine now!" << dendl;

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
            << " <old-format-store-path> <new-format-store-path> [options]"
            << std::endl;
  std::cerr << "  options:" << std::endl;
  std::cerr << "    --for-real        Really go through with the fix" << std::endl;
  std::cerr << "                      (default is a dry run)" << std::endl;
  std::cerr << "    --checks-only     Only check if fix is possible" << std::endl;
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

  bool sure_thing = false;
  bool dry = true;
  bool checks_only = false;
  for (vector<const char*>::iterator it = args.begin();
       it != args.end(); ) {
    if (ceph_argparse_double_dash(args, it)) {
      break;
    } else if (ceph_argparse_flag(args, it, "-h", "--help", (char*) NULL)) {
      usage(our_name);
      return 0;
    } else if (ceph_argparse_flag(args, it, "--i-am-sure", (char*) NULL)) {
      sure_thing = true;
    } else if (ceph_argparse_flag(args, it, "--for-real", (char*) NULL)) {
      dry = false;
    } else if (ceph_argparse_flag(args, it, "--checks-only", (char*)NULL)) {
      checks_only = true;
    } else {
      ++it;
    }
  }

  if (args.size() < 2) {
    usage(our_name);
    return 1;
  }

  if (!sure_thing) {
    derr << "*** This is a manual fix for bug #4521 ***" << dendl;
    derr << "*** Before using make sure you have backed up both the old-format"
         << " and new-format stores ***" << dendl;
    derr << "*** Add '--i-am-sure' once you are ready. ***" << dendl;
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

  MonitorDBStore *db_ptr;

  if (dry) {
    generic_dout(0) << "running dry" << dendl;
    db_ptr = new MemMonitorDBStore(db_path, "osdmap");
  } else {
    db_ptr = new MonitorDBStore(db_path);
  }

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

  do {
    ServiceVersions old_vers;
    ServiceVersions new_vers;

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

    err = check_fix_viability(old_vers, new_vers);
    if (err < 0) {
      derr << "we can't do anything to fix this" << dendl;
      break;
    }
    generic_dout(0) << "fix is viable" << dendl;

    if (checks_only)
      break;

    err = fix_osdmap_full(store, db, old_vers, new_vers);
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

