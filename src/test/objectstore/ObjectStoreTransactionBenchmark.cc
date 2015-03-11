// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <iostream>

using namespace std;

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/Cycles.h"
#include "global/global_init.h"
#include "os/ObjectStore.h"

class Transaction {
 private:
  ObjectStore::Transaction t;

 public:
  struct Tick {
    uint64_t ticks;
    uint64_t count;
    Tick(): ticks(0), count(0) {}
    void add(uint64_t a) {
      ticks += a;
      count++;
    }
  };
  static Tick write_ticks, setattr_ticks, omap_setkeys_ticks, omap_rmkeys_ticks;
  static Tick encode_ticks, decode_ticks, iterate_ticks;

  void write(coll_t cid, const ghobject_t& oid, uint64_t off, uint64_t len,
             const bufferlist& data) {
    uint64_t start_time = Cycles::rdtsc();
    t.write(cid, oid, off, len, data);
    write_ticks.add(Cycles::rdtsc() - start_time);
  }
  void setattr(coll_t cid, const ghobject_t& oid, const string &name,
               bufferlist& val) {
    uint64_t start_time = Cycles::rdtsc();
    t.setattr(cid, oid, name, val);
    setattr_ticks.add(Cycles::rdtsc() - start_time);
  }
  void omap_setkeys(coll_t cid, const ghobject_t &oid,
                    const map<string, bufferlist> &attrset) {

    uint64_t start_time = Cycles::rdtsc();
    t.omap_setkeys(cid, oid, attrset);
    omap_setkeys_ticks.add(Cycles::rdtsc() - start_time);
  }
  void omap_rmkeys(coll_t cid, const ghobject_t &oid,
                   const set<string> &keys) {
    uint64_t start_time = Cycles::rdtsc();
    t.omap_rmkeys(cid, oid, keys);
    omap_rmkeys_ticks.add(Cycles::rdtsc() - start_time);
  }

  void apply_encode_decode() {
    bufferlist bl;
    ObjectStore::Transaction d;
    uint64_t start_time = Cycles::rdtsc();
    t.encode(bl);
    encode_ticks.add(Cycles::rdtsc() - start_time);

    bufferlist::iterator bliter = bl.begin();
    start_time = Cycles::rdtsc();
    d.decode(bliter);
    decode_ticks.add(Cycles::rdtsc() - start_time);
  }

  void apply_iterate() {
    uint64_t start_time = Cycles::rdtsc();
    ObjectStore::Transaction::iterator i = t.begin();
    while (i.have_op()) {
    ObjectStore::Transaction::Op *op = i.decode_op();

      switch (op->op) {
      case ObjectStore::Transaction::OP_WRITE:
        {
          coll_t cid = i.get_cid(op->cid);
          ghobject_t oid = i.get_oid(op->oid);
          bufferlist bl;
          i.decode_bl(bl);
        }
        break;
      case ObjectStore::Transaction::OP_SETATTR:
        {
          coll_t cid = i.get_cid(op->cid);
          ghobject_t oid = i.get_oid(op->oid);
          string name = i.decode_string();
          bufferlist bl;
          i.decode_bl(bl);
          map<string, bufferptr> to_set;
          to_set[name] = bufferptr(bl.c_str(), bl.length());
        }
        break;
      case ObjectStore::Transaction::OP_OMAP_SETKEYS:
        {
          coll_t cid = i.get_cid(op->cid);
          ghobject_t oid = i.get_oid(op->oid);
          map<string, bufferptr> aset;
          i.decode_attrset(aset);
        }
        break;
      case ObjectStore::Transaction::OP_OMAP_RMKEYS:
        {
          coll_t cid = i.get_cid(op->cid);
          ghobject_t oid = i.get_oid(op->oid);
          set<string> keys;
          i.decode_keyset(keys);
        }
        break;
      }
    }
    iterate_ticks.add(Cycles::rdtsc() - start_time);
  }

  static void dump_stat() {
    cerr << " write op: " << Cycles::to_microseconds(write_ticks.ticks) << "us count: " << write_ticks.count << std::endl;
    cerr << " setattr op: " << Cycles::to_microseconds(setattr_ticks.ticks) << "us count: " << setattr_ticks.count << std::endl;
    cerr << " omap_setkeys op: " << Cycles::to_microseconds(Transaction::omap_setkeys_ticks.ticks) << "us count: " << Transaction::omap_setkeys_ticks.count << std::endl;
    cerr << " omap_rmkeys op: " << Cycles::to_microseconds(Transaction::omap_rmkeys_ticks.ticks) << "us count: " << Transaction::omap_rmkeys_ticks.count << std::endl;
    cerr << " encode op: " << Cycles::to_microseconds(Transaction::encode_ticks.ticks) << "us count: " << Transaction::encode_ticks.count << std::endl;
    cerr << " decode op: " << Cycles::to_microseconds(Transaction::decode_ticks.ticks) << "us count: " << Transaction::decode_ticks.count << std::endl;
    cerr << " iterate op: " << Cycles::to_microseconds(Transaction::iterate_ticks.ticks) << "us count: " << Transaction::iterate_ticks.count << std::endl;
  }
};

class PerfCase {
  static const uint64_t Kib = 1024;
  static const uint64_t Mib = 1024 * 1024;
  static const string info_epoch_attr;
  static const string info_info_attr;
  static const string attr;
  static const string snapset_attr;
  static const string pglog_attr;
  static const coll_t meta_cid;
  static const coll_t cid;
  static const ghobject_t pglog_oid;
  static const ghobject_t info_oid;
  map<string, bufferlist> data;

  ghobject_t create_object() {
    bufferlist bl = generate_random(100, 1);
    return ghobject_t(hobject_t(string("obj_")+string(bl.c_str()), string(), rand() & 2 ? CEPH_NOSNAP : rand(), rand() & 0xFF, 0, ""));
  }


  bufferlist generate_random(uint64_t len, int frag) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
    uint64_t per_frag = len / frag;
    bufferlist bl;
    for (int i = 0; i < frag; i++ ) {
      bufferptr bp(per_frag);
      for (unsigned int j = 0; j < len; j++) {
        bp[j] = alphanum[rand() % (sizeof(alphanum) - 1)];
      }
      bl.append(bp);
    }
    return bl;
  }
 public:
  PerfCase() {
    uint64_t four_kb = Kib * 4;
    uint64_t one_mb = Mib * 1;
    uint64_t four_mb = Mib * 4;
    data["4k"] = generate_random(four_kb, 1);
    data["1m"] = generate_random(one_mb, 1);
    data["4m"] = generate_random(four_mb, 1);
    data[attr] = generate_random(256, 1);
    data[snapset_attr] = generate_random(32, 1);
    data[pglog_attr] = generate_random(128, 1);
    data[info_epoch_attr] = generate_random(4, 1);
    data[info_info_attr] = generate_random(560, 1);
  }

  uint64_t rados_write_4k(int times) {
    uint64_t ticks = 0;
    uint64_t len = Kib *4;
    for (int i = 0; i < times; i++) {
      uint64_t start_time = 0;
      {
        Transaction t;
        ghobject_t oid = create_object();
        start_time = Cycles::rdtsc();
        t.write(cid, oid, 0, len, data["4k"]);
        t.setattr(cid, oid, attr, data[attr]);
        t.setattr(cid, oid, snapset_attr, data[snapset_attr]);
        t.apply_encode_decode();
        t.apply_iterate();
        ticks += Cycles::rdtsc() - start_time;
      }
      {
        Transaction t;
        map<string, bufferlist> pglog_attrset;
        map<string, bufferlist> info_attrset;
        set<string> keys;
        keys.insert(pglog_attr);
        pglog_attrset[pglog_attr] = data[pglog_attr];
        info_attrset[info_epoch_attr] = data[info_epoch_attr];
        info_attrset[info_info_attr] = data[info_info_attr];
        start_time = Cycles::rdtsc();
        t.omap_setkeys(meta_cid, pglog_oid, pglog_attrset);
        t.omap_setkeys(meta_cid, info_oid, info_attrset);
        t.omap_rmkeys(meta_cid, pglog_oid, keys);
        t.apply_encode_decode();
        t.apply_iterate();
        ticks += Cycles::rdtsc() - start_time;
      }
    }
    return ticks;
  }
};
const string PerfCase::info_epoch_attr("11.40_epoch");
const string PerfCase::info_info_attr("11.40_info");
const string PerfCase::attr("_");
const string PerfCase::snapset_attr("snapset");
const string PerfCase::pglog_attr("pglog_attr");
const coll_t PerfCase::meta_cid("meta");
const coll_t PerfCase::cid("meta");
const ghobject_t PerfCase::pglog_oid(hobject_t(sobject_t(object_t("cid_pglog"), 0)));
const ghobject_t PerfCase::info_oid(hobject_t(sobject_t(object_t("infos"), 0)));
Transaction::Tick Transaction::write_ticks, Transaction::setattr_ticks, Transaction::omap_setkeys_ticks, Transaction::omap_rmkeys_ticks;
Transaction::Tick Transaction::encode_ticks, Transaction::decode_ticks, Transaction::iterate_ticks;

void usage(const string &name) {
  cerr << "Usage: " << name << " [times] "
       << std::endl;
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  cerr << "args: " << args << std::endl;
  if (args.size() < 1) {
    usage(argv[0]);
    return 1;
  }

  uint64_t times = atoi(args[0]);
  PerfCase c;
  uint64_t ticks = c.rados_write_4k(times);
  Transaction::dump_stat();
  cerr << " Total rados op " << times << " run time " << Cycles::to_microseconds(ticks) << "us." << std::endl;

  return 0;
}
