// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <time.h>

#include "os/ObjectStore.h"
#include "os/bluestore/BlueStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/options.h" // for the size literals
#include <semaphore.h>



class C_do_action : public Context {
public:
  std::function<void()> action;
  C_do_action(std::function<void()> action)
    : action(action) {}

  void finish(int r) override {
    action();
  }
};

void create_deferred_and_terminate() {
  std::unique_ptr<ObjectStore> store;

  g_ceph_context->_conf._clear_safe_to_start_threads();
  g_ceph_context->_conf.set_val_or_die("bluestore_prefer_deferred_size", "4096");
  g_ceph_context->_conf.set_val_or_die("bluestore_allocator", "bitmap");
  g_ceph_context->_conf.set_val_or_die("bluestore_block_size", "10240000000");
  g_ceph_context->_conf.apply_changes(nullptr);

  int64_t poolid;
  coll_t cid;
  ghobject_t hoid;
  ObjectStore::CollectionHandle ch;
  std::string const db_store_dir = "bluestore.test_temp_dir_" + std::to_string(time(NULL));
  ceph_assert(::mkdir(db_store_dir.c_str(), 0777) == 0);
  store = ObjectStore::create(g_ceph_context,
                              "bluestore",
                              db_store_dir.c_str(),
                              "store_test_temp_journal");
  ceph_assert(store->mkfs() == 0);
  ceph_assert(store->mount() == 0);

  poolid = 11;
  cid = coll_t(spg_t(pg_t(1, poolid), shard_id_t::NO_SHARD));
  ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }

  {
    ObjectStore::Transaction t;
    std::string oid = "zapchajdziura";
    ghobject_t hoid(hobject_t(oid, "", CEPH_NOSNAP, 1, poolid, ""));
    bufferlist bl;
    bl.append(std::string(0xe000, '-'));
    t.write(cid, hoid, 0, 0xe000, bl);
    r = store->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }

  size_t object_count = 10;

  // initial fill
  bufferlist bl_64K;
  bl_64K.append(std::string(64 * 1024, '-'));

  std::atomic<size_t> prefill_counter{0};
  sem_t prefill_mutex;
  sem_init(&prefill_mutex, 0, 0);

  for (size_t o = 0; o < object_count; o++) {
    ObjectStore::Transaction t;
    std::string oid = "object-" + std::to_string(o);
    ghobject_t hoid(hobject_t(oid, "", CEPH_NOSNAP, 1, poolid, ""));

    t.write(cid, hoid, 0, bl_64K.length(), bl_64K);
    t.register_on_commit(new C_do_action([&] {
      if (++prefill_counter == object_count) {
	sem_post(&prefill_mutex);
      }
    }));

    r = store->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }
  sem_wait(&prefill_mutex);

  // small deferred writes over object
  // and complete overwrite of previous one
  bufferlist bl_8_bytes;
  bl_8_bytes.append("abcdefgh");
  std::atomic<size_t> deferred_counter{0};
  for (size_t o = 0; o < object_count - 1; o++) {
    ObjectStore::Transaction t;

    // sprinkle deferred writes
    std::string oid_d = "object-" + std::to_string(o + 1);
    ghobject_t hoid_d(hobject_t(oid_d, "", CEPH_NOSNAP, 1, poolid, ""));

    for(int i = 0; i < 16; i++) {
      t.write(cid, hoid_d, 4096 * i, bl_8_bytes.length(), bl_8_bytes);
    }

    // overwrite previous object
    std::string oid_m = "object-" + std::to_string(o);
    ghobject_t hoid_m(hobject_t(oid_m, "", CEPH_NOSNAP, 1, poolid, ""));
    t.write(cid, hoid_m, 0, bl_64K.length(), bl_64K);

    t.register_on_commit(new C_do_action([&] {
      if (++deferred_counter == object_count - 1) {
        exit(0);
      }
    }));
    r = store->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }
  sleep(10);
  ceph_assert(0 && "should not reach here");
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  create_deferred_and_terminate();
  return 0;
}
