// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <string>
#include <time.h>

#include "common/pretty_binary.h"
#include "global/global_context.h"
#include "kv/KeyValueDB.h"
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
#include "os/bluestore/Allocator.h"
#include "os/bluestore/AvlAllocator.h"

using namespace std;
typedef boost::mt11213b gen_type;

class C_do_action : public Context {
public:
  std::function<void()> action;
  C_do_action(std::function<void()> action)
    : action(action) {}

  void finish(int r) override {
    action();
  }
};

gen_type rng(0);
boost::uniform_int<> chargen('a', 'z');

std::string gen_string(size_t size) {
  std::string s;
  for (size_t i = 0; i < size; i++) {
    s.push_back(chargen(rng));
  }
  return s;
}

void create_deferred_and_terminate() {
  std::unique_ptr<ObjectStore> store;
  int64_t poolid;
  coll_t cid;
  ghobject_t hoid;
  ObjectStore::CollectionHandle ch;
  std::string const bluestore_dir = "bluestore.test_temp_dir";
  {
    string cmd = string("rm -rf ") + bluestore_dir;
    int r = ::system(cmd.c_str());
    ceph_assert(r == 0);
  }
  ceph_assert(::mkdir(bluestore_dir.c_str(), 0777) == 0);
  store = ObjectStore::create(g_ceph_context,
                              "bluestore",
                              bluestore_dir.c_str(),
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
  size_t keys_per_transaction = 100;
  size_t omap_push_repeats = 2200;

  // initial fill
  bufferlist bl_64K;
  bl_64K.append(std::string(64 * 1024, '-'));
  //write objects
  for (size_t o = 0; o < object_count; o++) {
    ObjectStore::Transaction t;
    std::string oid = "object-" + std::to_string(o);
    ghobject_t hoid(hobject_t(oid, "", CEPH_NOSNAP, 1, poolid, ""));
    t.write(cid, hoid, 0, bl_64K.length(), bl_64K);
    r = store->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }
  //spam omap
  for (size_t q = 0; q < omap_push_repeats; q++) {
    for (size_t o = 0; o < object_count; o++) {
      ObjectStore::Transaction t;
      std::string oid = "object-" + std::to_string(o);
      ghobject_t hoid(hobject_t(oid, "", CEPH_NOSNAP, 1, poolid, ""));

      std::map<std::string, bufferlist> new_keys;
      for (size_t m = 0; m < keys_per_transaction; m++) {
        bufferlist bl;
        bl.append(gen_string(100));
        new_keys.emplace(to_string(q)+gen_string(50), bl);
      }
      t.omap_setkeys(cid, hoid, new_keys);
      r = store->queue_transaction(ch, std::move(t));
      ceph_assert(r == 0);
    };
  }

  // small deferred writes over object
  // and complete overwrite of previous one
  bufferlist bl_8_bytes;
  bl_8_bytes.append("abcdefgh");
  std::atomic<size_t> deferred_counter{0};
  for (size_t o = 0; o < object_count/* - 1*/; o++) {
    ObjectStore::Transaction t;

    // sprinkle deferred writes
    std::string oid_d = "object-" + std::to_string(o/* + 1*/);
    ghobject_t hoid_d(hobject_t(oid_d, "", CEPH_NOSNAP, 1, poolid, ""));
    for(int i = 0; i < 16; i++) {
      t.write(cid, hoid_d, 4096 * i, bl_8_bytes.length(), bl_8_bytes);
    }
    // overwrite object content
    std::string oid_m = "object-" + std::to_string(o);
    ghobject_t hoid_m(hobject_t(oid_m, "", CEPH_NOSNAP, 1, poolid, ""));
    t.write(cid, hoid_m, 4096 * o, bl_64K.length(), bl_64K);

    t.register_on_commit(new C_do_action([&] {
      if (++deferred_counter == object_count) {
        exit(0);
      }
    }));
    r = store->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }
  sleep(100);
  ceph_assert(0 && "should not reach here");
}

void mount_check_L()
{
  std::unique_ptr<ObjectStore> store;
  store = ObjectStore::create(g_ceph_context,
    "bluestore", "bluestore.test_temp_dir", "store_test_temp_journal");
  // this should replay all deferred writes
  std::cout << "mounting..." << std::endl;
  ceph_assert(store->mount() == 0);
  std::cout << "checking for stale deferred (L)..." << std::endl;

  // now there should be no L entries
  BlueStore* bs = dynamic_cast<BlueStore*>(store.get());
  ceph_assert(bs);
  KeyValueDB* db = bs->get_kv();
  KeyValueDB::Iterator it = db->get_iterator("L");
  it->seek_to_first();
  if (it->valid()) {
    while (it->valid()) {
      std::cout << pretty_binary_string(it->key()) << std::endl;
      it->next();
    }
    ceph_assert(false && "there are L entries");
  }
  it.reset();
  ceph_assert(store->umount() == 0);
  std::cout << "all done and good" << std::endl;
}




/*
 * The test verifies that its not possible for deferred_replay procedure
 * to overwrite BlueFS data.
 * Corruption occurs when:
 * - BlueFS allocated some space
 * - deferred wrote over this space
 * Instead, stronger condition is checked:
 * - BlueFS allocated any space
 * - deferred wrote over
 */
void mount_check_alloc()
{
  std::unique_ptr<ObjectStore> store;

  ObjectStore::CollectionHandle ch;
  store = ObjectStore::create(g_ceph_context,
                              "bluestore",
                              "bluestore.test_temp_dir",
                              "store_test_temp_journal");
  // this should replay all deferred writes
  BlueStore* bs = dynamic_cast<BlueStore*>(store.get());
  ceph_assert(bs);

  bool called_allocate = false;
  vector<pair<uint64_t, uint64_t> > captured_allocations;
  bs->set_tracepoint_debug_deferred_replay_start(
    [&](){
      std::cout << "action before deferred replay" << std::endl;
      Allocator* alloc = bs->debug_get_alloc();
      alloc->foreach(
        [&](uint64_t offset, uint64_t length) {
          captured_allocations.emplace_back(offset, length);
        });
      std::cout << "sleeping to give compaction a chance" << std::endl;
      sleep(10);
      std::cout << "sleep end" << std::endl;
    });
  bs->set_tracepoint_debug_deferred_replay_end(
    [&](){
      std::cout << "action after deferred replay" << std::endl;
      Allocator* alloc = bs->debug_get_alloc();
      auto ca_it = captured_allocations.begin();
      alloc->foreach(
        [&](uint64_t offset, uint64_t length) {
          if (ca_it == captured_allocations.end()) {
            called_allocate = true;
            return;
          }
          if (ca_it->first != offset || ca_it->second != length) {
            called_allocate = true;
          }
          ca_it++;
        });
      std::cout << "called_allocate=" << called_allocate << std::endl;
      bs->set_tracepoint_debug_deferred_replay_track(nullptr);
      bs->set_tracepoint_debug_deferred_replay_start(nullptr);
      bs->set_tracepoint_debug_deferred_replay_end(nullptr);
    });

  interval_set<uint64_t> not_onode_allocations;
  bs->set_tracepoint_debug_init_alloc_done(
    [&](){
      Allocator* alloc = bs->debug_get_alloc();
      alloc->foreach(
        [&](uint64_t start, uint64_t len) {
          not_onode_allocations.insert(start, len);
        });
      bs->set_tracepoint_debug_init_alloc_done(nullptr);
    });
  interval_set<uint64_t> extents_sum;
  bs->set_tracepoint_debug_deferred_replay_track(
    [&](const bluestore_deferred_transaction_t& dtxn) {
      for (auto& op : dtxn.ops) {
        for (auto& e : op.extents) {
          extents_sum.insert(e.offset, e.length);
        }
      }
    });
  std::cout << "mounting..." << std::endl;
  ceph_assert(store->mount() == 0);
  std::cout << "mount done" << std::endl;
  std::cout << std::hex << "disk not used by onodes:" << not_onode_allocations << std::dec << std::endl;
  std::cout << std::hex << "disk deferred wrote to:" << extents_sum << std::dec << std::endl;
  std::cout << "allocated_some=" << called_allocate << std::endl;
  interval_set<uint64_t> wrote_to_not_onodes;
  wrote_to_not_onodes.intersection_of(extents_sum, not_onode_allocations);
  std::cout << std::hex << "disk not used by onodes written by deferred="
            << wrote_to_not_onodes << std::dec << std::endl;
  bool only_wrote_to_onodes = wrote_to_not_onodes.empty();
  bs->set_tracepoint_debug_deferred_replay_start(nullptr);
  ceph_assert(store->umount() == 0);

  ceph_assert(!called_allocate || only_wrote_to_onodes);
}




int argc;
char **argv;

boost::intrusive_ptr<CephContext> setup_env() {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
    NULL, args, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  g_ceph_context->_conf._clear_safe_to_start_threads();
  g_ceph_context->_conf.set_val_or_die("bluestore_prefer_deferred_size", "4096");
  g_ceph_context->_conf.set_val_or_die("bluefs_shared_alloc_size", "4096");
  g_ceph_context->_conf.set_val_or_die("bluestore_block_size", "10240000000");
  g_ceph_context->_conf.apply_changes(nullptr);
  return cct;
}

int main(int _argc, char **_argv) {
  argc = _argc;
  argv = _argv;

  pid_t first_test = fork();
  if (first_test == 0) {
    std::cout << "1. Testing deletion of deferred (L) entries." << std::endl;
    pid_t child = fork();
    if (child == 0) {
      auto cct = setup_env();
      g_ceph_context->_conf->bluestore_allocator = "bitmap";
      g_ceph_context->_conf->bluestore_rocksdb_options +=
          ",level0_file_num_compaction_trigger=4";
      create_deferred_and_terminate();
      ceph_assert(false && "should exit() earlier");
    } else {
      std::cout << "Waiting for fill omap and create deferred..." << std::endl;
      int stat;
      waitpid(child, &stat, 0);
      ceph_assert(WIFEXITED(stat) && WEXITSTATUS(stat) == 0);
      std::cout << "done and subprocess terminated." << std::endl;
      auto cct = setup_env();
      g_ceph_context->_conf->bluestore_allocator = "bitmap";
      g_ceph_context->_conf->bluestore_rocksdb_options +=
          ",level0_file_num_compaction_trigger=2";
      mount_check_L();
    }
  } else {
    int first_stat;
    waitpid(first_test, &first_stat, 0);
    ceph_assert(WIFEXITED(first_stat) && WEXITSTATUS(first_stat) == 0);
    std::cout << "2. Testing overwrite of space allocated by BlueFS" << std::endl;
    pid_t child = fork();
    if (child == 0) {
      auto cct = setup_env();
      g_ceph_context->_conf->bluestore_allocator = "avl";
      g_ceph_context->_conf->bluestore_rocksdb_options +=
          ",level0_file_num_compaction_trigger=4";
      create_deferred_and_terminate();
      ceph_assert(false && "should exit() earlier");
    } else {
      std::cout << "Waiting for fill omap and create deferred..." << std::endl;
      int stat;
      waitpid(child, &stat, 0);
      ceph_assert(WIFEXITED(stat) && WEXITSTATUS(stat) == 0);
      std::cout << "done and subprocess terminated." << std::endl;
      auto cct = setup_env();
      g_ceph_context->_conf->bluestore_allocator = "avl";
      g_ceph_context->_conf->bluestore_rocksdb_options +=
          ",level0_file_num_compaction_trigger=2";
      mount_check_alloc();
    }
  }
  return 0;
}
