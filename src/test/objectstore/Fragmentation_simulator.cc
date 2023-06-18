// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Fragmentation Simulator
 * Author: Tri Dao, daominhtri0503@gmail.com
 */
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/hobject.h"
#include "global/global_init.h"
#include "include/buffer_fwd.h"
#include "os/ObjectStore.h"
#include "test/objectstore/ObjectStoreImitator.h"
#include <gtest/gtest.h>
#include <iostream>

#define dout_context g_ceph_context

constexpr uint64_t _1Kb = 1024;
constexpr uint64_t _1Mb = 1024 * _1Kb;
constexpr uint64_t _1Gb = 1024 * _1Mb;

static bufferlist make_bl(size_t len, char c) {
  bufferlist bl;
  if (len > 0) {
    bl.reserve(len);
    bl.append(std::string(len, c));
  }

  return bl;
}

// --------- FragmentationSimulator ----------

class FragmentationSimulator {
public:
  struct WorkloadGenerator {
    virtual int generate_txns(ObjectStore::CollectionHandle &ch,
                              ObjectStoreImitator *os) = 0;
    virtual std::string name() = 0;

    WorkloadGenerator() {}
    virtual ~WorkloadGenerator() {}
  };
  using WorkloadGeneratorRef = std::shared_ptr<WorkloadGenerator>;
  std::vector<WorkloadGeneratorRef> generators;

  void add_generator(WorkloadGeneratorRef gen) {
    std::cout << "Generator: " << gen->name() << " added\n";
    generators.push_back(gen);
  }

  int begin_simulation_with_generators() {
    for (auto &g : generators) {
      ObjectStore::CollectionHandle ch =
          os->create_new_collection(coll_t::meta());
      ObjectStore::Transaction t;
      t.create_collection(ch->cid, 0);
      os->queue_transaction(ch, std::move(t));

      int r = g->generate_txns(ch, os);
      if (r < 0)
        return r;
    }

    return 0;
  }

  FragmentationSimulator(const std::string &alloc_type, uint64_t size,
                         uint64_t min_alloc_size = 4096) {
    std::cout << "Initializing simulator" << std::endl;

    os = new ObjectStoreImitator(g_ceph_context, "", min_alloc_size);
    std::cout << "Initializing allocator: " << alloc_type << " size: 0x"
              << std::hex << size << std::dec << "\n"
              << std::endl;
    os->init_alloc(alloc_type, size);
  }
  ~FragmentationSimulator() { delete os; }

private:
  ObjectStoreImitator *os;
};

// --------- SimpleCWGenerator ----------

struct SimpleCWGenerator : public FragmentationSimulator::WorkloadGenerator {
  std::string name() override { return "SimpleCW"; }
  int generate_txns(ObjectStore::CollectionHandle &ch,
                    ObjectStoreImitator *os) override {
    hobject_t h1;
    h1.oid = "obj_1";
    h1.set_hash(1);
    h1.pool = 1;
    auto oid = ghobject_t(h1);

    ObjectStore::Transaction t1;
    t1.create(ch->get_cid(), oid);
    os->queue_transaction(ch, std::move(t1));

    ObjectStore::Transaction t2;
    t2.write(ch->get_cid(), oid, 0, _1Mb, make_bl(_1Mb, 'c'));
    os->queue_transaction(ch, std::move(t2));

    return 0;
  }
};

// ----------- Tests -----------

TEST(FragmentationSimulator, simple) {
  auto sim = FragmentationSimulator("stupid", _1Gb);
  sim.add_generator(std::make_shared<SimpleCWGenerator>());
  sim.begin_simulation_with_generators();
}

// ----------- main -----------

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct =
      global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
                  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
