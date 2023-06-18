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
#include "os/ObjectStore.h"
#include "test/objectstore/ObjectStoreImitator.h"
#include <gtest/gtest.h>
#include <iostream>

#define dout_context g_ceph_context

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

  FragmentationSimulator() {
    std::cout << "Initializing simulator\n" << std::endl;
    os = new ObjectStoreImitator(g_ceph_context, "", 4096);
    os->init_alloc("btree", 1024 * 1024 * 1024, 4096);
  }
  ~FragmentationSimulator() { delete os; }

private:
  ObjectStoreImitator *os;
};

struct SimpleCWGenerator : public FragmentationSimulator::WorkloadGenerator {
  std::string name() override { return "SimpleCW"; }
  int generate_txns(ObjectStore::CollectionHandle &ch,
                    ObjectStoreImitator *os) override {
    hobject_t h1;
    h1.oid = "obj_1";
    h1.set_hash(1);
    h1.pool = 1;

    ObjectStore::Transaction t1;
    t1.create(ch->cid, ghobject_t(h1));
    os->queue_transaction(ch, std::move(t1));

    return 0;
  }
};

TEST(FragmentationSimulator, simple) {
  auto sim = FragmentationSimulator();
  sim.add_generator(std::make_shared<SimpleCWGenerator>());
  sim.begin_simulation_with_generators();
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct =
      global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
                  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
