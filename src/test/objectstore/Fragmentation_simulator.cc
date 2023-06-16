// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Fragmentation Simulator
 * Author: Tri Dao, tri.dao@uwaterloo.ca
 */
#include "common/hobject.h"
#include "os/ObjectStore.h"
#include "test/objectstore/ObjectStoreImitator.h"

class FragmentationSimulator {
public:
  struct WorkloadGenerator {
    virtual int generate_txns(ObjectStore::CollectionHandle &ch,
                              ObjectStoreImitator *os) = 0;
    virtual std::string name() = 0;

    WorkloadGenerator();
    virtual ~WorkloadGenerator();
  };
  using WorkloadGeneratorRef = std::unique_ptr<WorkloadGenerator>;
  std::vector<WorkloadGeneratorRef> generators;

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
