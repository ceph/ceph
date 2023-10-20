// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Fragmentation Simulator
 * Author: Tri Dao, daominhtri0503@gmail.com
 */
#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/common_init.h"
#include "common/hobject.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

#include "include/Context.h"
#include "include/buffer_fwd.h"
#include "os/ObjectStore.h"
#include "test/objectstore/ObjectStoreImitator.h"
#include <fstream>
#include <boost/random/uniform_int.hpp>
#include <fmt/core.h>
#include <mutex>
#include <string>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_test

constexpr uint64_t _1Kb = 1024;
constexpr uint64_t _1Mb = 1024 * _1Kb;
constexpr uint64_t _1Gb = 1024 * _1Mb;

typedef boost::mt11213b gen_type;

static bufferlist make_bl(size_t len, char c) {
  bufferlist bl;
  if (len > 0) {
    bl.reserve(len);
    bl.append(std::string(len, c));
  }

  return bl;
}

// --------- FragmentationSimulator ----------

class FragmentationSimulator : public ::testing::TestWithParam<std::string> {
  // Context that takes an arbitrary callback
  struct C_Callback : Context {
    C_Callback(std::function<void()> cb) : cb(cb) {}

    std::function<void()> cb;
    void finish(int r) override { cb(); }
  };

public:
  struct WorkloadGenerator {
    virtual int generate_txns(ObjectStore::CollectionHandle &ch,
                              ObjectStore *os) = 0;
    virtual std::string name() = 0;

    void register_txn(ObjectStore::Transaction &t) {
      std::unique_lock l(in_flight_lock);
      in_flight_txns++;
      t.register_on_commit(new C_Callback([this]() -> void {
        std::unique_lock l(in_flight_lock);
        if (--in_flight_txns == 0) {
          continue_cond.notify_all();
        }
      }));
    }

    // wait_till_finish blocks until all in-flight txns are finished
    void wait_till_finish() {
      std::unique_lock l(in_flight_lock);
      continue_cond.wait(l, [this]() -> bool { return !in_flight_txns; });
    }

    WorkloadGenerator() {}
    virtual ~WorkloadGenerator() {}

  private:
    unsigned in_flight_txns{0};
    ceph::condition_variable continue_cond;
    ceph::mutex in_flight_lock =
        ceph::make_mutex("WorkloadGenerator::in_flight_lock");
  };
  using WorkloadGeneratorRef = std::shared_ptr<WorkloadGenerator>;

  void add_generator(WorkloadGeneratorRef gen);
  int begin_simulation_with_generators(unsigned iterations);
  void init(const std::string &alloc_type, uint64_t size,
            uint64_t min_alloc_size = 4096);

  static void TearDownTestSuite() {}
  static void SetUpTestSuite() {}
  void TearDown() final {}

  FragmentationSimulator() = default;
  ~FragmentationSimulator() {
    if (os != nullptr)
      delete os;
  }

private:
  ObjectStoreImitator *os;
  std::vector<WorkloadGeneratorRef> generators;
};

void FragmentationSimulator::init(const std::string &alloc_type, uint64_t size,
                                  uint64_t min_alloc_size) {
  dout(0) << dendl;
  dout(20) << "Initializing ObjectStoreImitator" << dendl;
  os = new ObjectStoreImitator(g_ceph_context, "", min_alloc_size);

  dout(0) << "Initializing allocator: " << alloc_type << ", size: 0x"
          << std::hex << size << std::dec << dendl;
  os->init_alloc(alloc_type, size);
}

void FragmentationSimulator::add_generator(WorkloadGeneratorRef gen) {
  dout(5) << "Generator: " << gen->name() << " added" << dendl;
  generators.push_back(gen);
}

int FragmentationSimulator::begin_simulation_with_generators(
    unsigned iterations) {
  ObjectStore::CollectionHandle ch = os->create_new_collection(coll_t::meta());

  ObjectStore::Transaction t;
  t.create_collection(ch->cid, 0);
  os->queue_transaction(ch, std::move(t));

  gen_type rng(time(0));
  boost::uniform_int<> generator_idx(0, generators.size() - 1);

  while (iterations--) {
    int r = generators[generator_idx(rng)]->generate_txns(ch, os);
    if (r < 0)
      return r;
  }

  generators.clear();
  os->print_status();
  os->print_per_object_fragmentation();
  os->print_per_access_fragmentation();
  os->print_allocator_profile();

  return 0;
}

// --------- Generators ----------

struct SimpleCWGenerator : public FragmentationSimulator::WorkloadGenerator {
  std::string name() override { return "SimpleCW"; }
  int generate_txns(ObjectStore::CollectionHandle &ch,
                    ObjectStore *os) override {

    std::vector<ghobject_t> objs;
    for (unsigned i{0}; i < 100; ++i) {
      hobject_t h;
      h.oid = fmt::format("obj_{}", i);
      h.set_hash(1);
      h.pool = 1;
      objs.emplace_back(h);
    }

    std::vector<ObjectStore::Transaction> tls;
    for (unsigned i{0}; i < 100; ++i) {
      ObjectStore::Transaction t1;
      t1.create(ch->get_cid(), objs[i]);
      register_txn(t1);
      tls.emplace_back(std::move(t1));

      ObjectStore::Transaction t2;
      t2.write(ch->get_cid(), objs[i], 0, _1Mb, make_bl(_1Mb, 'c'));
      register_txn(t2);
      tls.emplace_back(std::move(t2));
    }

    os->queue_transactions(ch, tls);
    wait_till_finish();
    tls.clear();

    // Overwrite on object
    for (unsigned i{0}; i < 100; ++i) {
      ObjectStore::Transaction t;
      t.write(ch->get_cid(), objs[i], _1Kb * i, _1Mb * 3,
              make_bl(_1Mb * 3, 'x'));
      tls.emplace_back(std::move(t));
    }

    os->queue_transactions(ch, tls);
    wait_till_finish();
    tls.clear();

    for (unsigned i{0}; i < 50; ++i) {
      ObjectStore::Transaction t1, t2;
      t1.clone(ch->get_cid(), objs[i], objs[i + 50]);
      tls.emplace_back(std::move(t1));

      t2.clone(ch->get_cid(), objs[i + 50], objs[i]);
      tls.emplace_back(std::move(t2));
    }

    os->queue_transactions(ch, tls);
    wait_till_finish();
    tls.clear();
    return 0;
  }
};

struct RandomCWGenerator : public FragmentationSimulator::WorkloadGenerator {
  RandomCWGenerator() : WorkloadGenerator(), rng(time(0)) {}
  std::string name() override { return "RandomCW"; }
  int generate_txns(ObjectStore::CollectionHandle &ch,
                    ObjectStore *os) override {

    hobject_t h1;
    h1.oid = fmt::format("obj1");
    h1.set_hash(1);
    h1.pool = 1;
    ghobject_t obj1(h1);

    hobject_t h2;
    h2.oid = fmt::format("obj2");
    h2.set_hash(2);
    h2.pool = 1;
    ghobject_t obj2(h2);

    std::vector<ObjectStore::Transaction> tls;

    ObjectStore::Transaction t1;
    t1.create(ch->get_cid(), obj1);
    tls.emplace_back(std::move(t1));

    ObjectStore::Transaction t2;
    t2.create(ch->get_cid(), obj2);
    tls.emplace_back(std::move(t2));

    os->queue_transactions(ch, tls);
    wait_till_finish();

    boost::uniform_int<> u_size(0, _1Mb * 4);
    boost::uniform_int<> u_offset(0, _1Mb);

    for (unsigned i{0}; i < 200; ++i) {
      tls.clear();

      ObjectStore::Transaction t3;
      auto size = u_size(rng);
      auto offset = u_offset(rng);

      t3.write(ch->get_cid(), obj1, offset, size, make_bl(size, 'c'));
      tls.emplace_back(std::move(t3));

      ObjectStore::Transaction t4;
      size = u_size(rng);
      offset = u_offset(rng);

      t4.write(ch->get_cid(), obj2, offset, size, make_bl(size, 'c'));
      tls.emplace_back(std::move(t4));

      os->queue_transactions(ch, tls);
      wait_till_finish();

      bufferlist dummy;

      size = u_size(rng);
      offset = u_offset(rng);
      os->read(ch, obj1, offset, size, dummy);

      dummy.clear();

      size = u_size(rng);
      offset = u_offset(rng);
      os->read(ch, obj2, offset, size, dummy);
    }

    tls.clear();
    return 0;
  }

private:
  gen_type rng;
};

// Testing the Imitator with multiple threads. We're mainly testing for
// Collection correctness, as only one thread can act on an object at once in
// BlueStore
struct MultiThreadedCWGenerator
    : public FragmentationSimulator::WorkloadGenerator {
  std::string name() override { return "MultiThreadedCW"; }
  int generate_txns(ObjectStore::CollectionHandle &ch,
                    ObjectStore *os) override {

    auto t1 = std::thread([&]() {
      hobject_t h1;
      h1.oid = fmt::format("obj1");
      h1.set_hash(1);
      h1.pool = 1;
      ghobject_t obj1(h1);

      ObjectStore::Transaction t_create;
      t_create.create(ch->get_cid(), obj1);
      os->queue_transaction(ch, std::move(t_create));

      ObjectStore::Transaction t_write;
      t_write.write(ch->get_cid(), obj1, 0, _1Mb, make_bl(_1Mb, 'a'));
      os->queue_transaction(ch, std::move(t_write));

      bufferlist bl;
      os->read(ch, obj1, 0, _1Mb, bl);
    });

    auto t2 = std::thread([&]() {
      hobject_t h2;
      h2.oid = fmt::format("obj2");
      h2.set_hash(2);
      h2.pool = 1;
      ghobject_t obj2(h2);

      ObjectStore::Transaction t_create;
      t_create.create(ch->get_cid(), obj2);
      os->queue_transaction(ch, std::move(t_create));

      ObjectStore::Transaction t_write;
      t_write.write(ch->get_cid(), obj2, 0, _1Mb, make_bl(_1Mb, 'a'));
      os->queue_transaction(ch, std::move(t_write));

      bufferlist bl;
      os->read(ch, obj2, 0, _1Mb, bl);
    });

    t1.join();
    t2.join();
    wait_till_finish();

    return 0;
  }
};

// Replay ops from OSD on the Simulator
// Not tested
struct OpsReplayer : public FragmentationSimulator::WorkloadGenerator {
  std::string name() override { return "OpsReplayer"; }
  int generate_txns(ObjectStore::CollectionHandle &ch,
                    ObjectStore *os) override {
    std::unordered_map<std::string, std::string> row;
    std::vector<std::string> col_names;
    std::string line, col;

    std::getline(f, line);
    std::istringstream stream(line);
    // init column names
    while (stream >> col) {
      if (col != "|") {
        row[col] = "";
        col_names.push_back(col);
      }
    }

    // skipping over '---'
    std::getline(f, line);

    while (std::getline(f, line)) {
      stream.str(line);
      for (unsigned i{0}; stream >> col; i++) {
        row[col_names[i]] = col;
      }

      hobject_t h;
      h.oid = row["name"];
      ghobject_t oid(h);

      std::string op_type = row["op_type"];
      ObjectStore::Transaction t;
      if (op_type == "truncate") {
        uint64_t offset = std::stoi(row["offset"]);
        t.truncate(ch->get_cid(), oid, offset);
      } else {
        std::string offset_extent = row["offset/extent"];
        auto tilde_pos = offset_extent.find('~');
        uint64_t offset = std::stoi(offset_extent.substr(0, tilde_pos));
        uint64_t extent = std::stoi(offset_extent.substr(tilde_pos + 1));

        if (op_type == "write") {
          t.write(ch->get_cid(), oid, offset, extent, make_bl('x', extent));
        } else if (op_type == "read") {
          bufferlist bl;
          os->read(ch, oid, offset, extent, bl);
        } else if (op_type == "zero") {
          t.zero(ch->get_cid(), oid, offset, extent);
        }
      }

      if (op_type != "read") {
        os->queue_transaction(ch, std::move(t));
        wait_till_finish();
      }
    }

    return 0;
  }

  bool init_src(std::string path) {
    f.open(path);
    return f.is_open();
  }

private:
  std::ifstream f;
};

// ----------- Tests -----------

TEST_P(FragmentationSimulator, SimpleCWGenerator) {
  init(GetParam(), _1Gb);
  add_generator(std::make_shared<SimpleCWGenerator>());
  begin_simulation_with_generators(1);
}

TEST_P(FragmentationSimulator, RandomCWGenerator) {
  init(GetParam(), _1Mb * 16);
  add_generator(std::make_shared<RandomCWGenerator>());
  begin_simulation_with_generators(1);
}

TEST_P(FragmentationSimulator, MultiThreadedCWGenerator) {
  init(GetParam(), _1Mb * 4);
  add_generator(std::make_shared<MultiThreadedCWGenerator>());
  begin_simulation_with_generators(1);
}

// ----------- main -----------

INSTANTIATE_TEST_SUITE_P(Allocator, FragmentationSimulator,
                         ::testing::Values("stupid", "bitmap", "avl", "btree",
                                           "hybrid"));

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct =
      global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
                  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
