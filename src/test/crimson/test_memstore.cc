#include <chrono>
#include <cstring>
#include <numeric>
#include <fstream>
#include <seastar/core/app-template.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/sharded.hh>
#include "crimson/thread/ThreadPool.h"
#include <gtest/gtest.h>
#include "include/ceph_assert.h"
#include "common/errno.h"
#include "crimson/os/ConfigObs.h"
#include "crimson/os/memstore/MemStore.h"
#include "crimson/os/store_context.h"

class C_MEM_OnCommit : public Context {
  int cpuid;
public:
  C_MEM_OnCommit(int id): cpuid(id) {}
  void finish(int) override {
    std::cout << "posix thread cpuid is "<<sched_getcpu()<<std::endl;
    auto fut = seastar::alien::submit_to(cpuid,[this]{
      std::cout<<"seastar cpuid is "<<cpuid<<" ; return to shard #"<<seastar::engine().cpu_id()<<std::endl;
      return seastar::make_ready_future<>(); 
    });
    fut.wait();
  }
};

static seastar::future<> test_config(ConfigObs* cobs)
{
  return ceph::common::sharded_conf().start().then([cobs] {
    return ceph::common::sharded_conf().invoke_on(0, &Config::start);
  }).then([cobs] {
    return ceph::common::sharded_conf().invoke_on(0,[cobs](Config& config) {
      config.add_observer(cobs);
      return config.set_val(memstore_device_bytes,
                            "1073741824"); //1_G
    });
  }).then([cobs] {
    return ceph::common::sharded_conf().invoke_on(0,[cobs](Config& config) {
      return config.set_val(memstore_page_set,"false");
    });
  }).then([cobs] {
    return ceph::common::sharded_conf().invoke_on(0,[cobs](Config& config) {
      return config.set_val(memstore_page_size,"65536");
    });
  });
}

namespace {
ghobject_t make_ghobject(const char *oid)
{
  return ghobject_t{hobject_t{oid, "", CEPH_NOSNAP, 0, 0, ""}};
}

} // anonymous namespace

static void rm_r(const string& path)
{
  string cmd = string("rm -r ") + path;
  cout << "==> " << cmd << std::endl;
  int r = ::system(cmd.c_str());
  if (r) {
    if (r == -1) {
      r = errno;
      cout << "system() failed to fork() " << cpp_strerror(r)
           << ", continuing anyway" << std::endl;
    } else {
      cout << "failed with exit code " << r
           << ", continuing anyway" << std::endl;
    }
  }
}

class MemStoreTest{
  const std::string type;
  const std::string data_dir;
public:
  std::unique_ptr<MemStore> store;
  ObjectStore::CollectionHandle ch;
  const coll_t cid;
  StoreContext* scct;

public:
  explicit MemStoreTest(const std::string& type)
    : type(type), data_dir(type + ".test_temp_dir")
  { 
    scct = new StoreContext();
  }
  ~MemStoreTest(){
    if (scct)
      delete scct;
  }
  void SetUp () {
    ifstream ifile(data_dir.c_str());
    if (!ifile) {
      int r = ::mkdir(data_dir.c_str(), 0777);
      if (r < 0) {
        r = -errno;
        std::cout << __func__ << ": unable to create " << data_dir << ": " << cpp_strerror(r) << std::endl;
      }
      ASSERT_EQ(0, r);
    }
    store.reset(new (std::nothrow) MemStore(scct, data_dir));
    if (!store) {
      std::cout << __func__ << ": objectstore type " << type << " doesn't exist yet!" << std::endl;
    }  
    ASSERT_TRUE(store);
    ASSERT_EQ(0, store->mkfs());
    ASSERT_EQ(0, store->mount());
  }
  void TearDown() {
    ch.reset();
    if (store) {
      int r = store->umount();
      EXPECT_EQ(0, r);
      rm_r(data_dir);
    }
  }

};


seastar::future<> test_newcollection(ceph::thread::ThreadPool& tp, std::shared_ptr<MemStoreTest> st)
{
  static int cpuid = seastar::engine().cpu_id();
  auto alien_exec = [&tp, st]() mutable{
    return tp.submit([=]() {
      ObjectStore::Transaction t;
      st->ch = st->store->create_new_collection(st->cid);
      t.create_collection(st->cid, 4);
      t.register_on_applied(new C_MEM_OnCommit(cpuid));

      unsigned r = st->store->queue_transaction(st->ch, std::move(t));
      //std::this_thread::sleep_for(10ns);
      return r; 
    });
  };
  return alien_exec().then([] (unsigned r) {
      ASSERT_EQ(0U, r);
      //return seastar::make_ready_future<>();
    });
}

seastar::future<> test_memstoreclone(ceph::thread::ThreadPool& tp, std::shared_ptr<MemStoreTest> st)
{
  static const auto src = make_ghobject("src1");
  static const auto dst = make_ghobject("dst1");
  static bufferlist srcbl, dstbl, result, expected;
  int cpuid = seastar::engine().cpu_id();

  auto alien_exec = [&tp, st, cpuid]() mutable{
    return tp.submit([=]() {
      unsigned r;
      srcbl.append("111111111111");
      dstbl.append("222222222222");
      expected.append("221111111122");

      ObjectStore::Transaction t;
      t.write(st->cid, src, 0, 12, srcbl);
      t.write(st->cid, dst, 0, 12, dstbl);
      t.clone_range(st->cid, src, dst, 2, 8, 2);
      t.register_on_applied(new C_MEM_OnCommit(cpuid));
      r = st->store->queue_transaction(st->ch, std::move(t));
      ceph_assert(r==0);
      r = st->store->read(st->ch, dst, 0, 12, result);
      ceph_assert(r==12);     
      return result;
    });
  };
  
  return alien_exec().then([=] (bufferlist re) {
      ASSERT_EQ(expected, re); 
  }); 
}

seastar::future<> test_clonerangehole(ceph::thread::ThreadPool& tp, std::shared_ptr<MemStoreTest> st)
{
  static const auto src = make_ghobject("src2");
  static const auto dst = make_ghobject("dst2");
  static bufferlist srcbl, dstbl, result, expected;
  int cpuid = seastar::engine().cpu_id();
 
  auto alien_exec = [&tp, st, cpuid]() mutable{
    return tp.submit([=]() {
      unsigned r;
      srcbl.append("1111");
      dstbl.append("222222222222");
      expected.append("22\000\000\000\000\000\000\000\00022", 12);
 
      ObjectStore::Transaction t;
      t.write(st->cid, src, 12, 4, srcbl);
      t.write(st->cid, dst, 0, 12, dstbl);
      t.clone_range(st->cid, src, dst, 2, 8, 2);
      t.register_on_applied(new C_MEM_OnCommit(cpuid));
      r = st->store->queue_transaction(st->ch, std::move(t));
      ceph_assert(r==0);
      r = st->store->read(st->ch, dst, 0, 12, result);
      ceph_assert(r==12);     
      return result;
    });
  };
  return alien_exec().then([=] (bufferlist re) {
      ASSERT_EQ(expected, re);
  });

}

int main(int argc, char** argv)
{
  ceph::thread::ThreadPool tp{2, 128, 20};
  seastar::app_template app;
  auto st = std::shared_ptr<MemStoreTest>(new MemStoreTest("memstore"));     
  st->SetUp(); 
  return app.run(argc, argv, [&tp,st] {
    return test_config(st->scct->get_cobs()).then([&tp,st]{
      return tp.start().then([&tp,st] {
          return test_newcollection(tp,st).then([&tp,st]{
            return test_memstoreclone(tp,st).then([&tp,st]{
              return test_clonerangehole(tp,st);
            });
          });
      }).then([&tp,st] {
        return tp.stop();
      }).then([st] {
        return ceph::common::sharded_conf().invoke_on(0,[st](Config& config){
          config.remove_observer(st->scct->get_cobs());
        });
      }).finally([st] {
      //  asm volatile("int $3");
        return ceph::common::sharded_conf().stop();
      });
    }).finally([st] {
      st->TearDown();
      std::cout << "All tests succeeded" << std::endl;
    });
  });
}

