// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "include/neorados/RADOS.hpp"
#include "include/scope_guard.h"
#include "common/async/blocked_completion.h"
#include "common/async/context_pool.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include <iostream>
#include <thread>
#include <ranges>
#include <coroutine>
#include "global/global_init.h"
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/system/system_error.hpp>

boost::intrusive_ptr<CephContext> cct;

namespace neorados {

using namespace std::literals;

class TestNeoRADOS : public ::testing::Test {
public:
  TestNeoRADOS() {
  }
};

class TestNeoRADOSAsync : public ::testing::Test {
public:
  TestNeoRADOSAsync() {
  }
};

class TestNeoRADOSCoro : public ::testing::Test {
public:
  TestNeoRADOSCoro() {
  }
};

TEST_F(TestNeoRADOS, MakeWithLibRADOS) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  EXPECT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  EXPECT_THROW(
    rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, ceph::async::use_blocked),
    boost::system::system_error);
}

TEST_F(TestNeoRADOS, MakeWithCCT) {
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p,
      ceph::async::use_blocked);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  EXPECT_THROW(
      rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, ceph::async::use_blocked),
      boost::system::system_error);
}

TEST_F(TestNeoRADOS, MakeWithBuilder) {
  GTEST_SKIP(); // the build command hangs with the "use_blocked" flag
  boost::asio::io_context ctx;
  auto rados = RADOS::Builder{}.build(ctx, ceph::async::use_blocked);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  EXPECT_THROW(
    rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
          std::move(op), &bl, ceph::async::use_blocked),
    boost::system::system_error);
}

TEST_F(TestNeoRADOS, CreatePool) {
  try {
    ceph::async::io_context_pool p(1);
    auto rados = RADOS::make_with_cct(cct.get(), p,
        ceph::async::use_blocked);

    const std::string pool{"piscine"};
    auto pool_deleter = make_scope_guard([&]() {
        rados.delete_pool(pool, ceph::async::use_blocked);
      });
    rados.create_pool(pool, std::nullopt, ceph::async::use_blocked);
    const auto pool_id = rados.lookup_pool(pool, ceph::async::use_blocked);
    EXPECT_GT(pool_id, 0);
  } catch (const std::exception& e) {
    std::cout << "got exception: " << e.what() << std::endl;
    ADD_FAILURE();
  }
}

void fill_objects(int min, int max, std::set<std::string>& objects) {
  std::ranges::iota_view numbers{min, max};
  std::transform(numbers.begin(), numbers.end(), 
      std::inserter(objects, objects.begin()),
      [](auto i){return std::to_string(i);});
}

TEST_F(TestNeoRADOS, CreateAndListObjects) {
  try {
    ceph::async::io_context_pool p(1);
    auto rados = RADOS::make_with_cct(cct.get(), p,
        ceph::async::use_blocked);

    const std::string pool{"piscine"};
    auto pool_deleter = make_scope_guard([&]() {
        rados.delete_pool(pool, ceph::async::use_blocked);
      });
    rados.create_pool(pool, std::nullopt, ceph::async::use_blocked);
    const auto pool_id = rados.lookup_pool(pool, ceph::async::use_blocked);
    IOContext io_ctx(pool_id);
    std::set<std::string> objects;
    fill_objects(1, 100, objects);
    for (const auto& o : objects) {
      WriteOp op;
      ceph::bufferlist bl;
      bl.append("nothing to see here");
      op.write_full(std::move(bl));
      rados.execute(o, io_ctx, std::move(op), ceph::async::use_blocked);
    }

    std::set<std::string> fetched_objects;
    auto b = Cursor::begin();
    const auto e = Cursor::end();
    std::vector<Entry> v;
    Cursor next = Cursor::begin();
    do {
      std::tie(v, next) = rados.enumerate_objects(io_ctx, b, e, 1000, {}, ceph::async::use_blocked);
      std::transform(v.cbegin(), v.cend(), 
          std::inserter(fetched_objects, fetched_objects.begin()), 
          [](const Entry& e){return e.oid;});
    } while (next != Cursor::end());
    EXPECT_EQ(fetched_objects, objects);
  } catch (const std::exception& e) {
    std::cout << "got exception: " << e.what() << std::endl;
    ADD_FAILURE();
  }
}

// return whether s1 is subset of s2
// by checking if: s2 == s1 U s2
template<typename T>
bool is_subset(const std::set<T>& s1, const std::set<T>& s2) {
  std::set<T> union_set;
  std::set_union(s1.begin(), s1.end(), 
      s2.begin(), s2.end(),
      std::inserter(union_set, union_set.begin()));
  return union_set == s2;
}

TEST_F(TestNeoRADOS, ListPools) {
  try {
    ceph::async::io_context_pool p(1);
    auto rados = RADOS::make_with_cct(cct.get(), p,
        ceph::async::use_blocked);

    const std::set<std::string> pools{"piscine", "piscina", "zwembad"};

    auto pool_deleter = make_scope_guard([&]() {
        std::for_each(pools.begin(), pools.end(), [&](const auto& pool) {
          rados.delete_pool(pool, ceph::async::use_blocked);
        });
      });

    std::for_each(pools.begin(), pools.end(), [&](const auto& pool) {
        rados.create_pool(pool, std::nullopt, ceph::async::use_blocked);
      });

    const auto list = rados.list_pools(ceph::async::use_blocked);
    std::set<std::string> fetched_pools;
    std::transform(list.begin(), list.end(), std::inserter(fetched_pools, fetched_pools.begin()), 
        [](const auto& p){return p.second;});

    EXPECT_TRUE(is_subset(pools, fetched_pools));
  } catch (const std::exception& e) {
    std::cout << "got exception: " << e.what() << std::endl;
    ADD_FAILURE();
  }
}

TEST_F(TestNeoRADOSAsync, MakeWithLibRADOS) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  EXPECT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  EXPECT_THROW(
    rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, boost::asio::use_future).get(),
    boost::system::system_error);
}

TEST_F(TestNeoRADOSAsync, MakeWithCCT) {
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p,
      boost::asio::use_future).get();

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  EXPECT_THROW(
      rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, boost::asio::use_future).get(),
      boost::system::system_error);
}

TEST_F(TestNeoRADOSAsync, MakeWithBuilder) {
  GTEST_SKIP(); // the build command hangs with the "use_future" flag
  boost::asio::io_context ctx;
  auto rados = RADOS::Builder{}.build(ctx, boost::asio::use_future).get();

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  EXPECT_THROW(
    rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
          std::move(op), &bl, boost::asio::use_future).get(),
    boost::system::system_error);
}

TEST_F(TestNeoRADOSAsync, CreatePool) {
  try {
    ceph::async::io_context_pool p(1);
    auto rados = RADOS::make_with_cct(cct.get(), p,
        boost::asio::use_future).get();

    const std::string pool{"piscine"};
    auto pool_deleter = make_scope_guard([&]() {
        rados.delete_pool(pool, boost::asio::use_future).get();
      });
    rados.create_pool(pool, std::nullopt, boost::asio::use_future).get();
    const auto pool_id = rados.lookup_pool(pool, boost::asio::use_future).get();

    EXPECT_GT(pool_id, 0);
  } catch (const std::exception& e) {
    std::cout << "got exception: " << e.what() << std::endl;
    ADD_FAILURE();
  }
}

TEST_F(TestNeoRADOSAsync, CreateAndListObjects) {
  try {
    ceph::async::io_context_pool p(1);
    auto rados = RADOS::make_with_cct(cct.get(), p,
        boost::asio::use_future).get();

    const std::string pool{"piscine"};
    auto pool_deleter = make_scope_guard([&]() {
        rados.delete_pool(pool, boost::asio::use_future).get();
      });
    rados.create_pool(pool, std::nullopt, boost::asio::use_future).get();
    const auto pool_id = rados.lookup_pool(pool, boost::asio::use_future).get();
    IOContext io_ctx(pool_id);
    std::set<std::string> objects;
    fill_objects(1, 100, objects);
    std::vector<std::future<void>> waiters; 
    for (const auto& o : objects) {
      WriteOp op;
      ceph::bufferlist bl;
      bl.append("there is nothing to see here");
      op.write_full(std::move(bl));
      waiters.emplace_back(rados.execute(o, io_ctx, std::move(op), boost::asio::use_future));
    }

    for (auto& w : waiters) {
      w.get();
    }

    std::set<std::string> fetched_objects;
    auto b = Cursor::begin();
    const auto e = Cursor::end();
    std::vector<Entry> v;
    Cursor next = Cursor::begin();
    do {
      std::tie(v, next) = rados.enumerate_objects(io_ctx, b, e, 1000, {}, boost::asio::use_future).get();
      std::transform(v.cbegin(), v.cend(), 
        std::inserter(fetched_objects, fetched_objects.begin()), 
        [](const Entry& e){return e.oid;});
    } while (next != Cursor::end());
    EXPECT_EQ(fetched_objects, objects);
  } catch (const std::exception& e) {
    std::cout << "got exception: " << e.what() << std::endl;
    ADD_FAILURE();
  }
}

TEST_F(TestNeoRADOSAsync, ListPools) {
  try {
    ceph::async::io_context_pool p(1);
    auto rados = RADOS::make_with_cct(cct.get(), p,
        boost::asio::use_future).get();

    const std::set<std::string> pools{"piscine", "piscina", "zwembad"};

    auto pool_deleter = make_scope_guard([&]() {
        std::for_each(pools.begin(), pools.end(), [&](const auto& pool) {
          rados.delete_pool(pool, boost::asio::use_future).get();
        });
      });

    std::for_each(pools.begin(), pools.end(), [&](const auto& pool) {
        rados.create_pool(pool, std::nullopt, boost::asio::use_future).get();
      });

    const auto list = rados.list_pools(boost::asio::use_future).get();
    std::set<std::string> fetched_pools;
    std::transform(list.begin(), list.end(), std::inserter(fetched_pools, fetched_pools.begin()), 
        [](const auto& p){return p.second;});

    EXPECT_TRUE(is_subset(pools, fetched_pools));
  } catch (const std::exception& e) {
    std::cout << "got exception: " << e.what() << std::endl;
    ADD_FAILURE();
  }
}

TEST_F(TestNeoRADOSAsync, WriteAndReadObjects) {
  //TODO
}

TEST_F(TestNeoRADOSAsync, DeleteObjects) {
  //TODO
}

TEST_F(TestNeoRADOSCoro, MakeWithLibRADOS) {
  boost::asio::io_context ctx;
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  EXPECT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);
  boost::system::error_code ec;

  // provide pool that doesn't exists -- just testing round-trip
  boost::asio::co_spawn(ctx, 
    [&]() -> boost::asio::awaitable<void> {
      ReadOp op;
      bufferlist bl;
      op.read(0, 0, &bl);
      co_await rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
          std::move(op), &bl, boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    }
  );

  EXPECT_THROW(
    ctx.run(),
    boost::system::system_error);
}

TEST_F(TestNeoRADOSCoro, MakeWithCCT) {
  boost::asio::io_context ctx;
  boost::system::error_code ec;
  ceph::async::io_context_pool p(1);

  // provide pool that doesn't exists -- just testing round-trip
  boost::asio::co_spawn(ctx, 
    [&]() -> boost::asio::awaitable<void> {
      auto rados = co_await RADOS::make_with_cct(cct.get(), p, boost::asio::use_awaitable);
      ReadOp op;
      bufferlist bl;
      op.read(0, 0, &bl);
      co_await rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
          std::move(op), &bl, boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    }
  );

  EXPECT_THROW(
    ctx.run(),
    boost::system::system_error);
}

TEST_F(TestNeoRADOSCoro, MakeWithBuilder) {
  boost::asio::io_context ctx;
  boost::system::error_code ec;

  // provide pool that doesn't exists -- just testing round-trip
  boost::asio::co_spawn(ctx, 
    [&]() -> boost::asio::awaitable<void> {
      auto rados = co_await RADOS::Builder{}.build(ctx, boost::asio::use_awaitable);
      ReadOp op;
      bufferlist bl;
      op.read(0, 0, &bl);
      co_await rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
          std::move(op), &bl, boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    }
  );

  EXPECT_THROW(
    ctx.run(),
    boost::system::system_error);
}

TEST_F(TestNeoRADOSCoro, CreatePool) {
  boost::asio::io_context ctx;
  boost::system::error_code ec;
  const std::string pool{"piscine"};
  unsigned pool_id = 0;

  boost::asio::co_spawn(ctx, 
    [&]() -> boost::asio::awaitable<void> {
      auto rados = co_await RADOS::Builder{}.build(ctx, boost::asio::use_awaitable);
      co_await rados.create_pool(pool, std::nullopt, 
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);

      pool_id = co_await rados.lookup_pool(pool, 
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    }
  );

  EXPECT_NO_THROW(ctx.run());
  EXPECT_GT(pool_id, 0);

  // cleanup pool
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p, ceph::async::use_blocked);
  rados.delete_pool(pool, ceph::async::use_blocked);
}

TEST_F(TestNeoRADOSCoro, CreateAndListObjects) {
  boost::asio::io_context ctx;
  boost::system::error_code ec;
  
  std::set<std::string> objects;
  std::set<std::string> fetched_objects;
  fill_objects(1, 100, objects);
  const std::string pool{"piscine"};

  boost::asio::co_spawn(ctx, 
    [&]() -> boost::asio::awaitable<void> {
      auto rados = co_await RADOS::Builder{}.build(ctx, boost::asio::use_awaitable);
      co_await rados.create_pool(pool, std::nullopt, 
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);

      const auto pool_id = co_await rados.lookup_pool(pool, 
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);

      IOContext io_ctx(pool_id);
      for (const auto& o : objects) {
        WriteOp op;
        ceph::bufferlist bl;
        bl.append("there is nothing to see here");
        op.write_full(std::move(bl));
        co_await rados.execute(o, io_ctx, std::move(op),
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (ec) throw boost::system::system_error(ec);
      }

      auto b = Cursor::begin();
      const auto e = Cursor::end();
      std::vector<Entry> v;
      Cursor next = Cursor::begin();
      do {
        std::tie(v, next) = co_await rados.enumerate_objects(io_ctx, b, e, 1000, {},
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (ec) throw boost::system::system_error(ec);
        std::transform(v.cbegin(), v.cend(), 
          std::inserter(fetched_objects, fetched_objects.begin()), 
          [](const Entry& e){return e.oid;}
        );
      } while (next != Cursor::end());

    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    }
  );

  EXPECT_NO_THROW(ctx.run());
  EXPECT_EQ(fetched_objects, objects);

  // cleanup pool
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p, ceph::async::use_blocked);
  rados.delete_pool(pool, ceph::async::use_blocked);
}

TEST_F(TestNeoRADOSCoro, ListPools) {
  boost::asio::io_context ctx;
  boost::system::error_code ec;

  const std::set<std::string> pools{"piscine", "piscina", "zwembad"};
  std::set<std::string> fetched_pools;

  boost::asio::co_spawn(ctx, 
    [&]() -> boost::asio::awaitable<void> {
      auto rados = co_await RADOS::Builder{}.build(ctx, boost::asio::use_awaitable);
      for (const auto& pool: pools) {
        co_await rados.create_pool(pool, std::nullopt, 
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (ec) throw boost::system::system_error(ec);
      }

      const auto list = co_await rados.list_pools(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) throw boost::system::system_error(ec);
      std::transform(list.begin(), list.end(), std::inserter(fetched_pools, fetched_pools.begin()), 
          [](const auto& p){return p.second;});
      }, [](std::exception_ptr e) {
        if (e) std::rethrow_exception(e);
      }
    );

  EXPECT_NO_THROW(ctx.run());
  EXPECT_TRUE(is_subset(pools, fetched_pools));
  
  // cleanup pools
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p, ceph::async::use_blocked);
  std::for_each(pools.begin(), pools.end(), [&](const auto& pool) {
    rados.delete_pool(pool, ceph::async::use_blocked);
  });
}

TEST_F(TestNeoRADOSCoro, WriteAndReadObjects) {
  //TODO
}

TEST_F(TestNeoRADOSCoro, DeleteObjects) {
  //TODO
}
} // namespace neorados

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int seed = getpid();
  std::cout << "seed " << seed << std::endl;
  srand(seed);

  std::vector<const char*> args;
  cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  return RUN_ALL_TESTS();
}

