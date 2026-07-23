#include <optional>
#include <boost/asio/bind_executor.hpp>
#include "include/neorados/RADOS.hpp"
#include "test/neorados/common_tests.h"

TEST(NeoRadosCompletion, CrossExecutor)
{
  boost::asio::io_context rados_context;
  std::optional<neorados::RADOS> rados;
  std::optional<neorados::IOContext> pool;

  neorados::RADOS::Builder{}.build(rados_context,
      [&rados, &pool] (boost::system::error_code ec, neorados::RADOS r) {
        if (ec) {
          throw boost::system::system_error(ec);
        }
        rados = std::move(r);

        const char* pool_name = "ceph_test_neorados_completions";
        rados->lookup_pool(pool_name,
            [&rados, &pool, pool_name] (boost::system::error_code ec, int64_t id) {
              if (ec == boost::system::errc::no_such_file_or_directory) {
                create_pool(*rados, pool_name,
                    [&pool] (boost::system::error_code ec, int64_t id) {
                      if (ec) {
                        throw boost::system::system_error(ec);
                      }
                      pool.emplace(id);
                    });
              } else if (ec) {
                throw boost::system::system_error(ec);
              } else {
                pool.emplace(id);
              }
            });
      });
  rados_context.run();

  // expect ReadOp completion on a separate execution context
  boost::asio::io_context completion_context;
  std::optional<boost::system::error_code> ec;
  uint64_t size;
  ceph::real_time mtime;

  rados->execute("nonexistent", *pool, neorados::ReadOp{}.stat(&size, &mtime), nullptr,
      boost::asio::bind_executor(completion_context,
          [&ec] (boost::system::error_code e) { ec = e; }));

  rados_context.restart();
  rados_context.run();

  EXPECT_FALSE(ec); // completion does not run on rados_context

  completion_context.run();

  EXPECT_TRUE(ec);

  rados->delete_pool(pool->get_pool(), boost::asio::detached);

  rados_context.restart();
  rados_context.run();
}
