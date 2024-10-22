#include <gtest/gtest.h>
#include <rgw_redis_lock.h>
#include <rgw_redis_queue.h>

#include <boost/asio/io_context.hpp>
#include <memory>

using boost::redis::config;
using boost::redis::connection;

class RGWRedisQueueTest : public ::testing::Test {
 protected:
  boost::asio::io_context io;
  connection* conn;
  config* cfg;

  void SetUp() {
    cfg = new config();
    conn = new connection(io);

    boost::asio::spawn(
        io,
        [this](boost::asio::yield_context yield) {
          int res = rgw::redis::load_lua_rgwlib(io, conn, cfg, yield);
          ASSERT_EQ(res, 0);
        },
        [this](std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
          io.stop();
        });
    io.run();
  }

  void TearDown() {
    delete conn;
    delete cfg;
  }
};

TEST_F(RGWRedisQueueTest, QueueInit) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        res = rgw::redisqueue::queue_init(conn, "test_queue", 100, yield);
        ASSERT_EQ(res, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, Reserve) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, Commit) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);

        std::string data = R"({
          "Records": [
            {
              "version": "0",
              "region": "test-region"
            },
            {
              "version": "1",
              "region": "test-region"
            }
          ]
        })";

        res = rgw::redisqueue::commit(conn, "test_queue", data, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue + 1);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, Abort) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);

        res = rgw::redisqueue::abort(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, ReadAndAck) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
        ASSERT_EQ(res, 0);

        std::string data = R"({
          "Records": [
            {
              "version": "0",
              "region": "test-region"
            },
            {
              "version": "1",
              "region": "test-region"
            }
          ]
        })";

        res = rgw::redisqueue::commit(conn, "test_queue", data, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue + 1);

        std::string read_res;
        res = rgw::redisqueue::read(conn, "test_queue", read_res, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(read_res, data);

        res = rgw::redisqueue::ack(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

// Take a valid lock and try to read
TEST_F(RGWRedisQueueTest, LockedReadValidLock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::string read_res;

        std::string data = R"({
          "Records": [
            {
              "version": "0",
              "region": "test-region"
            },
            {
              "version": "1",
              "region": "test-region"
            }
          ]
        })";

        // Lock the queue
        std::string lock_name = "lock:test_queue";
        std::string lock_cookie = "mycookie";
        int duration = 500;
        int return_code =
            rgw::redislock::lock(conn, lock_name, lock_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        res = rgw::redisqueue::locked_read(conn, "test_queue", lock_cookie,
                                           read_res, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(data, read_res);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, LockedReadInvalidLock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::string read_res;

        // Lock the queue
        std::string lock_name = "lock:test_queue";
        std::string lock_cookie = "mycookie";
        int duration = 500;
        int return_code =
            rgw::redislock::lock(conn, lock_name, lock_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // Try to read with an invalid lock
        std::string invalid_lock_cookie = "invalid_cookie";
        res = rgw::redisqueue::locked_read(
            conn, "test_queue", invalid_lock_cookie, read_res, yield);
        ASSERT_NE(res, 0);
        ASSERT_EQ(res, -EBUSY);
        ASSERT_EQ(read_res, "");

        // Try to read an expired lock
        boost::asio::steady_timer timer(io, std::chrono::milliseconds(1000));
        timer.async_wait(yield);

        res = rgw::redisqueue::locked_read(conn, "test_queue", lock_cookie,
                                           read_res, yield);
        ASSERT_NE(res, 0);
        ASSERT_EQ(res, -ENOENT);
        ASSERT_EQ(read_res, "");
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, AckReadLocked) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::string read_res;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
        ASSERT_EQ(res, 0);

        std::string data = R"({
          "Records": [
            {
              "version": "0",
              "region": "test-region"
            },
            {
              "version": "1",
              "region": "test-region"
            }
          ]
        })";

        res = rgw::redisqueue::commit(conn, "test_queue", data, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue + 1);

        std::string lock_name = "lock:test_queue";
        std::string lock_cookie = "mycookie";
        int duration = 500;
        int return_code =
            rgw::redislock::lock(conn, lock_name, lock_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        res = rgw::redisqueue::locked_read(conn, "test_queue", lock_cookie,
                                           read_res, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(data, read_res);

        res =
            rgw::redisqueue::locked_ack(conn, "test_queue", "mycookie", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, CleanupStaleReservations) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);

        boost::asio::steady_timer timer(io, std::chrono::milliseconds(2000));
        timer.async_wait(yield);

        int stale_timeout = 1;  // 1 second
        res = rgw::redisqueue::cleanup_stale_reservations(conn, "test_queue",
                                                          stale_timeout, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        // The timeout used in all tests is smaller then 2 seconds
        // Hence all reservations should be cleaned up
        ASSERT_EQ(std::get<0>(status), 0);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, QueueStats) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<uint64_t, uint32_t> stats;
        std::size_t reserve_size = 4 * 1024U;

        res = rgw::redisqueue::queue_stats(conn, "test_queue", stats, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(stats), 0);
        ASSERT_EQ(std::get<1>(stats), 0);

        int jitter = rand() % 20;
        for (int i = 0; i < jitter; i++) {
          res =
              rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
          ASSERT_EQ(res, 0);
        }

        res = rgw::redisqueue::queue_stats(conn, "test_queue", stats, yield);
        ASSERT_EQ(res, 0);

        ASSERT_EQ(std::get<1>(stats), jitter);

        for (int i = 0; i < jitter; i++) {
          res = rgw::redisqueue::abort(conn, "test_queue", yield);
          ASSERT_EQ(res, 0);
        }

        res = rgw::redisqueue::queue_stats(conn, "test_queue", stats, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(stats), 0);
        ASSERT_EQ(std::get<1>(stats), 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, BatchAckReadLocked) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::string read_res;
        std::vector<std::string> read_data_batch;
        std::vector<rgw::redisqueue::rgw_queue_entry> read_res_entries;
        bool truncated;
        std::tuple<uint32_t, uint32_t> status;
        std::size_t reserve_size = 4 * 1024U;
        int batch_size = 5;
        int jitter = rand() % 20;

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        for (int i = 0; i < batch_size + jitter; i++) {
          res =
              rgw::redisqueue::reserve(conn, "test_queue", reserve_size, yield);
          ASSERT_EQ(res, 0);
        }

        std::string data = R"({
          "Records": [
            {
              "version": "0",
              "region": "test-region"
            },
            {
              "version": "1",
              "region": "test-region"
            }
          ]
        })";

        for (int i = 0; i < batch_size + jitter; i++) {
          res = rgw::redisqueue::commit(conn, "test_queue", data, yield);
          ASSERT_EQ(res, 0);
        }

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue + batch_size + jitter);

        std::string lock_name = "lock:test_queue";
        std::string lock_cookie = "mycookie";
        int duration = 500;
        int return_code =
            rgw::redislock::lock(conn, lock_name, lock_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        std::vector<std::string> data_list_batch(batch_size, data);
        res = rgw::redisqueue::locked_read(conn, "test_queue", lock_cookie,
                                           read_res, batch_size, yield);
        rgw::redisqueue::redis_queue_parse_result(read_res, read_res_entries,
                                                  &truncated);
        ASSERT_EQ(truncated, true);
        for (auto entry : read_res_entries) {
          read_data_batch.push_back(entry.data);
        }

        ASSERT_EQ(res, 0);
        ASSERT_EQ(data_list_batch, read_data_batch);

        res = rgw::redisqueue::locked_ack(conn, "test_queue", lock_cookie,
                                          batch_size, yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue + jitter);

        // Read the remaining data
        int committed_queue_len = std::get<1>(status);
        while (committed_queue_len > 0) {
          read_data_batch.clear();
          data_list_batch.clear();
          read_res_entries.clear();

          int expected_data_size = std::min(batch_size, committed_queue_len);
          data_list_batch = std::vector<std::string>(expected_data_size, data);

          res = rgw::redisqueue::locked_read(conn, "test_queue", lock_cookie,
                                             read_res, batch_size, yield);
          redis_queue_parse_result(read_res, read_res_entries, &truncated);
          for (auto entry : read_res_entries) {
            read_data_batch.push_back(entry.data);
          }
          ASSERT_EQ(res, 0);
          ASSERT_EQ(data_list_batch, read_data_batch);

          int read_size = read_data_batch.size();
          res = rgw::redisqueue::locked_ack(conn, "test_queue", lock_cookie,
                                            read_size, yield);
          ASSERT_EQ(res, 0);

          res =
              rgw::redisqueue::queue_status(conn, "test_queue", status, yield);
          ASSERT_EQ(res, 0);
          ASSERT_EQ(std::get<0>(status), initial_reserve);
          ASSERT_EQ(std::get<1>(status), committed_queue_len - read_size);

          committed_queue_len = std::get<1>(status);
        }
        // On the last call to read, the entire queue should be returned
        ASSERT_EQ(truncated, false);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}