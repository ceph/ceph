#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/optional.hpp>
#include <boost/tuple/tuple.hpp>
#include <thread>
#include <vector>
#include "rgw_auth_keystone.h"

using namespace boost::asio;

class SecretCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
	cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT));
        // Initialize the cache with a valid token
        cache.add("valid_token", token, "valid_secret");
    }

    void TearDown() override {
        // Clear the cache after each test
        cache.clear();
    }

    boost::intrusive_ptr<CephContext> cct;
    rgw::auth::keystone::SecretCache cache(cct);
    rgw::keystone::TokenEnvelope token;
};

TEST_F(SecretCacheTest, TokenFoundInCache) {
    optional_yield y{null_yield}; // No yield context for synchronous test

    auto result = cache.find("valid_token", y);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->get<1>(), "valid_secret");
}

TEST_F(SecretCacheTest, TokenNotFoundInCache) {
    optional_yield y{null_yield}; // No yield context for synchronous test

    auto result = cache.find("invalid_token", y);
    ASSERT_FALSE(result);
}

TEST_F(SecretCacheTest, TokenLookupWithWaiting) {
    io_context io_ctx;

    // Spawn a coroutine to simulate an asynchronous lookup
    spawn(io_ctx, [&](yield_context yield) {
        optional_yield y{yield};

        // Attempt to find the token (should wait)
        auto result = cache.find("valid_token", y);
        ASSERT_TRUE(result); // Token should be found after waiting
        EXPECT_EQ(result->get<1>(), "valid_secret");
    });

    io_ctx.run();
}

TEST_F(SecretCacheTest, ConcurrentAccess) {
    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<bool> results(num_threads, false);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            io_context io_ctx;

            spawn(io_ctx, [&](yield_context yield) {
                optional_yield y{yield};

                // Attempt to find the token
                auto result = cache.find("valid_token", y);
                results[i] = (result && result->get<1>() == "valid_secret");
            });

            io_ctx.run();
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that all threads found the token
    for (bool result : results) {
        EXPECT_TRUE(result);
    }
}

TEST_F(SecretCacheTest, TokenLookupWithAuthRequestCacheWait) {
    io_context io_ctx;

    // Spawn a coroutine to simulate an asynchronous lookup
    spawn(io_ctx, [&](yield_context yield) {
        optional_yield y{yield};

        // Remove the token from the cache to simulate a cache miss
        cache.clear();

        // Attempt to find the token (should wait)
        auto result = cache.find("valid_token", y);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->get<1>(), "valid_secret");
    });

    // Add the token to the cache after a short delay
    std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cache.add("valid_token", token, "valid_secret");
    }).detach();

    io_ctx.run();
}
