/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>

#include "gtest/gtest.h"

#include "test_client.h"

using namespace std::placeholders;

typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
static TimePoint now() { return std::chrono::system_clock::now(); }


TEST(test_client, full_bore_timing) {
    std::atomic_ulong count(0);
    TestResponse resp(0, 0);
    TestClient* client;
    auto start = now();
    client = new TestClient(0,
                            [&] (const TestRequest& req) {
                                ++count;
                                client->submitResponse(resp);
                            },
                            1000, // ops to run
                            100, // iops goal
                            5); // outstanding ops allowed
    client->waitUntilDone();
    auto end = now();
    EXPECT_EQ(count, 1000) << "didn't get right number of ops";

    int milliseconds = (end - start) / std::chrono::milliseconds(1);
    EXPECT_GT(milliseconds, 10000) << "timing too fast to be correct";
    EXPECT_LT(milliseconds, 12000) << "timing suspiciously slow";
}


TEST(test_client, paused_timing) {
    std::atomic_ulong count(0);
    std::atomic_ulong unresponded_count(0);
    std::atomic_bool auto_respond(false);

    TestResponse resp(0, 0);
    TestClient* client;
    auto start = now();
    client = new TestClient(0,
                            [&] (const TestRequest& req) {
                                ++count;
                                if (auto_respond.load()) {
                                    client->submitResponse(resp);
                                } else {
                                    ++unresponded_count;
                                }
                            },
                            1000, // ops to run
                            100, // iops goal
                            50); // outstanding ops allowed
    std::thread t([&]() {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            EXPECT_EQ(unresponded_count.load(), 50) <<
                "should have 50 unresponded calls";
            auto_respond = true;
            // respond to those 50 calls
            for(int i = 0; i < 50; ++i) {
                client->submitResponse(resp);
                --unresponded_count;
            }
        });
    
    client->waitUntilDone();
    auto end = now();
    int milliseconds = (end - start) / std::chrono::milliseconds(1);

    // the 50 outstanding ops allowed means the first half-second of
    // requests get responded to during the 5 second pause. So we have
    // to adjust our expectations by a half-second.
    EXPECT_GT(milliseconds, 15000 - 500) << "timing too fast to be correct";
    EXPECT_LT(milliseconds, 17000 - 500) << "timing suspiciously slow";
    t.join();
}
