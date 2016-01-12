/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#include <atomic>
#include <chrono>
#include <iostream>

#include "gtest/gtest.h"

#include "test_client.h"

using namespace std::placeholders;
namespace chrono = std::chrono;

typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
static TimePoint now() { return std::chrono::system_clock::now(); }


TEST(test_client, timing) {
    std::atomic_ulong count;
    count.store(0);
    TestResponse resp(0, 0);
    TestClient* client;
    auto start = now();
    client = new TestClient(0,
                            [&] (const TestRequest& req) {
                                ++count;
                                client->submitResponse(resp);
                            },
                            1000,
                            100,
                            10);
    client->waitUntilDone();
    auto end = now();
    int milliseconds = (end - start) / chrono::milliseconds(1);
    std::cout << milliseconds << std::endl;
}
