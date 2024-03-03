#include <gtest/gtest.h>
#include "redis_push_client.h"
#include "redis_pull_client.h"

class RedisClientTest : public ::testing::Test {
protected:
    PushClient* pushClient;
    PullClient* pullClient;
    std::string host = "127.0.0.1";
    int port = 6379;
    std::string queueName = "test_queue";

    void SetUp() override {
        pushClient = new PushClient(host, port);
        pullClient = new PullClient(host, port);
    }

    void TearDown() override {
        if (pushClient) {
            pushClient->disconnect(); // Ensure the push client disconnects
            delete pushClient;
            pushClient = nullptr;
        }
        if (pullClient) {
            pullClient->disconnect(); // Ensure the pull client disconnects
            delete pullClient;
            pullClient = nullptr;
        }
    }
};

TEST_F(RedisClientTest, PushAndPull) {
    std::string testData = "hello world";
    pushClient->push(queueName, testData);
    std::string result = pullClient->pull(queueName);
    EXPECT_EQ(testData, result);
}

// Test for pushing and pulling empty data
TEST_F(RedisClientTest, PushAndPullEmptyData) {
    std::string emptyData = "";
    pushClient->push(queueName, emptyData);
    std::string result = pullClient->pull(queueName);
    EXPECT_EQ(emptyData, result);
}

// Test for pushing and pulling large data
TEST_F(RedisClientTest, PushAndPullLargeData) {
    std::string largeData(1024 * 1024, 'a'); // 1MB of 'a'
    pushClient->push(queueName, largeData);
    std::string result = pullClient->pull(queueName);
    EXPECT_EQ(largeData, result);
}

// Test pushing and pulling multiple items
TEST_F(RedisClientTest, PushAndPullMultipleItems) {
    std::vector<std::string> testData = {"first", "second", "third"};
    for (const auto& data : testData) {
        pushClient->push(queueName, data);
    }

    for (const auto& expectedData : testData) {
        std::string result = pullClient->pull(queueName);
        EXPECT_EQ(expectedData, result);
    }
}

// Test for handling connection failure
TEST_F(RedisClientTest, ConnectionFailure) {
    // Assuming there's a way to simulate a connection failure or to disconnect the client
    pushClient->disconnect();
    EXPECT_THROW(pushClient->push(queueName, "data"), std::exception);
}

// Test for invalid queue name
TEST_F(RedisClientTest, InvalidQueueName) {
    std::string invalidQueueName = "";
    EXPECT_THROW(pushClient->push(invalidQueueName, "data"), std::invalid_argument);
}
