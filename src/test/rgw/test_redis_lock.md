## Test Redis Lock

### Pre-requisites
The test assumes that a redis server is running on the same host on the default port: 6379.

Redis can be installed following the instructions in the official documentation: https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/

A simple and clean way is to use docker:
https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/docker/

To start the redis server:
```
docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest
```

To start the redis stack (redis server + redis insight):
Redis insight is a web-based interface to interact with the redis server.
```
docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
```


### Running the tests

1. To build the test, run:
```
cd build
ninja
```

2. To run the tests, run:
```
./bin/ceph_test_rgw_redis_lock
```