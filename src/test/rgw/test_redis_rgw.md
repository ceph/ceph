

## Pre-requisites
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

## Running the tests

To build the test, run:
```
cd build
ninja
```

### Test Redis Lock

```
./bin/ceph_test_rgw_redis_lock
```

#### Using redis-cli

- [x] Lock: `FCALL lock 1 <lock_name> <cookie> <duration>`

- [x] Unlock: `FCALL unlock 1 <lock_name>`

- [ ] Assert Lock: `FCALL assert_lock 1 <lock_name> <cookie>`



### Test Redis Queue 

```
./bin/ceph_test_rgw_redis_queue
```

#### Using redis-cli

- [x] Reserve: `FCALL reserve 1 <queue_name> <reserve_size>`

- [x] Commit: `FCALL commit 1 <queue_name> <message>`

- [x] Abort: `FCALL abort 1 <queue_name>`

- [ ] Read: TODO

- [ ] Locked Read: TODO

- [ ] Stale Cleanup: TODO


- Check queue lengths:
    - Reserve Queue: `LLEN reserve:<queue_name>`
    - Commit Queue: `LLEN queue:<queue_name>` 

