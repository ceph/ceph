# src/test/ — C++ Unit Tests

GTest/GMock. Test files mirror source structure. Register in `CMakeLists.txt` via `add_ceph_unittest()`.

```bash
cd build && ninja unittest_foo && ./bin/unittest_foo
GTEST_FILTER="Suite.Name" ./bin/unittest_foo
```

`run-make-check.sh` builds everything and runs all tests — slow. For iteration, build and run individual tests directly.
