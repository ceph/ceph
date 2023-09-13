// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "include/neorados/RADOS.hpp"
#include "common/async/blocked_completion.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include <iostream>

namespace neorados {

class TestNeoRADOS : public ::testing::Test {
public:
  TestNeoRADOS() {
  }
};

TEST_F(TestNeoRADOS, MakeWithLibRADOS) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  ASSERT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  ASSERT_THROW(
    rados.execute({"dummy-obj"}, std::numeric_limits<int64_t>::max(),
                  std::move(op), nullptr, ceph::async::use_blocked),
    boost::system::system_error);
}

} // namespace neorados

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int seed = getpid();
  std::cout << "seed " << seed << std::endl;
  srand(seed);

  return RUN_ALL_TESTS();
}
