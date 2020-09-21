#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <vector>

#include "include/types.h"
#include "common/code_environment.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/msgr.h"
#include "gtest/gtest.h"
#include "auth/Crypto.h"
#include "common/ceph_crypto.h"

// TODO: ensure OpenSSL init

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
