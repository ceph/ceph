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

#ifdef USE_NSS
void *init_crypto(void *p) {
  ceph::crypto::init(g_ceph_context);
  return NULL;
}

// Tests for a race condition in libnss when calling crypto_init
// multiple times simultaneously from different threads.
TEST(CRYPTO_INIT, NSS_RACE) {
  std::vector<const char*> args;
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  // Most reliably reproduced with more threads than cores.
  long n_thread = sysconf(_SC_NPROCESSORS_ONLN) * 2;
  pthread_t *ts = (pthread_t*)malloc(n_thread * sizeof(pthread_t));
  int i;
  for (i = 0; i < n_thread; i++) {
    pthread_create(&ts[i], NULL, init_crypto, NULL);
  }
  for (i = 0; i < n_thread; i++) {
    int k;
    void *p = (void*)&k;
    pthread_join(ts[i], &p);
  }
  free(ts);
}

#endif

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
