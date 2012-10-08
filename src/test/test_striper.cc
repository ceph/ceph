#include "gtest/gtest.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include "osdc/Striper.h"

TEST(Striper, Stripe1)
{
  ceph_file_layout l;
  memset(&l, 0, sizeof(l));

  l.fl_object_size = 262144;
  l.fl_stripe_unit = 4096;
  l.fl_stripe_count = 3;

  vector<ObjectExtent> ex;
  Striper::file_to_extents(g_ceph_context, 1, &l, 5006035, 46419, ex);

  cout << "result " << ex << std::endl;

  ASSERT_EQ(3u, ex.size());
}



int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
