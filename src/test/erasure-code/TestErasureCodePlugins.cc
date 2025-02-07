// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 */

#include <errno.h>
#include <stdlib.h>
#include "arch/probe.h"
#include "arch/intel.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "common/config_proxy.h"
#include "gtest/gtest.h"

using namespace std;

class PluginTest: public ::testing::TestWithParam<const char *> {
public:
  ErasureCodeProfile profile;
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;
  int chunk_size;
  
  PluginTest() {
    std::stringstream ss(GetParam());
    while (ss.good()) {
      std::string keyvalue,k,v;
      getline(ss, keyvalue, ' ');
      std::stringstream kv(keyvalue);
      getline(kv, k, '=');
      getline(kv, v, '=');
      profile[k] = v;
    }
  }
  std::string get_plugin() {
    return profile["plugin"];
  }

  void initialize() {
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory(get_plugin(),
				  g_conf().get_val<std::string>("erasure_code_dir"),
				  profile,
				  &erasure_code,
				  &cerr));
    EXPECT_TRUE(erasure_code.get());
    chunk_size = erasure_code->get_chunk_size(get_k()*4096);
  }

  unsigned int get_k()
  {
    return erasure_code->get_data_chunk_count();
  }

  unsigned int get_m()
  {
    return erasure_code->get_coding_chunk_count();
  }

  unsigned int get_k_plus_m()
  {
    return erasure_code->get_chunk_count();
  }

  unsigned int get_w()
  {
    return std::stoul(profile["w"]);
  }

  unsigned int get_packetsize()
  {
    return std::stoul(profile["packetsize"]);
  }
};

TEST_P(PluginTest,Initialize)
{
  initialize();
}

TEST_P(PluginTest,MinimumGranularity)
{
  initialize();
  if (profile.find("w") != profile.end() && profile.find("packetsize") != profile.end()) {
    EXPECT_EQ(erasure_code->get_minimum_granularity(), get_w() * get_packetsize());
  }
  else {
    EXPECT_EQ(erasure_code->get_minimum_granularity(), 1);
  }
}

INSTANTIATE_TEST_SUITE_P(
  PluginTests,
  PluginTest,
  ::testing::Values(
    "plugin=isa technique=reed_sol_van k=2 m=1",
    "plugin=isa technique=reed_sol_van k=3 m=1",
    "plugin=isa technique=reed_sol_van k=4 m=1",
    "plugin=isa technique=reed_sol_van k=5 m=1",
    "plugin=isa technique=reed_sol_van k=6 m=1",
    "plugin=isa technique=reed_sol_van k=2 m=2",
    "plugin=isa technique=reed_sol_van k=3 m=2",
    "plugin=isa technique=reed_sol_van k=4 m=2",
    "plugin=isa technique=reed_sol_van k=5 m=2",
    "plugin=isa technique=reed_sol_van k=6 m=2",
    "plugin=isa technique=reed_sol_van k=2 m=3",
    "plugin=isa technique=reed_sol_van k=3 m=3",
    "plugin=isa technique=reed_sol_van k=4 m=3",
    "plugin=isa technique=reed_sol_van k=5 m=3",
    "plugin=isa technique=reed_sol_van k=6 m=3",
    "plugin=isa technique=cauchy k=2 m=1",
    "plugin=isa technique=cauchy k=3 m=1",
    "plugin=isa technique=cauchy k=4 m=1",
    "plugin=isa technique=cauchy k=5 m=1",
    "plugin=isa technique=cauchy k=6 m=1",
    "plugin=isa technique=cauchy k=2 m=2",
    "plugin=isa technique=cauchy k=3 m=2",
    "plugin=isa technique=cauchy k=4 m=2",
    "plugin=isa technique=cauchy k=5 m=2",
    "plugin=isa technique=cauchy k=6 m=2",
    "plugin=isa technique=cauchy k=2 m=3",
    "plugin=isa technique=cauchy k=3 m=3",
    "plugin=isa technique=cauchy k=4 m=3",
    "plugin=isa technique=cauchy k=5 m=3",
    "plugin=isa technique=cauchy k=6 m=3",
    "plugin=jerasure technique=reed_sol_van k=2 m=1",
    "plugin=jerasure technique=reed_sol_van k=3 m=1",
    "plugin=jerasure technique=reed_sol_van k=4 m=1",
    "plugin=jerasure technique=reed_sol_van k=5 m=1",
    "plugin=jerasure technique=reed_sol_van k=6 m=1",
    "plugin=jerasure technique=reed_sol_van k=2 m=2",
    "plugin=jerasure technique=reed_sol_van k=3 m=2",
    "plugin=jerasure technique=reed_sol_van k=4 m=2",
    "plugin=jerasure technique=reed_sol_van k=5 m=2",
    "plugin=jerasure technique=reed_sol_van k=6 m=2",
    "plugin=jerasure technique=reed_sol_van k=2 m=3",
    "plugin=jerasure technique=reed_sol_van k=3 m=3",
    "plugin=jerasure technique=reed_sol_van k=4 m=3",
    "plugin=jerasure technique=reed_sol_van k=5 m=3",
    "plugin=jerasure technique=reed_sol_van k=6 m=3",
    "plugin=jerasure technique=reed_sol_r6_op k=2 m=2",
    "plugin=jerasure technique=reed_sol_r6_op k=3 m=2",
    "plugin=jerasure technique=reed_sol_r6_op k=4 m=2",
    "plugin=jerasure technique=reed_sol_r6_op k=5 m=2",
    "plugin=jerasure technique=reed_sol_r6_op k=6 m=2",
    "plugin=jerasure technique=cauchy_orig k=2 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=3 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=4 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=5 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=6 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=2 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=3 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=4 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=5 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=6 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=2 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=3 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=4 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=5 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_orig k=6 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=2 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=3 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=4 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=5 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=6 m=1 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=2 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=3 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=4 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=5 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=6 m=2 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=2 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=3 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=4 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=5 m=3 packetsize=32",
    "plugin=jerasure technique=cauchy_good k=6 m=3 packetsize=32",
    "plugin=jerasure technique=liberation k=2 m=1 packetsize=32",
    "plugin=jerasure technique=liberation k=3 m=1 packetsize=32",
    "plugin=jerasure technique=liberation k=4 m=1 packetsize=32",
    "plugin=jerasure technique=liberation k=5 m=1 packetsize=32",
    "plugin=jerasure technique=liberation k=6 m=1 packetsize=32",
    "plugin=jerasure technique=liberation k=2 m=2 packetsize=32",
    "plugin=jerasure technique=liberation k=3 m=2 packetsize=32",
    "plugin=jerasure technique=liberation k=4 m=2 packetsize=32",
    "plugin=jerasure technique=liberation k=5 m=2 packetsize=32",
    "plugin=jerasure technique=liberation k=6 m=2 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=2 m=1 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=3 m=1 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=4 m=1 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=5 m=1 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=6 m=1 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=2 m=2 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=3 m=2 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=4 m=2 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=5 m=2 packetsize=32",
    "plugin=jerasure technique=blaum_roth k=6 m=2 packetsize=32",
    "plugin=jerasure technique=liber8tion k=2 m=2 packetsize=32",
    "plugin=jerasure technique=liber8tion k=3 m=2 packetsize=32",
    "plugin=jerasure technique=liber8tion k=4 m=2 packetsize=32",
    "plugin=jerasure technique=liber8tion k=5 m=2 packetsize=32",
    "plugin=jerasure technique=liber8tion k=6 m=2 packetsize=32",
    "plugin=clay k=2 m=1",
    "plugin=clay k=3 m=1",
    "plugin=clay k=4 m=1",
    "plugin=clay k=5 m=1",
    "plugin=clay k=6 m=1",
    "plugin=clay k=2 m=2",
    "plugin=clay k=3 m=2",
    "plugin=clay k=4 m=2",
    "plugin=clay k=5 m=2",
    "plugin=clay k=6 m=2",
    "plugin=clay k=2 m=3",
    "plugin=clay k=3 m=3",
    "plugin=clay k=4 m=3",
    "plugin=clay k=5 m=3",
    "plugin=clay k=6 m=3",
    "plugin=shec technique=single k=2 m=1 c=1",
    "plugin=shec technique=single k=3 m=1 c=1",
    "plugin=shec technique=single k=4 m=1 c=1",
    "plugin=shec technique=single k=5 m=1 c=1",
    "plugin=shec technique=single k=6 m=1 c=1",
    "plugin=shec technique=single k=2 m=2 c=1",
    "plugin=shec technique=single k=3 m=2 c=1",
    "plugin=shec technique=single k=4 m=2 c=1",
    "plugin=shec technique=single k=5 m=2 c=1",
    "plugin=shec technique=single k=6 m=2 c=1",
    "plugin=shec technique=single k=3 m=3 c=1",
    "plugin=shec technique=single k=4 m=3 c=1",
    "plugin=shec technique=single k=5 m=3 c=1",
    "plugin=shec technique=single k=6 m=3 c=1",
    "plugin=shec technique=single k=3 m=3 c=2",
    "plugin=shec technique=single k=4 m=3 c=2",
    "plugin=shec technique=single k=5 m=3 c=2",
    "plugin=shec technique=single k=6 m=3 c=2",
    "plugin=shec technique=multiple k=2 m=1 c=1",
    "plugin=shec technique=multiple k=3 m=1 c=1",
    "plugin=shec technique=multiple k=4 m=1 c=1",
    "plugin=shec technique=multiple k=5 m=1 c=1",
    "plugin=shec technique=multiple k=6 m=1 c=1",
    "plugin=shec technique=multiple k=2 m=2 c=1",
    "plugin=shec technique=multiple k=3 m=2 c=1",
    "plugin=shec technique=multiple k=4 m=2 c=1",
    "plugin=shec technique=multiple k=5 m=2 c=1",
    "plugin=shec technique=multiple k=6 m=2 c=1",
    "plugin=shec technique=multiple k=3 m=3 c=1",
    "plugin=shec technique=multiple k=4 m=3 c=1",
    "plugin=shec technique=multiple k=5 m=3 c=1",
    "plugin=shec technique=multiple k=6 m=3 c=1",
    "plugin=shec technique=multiple k=3 m=3 c=2",
    "plugin=shec technique=multiple k=4 m=3 c=2",
    "plugin=shec technique=multiple k=5 m=3 c=2",
    "plugin=shec technique=multiple k=6 m=3 c=2",
    "plugin=lrc mapping=_DD layers=[[\"cDD\",\"\"]]",
    "plugin=lrc mapping=_DDD layers=[[\"cDDD\",\"\"]]",
    "plugin=lrc mapping=_DDDD layers=[[\"cDDDD\",\"\"]]",
    "plugin=lrc mapping=_DDDDD layers=[[\"cDDDDD\",\"\"]]",
    "plugin=lrc mapping=_DDDDDD layers=[[\"cDDDDDD\",\"\"]]",
    "plugin=lrc mapping=_D_D layers=[[\"cDcD\",\"\"]]",
    "plugin=lrc mapping=_D_DD layers=[[\"cDcDD\",\"\"]]",
    "plugin=lrc mapping=_D_DDD layers=[[\"cDcDDD\",\"\"]]",
    "plugin=lrc mapping=_D_DDDD layers=[[\"cDcDDDD\",\"\"]]",
    "plugin=lrc mapping=_D_DDDDD layers=[[\"cDcDDDDD\",\"\"]]",
    "plugin=lrc mapping=_D_D_ layers=[[\"cDcDc\",\"\"]]",
    "plugin=lrc mapping=_D_D_D layers=[[\"cDcDcD\",\"\"]]",
    "plugin=lrc mapping=_D_D_DD layers=[[\"cDcDcDD\",\"\"]]",
    "plugin=lrc mapping=_D_D_DDD layers=[[\"cDcDcDDD\",\"\"]]",
    "plugin=lrc mapping=_D_D_DDDD layers=[[\"cDcDcDDDD\",\"\"]]",
    "plugin=jerasure technique=reed_sol_van k=6 m=3 w=16",
    "plugin=jerasure technique=reed_sol_van k=6 m=3 w=32",
    "plugin=jerasure technique=liberation k=6 m=2 packetsize=32 w=11",
    "plugin=jerasure technique=liberation k=6 m=2 packetsize=36 w=13",
    "plugin=jerasure technique=blaum_roth k=6 m=2 packetsize=44 w=7",
    "plugin=jerasure technique=blaum_roth k=6 m=2 packetsize=60 w=10",
    "plugin=jerasure technique=liber8tion k=2 m=2 packetsize=92"
  )
);

/*
 * Local Variables:
 * compile-command: "cd ../.. ; ninja &&
 *   ninja unittest_erasure_code_plugins &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugins \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
