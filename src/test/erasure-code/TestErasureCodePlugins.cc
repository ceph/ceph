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

  void generate_chunk(bufferlist& bl)
  {
    ceph::util::random_number_generator<char> random_generator = ceph::util::random_number_generator<char>();
    ceph::bufferptr b = buffer::create_aligned(chunk_size, 4096); //ceph::bufferptr(chunk_size);
    for (int i = 0; i < chunk_size; i++) {
      b[i] = random_generator();
    }
    bl.append(b);
  }

  void generate_chunk(bufferlist& bl, char c)
  {
    ceph::bufferptr b = buffer::create_aligned(chunk_size, 4096); //ceph::bufferptr(chunk_size);
    for (int i = 0; i < chunk_size; i++) {
      b[i] = c;
    }
    bl.append(b);
  }
};

TEST_P(PluginTest,Initialize)
{
  initialize();
}

TEST_P(PluginTest,PartialRead)
{
  initialize();
  set<int> want_to_encode;
  for (unsigned int i = 0 ; i < get_k_plus_m(); i++) {
    want_to_encode.insert(i);
  }
  // Test erasure code is systematic and that the data
  // order is described by get_chunk_mapping().
  //
  // Create a buffer and encode it. Compare the
  // encoded shards of data with the equivalent
  // range of the buffer.
  //
  // If there are no differences the plugin should
  // report that it supports PARTIAL_READ_OPTIMIZATION
  bufferlist bl;
  for (unsigned int i = 0; i < get_k(); i++) {
    generate_chunk(bl);
  }
  map<int,bufferlist> encoded;
  erasure_code->encode(want_to_encode, bl, &encoded);
  std::vector<int> chunk_mapping = erasure_code->get_chunk_mapping();
  bool different = false;
  for (unsigned int i = 0; i < get_k_plus_m(); i++) {
    EXPECT_EQ(chunk_size, encoded[i].length());
    unsigned int index = (chunk_mapping.size() > i) ? chunk_mapping[i] : i;
    if (i < get_k()) {
      bufferlist expects;
      expects.substr_of(bl, i * chunk_size, chunk_size);
      if (expects != encoded[index]) {
	different = true;
      }
    }
  }
  if (erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION) {
    // Plugin should not have PARTIAL_READ_OPTIMIZATION enabled, this
    // failure proves that it can cause a data integrity issue
    EXPECT_EQ(different, false);
  } else {
    // Very rare chance of a false positive because input buffers are random,
    // repeatedly hitting this failure means the plugin should be reporting
    // support for PARTIAL_READ_OPTIMIZAION
    EXPECT_EQ(different, true);
  }
}

TEST_P(PluginTest,PartialWrite)
{
  initialize();
  set<int> want_to_encode;
  for (unsigned int i = 0 ; i < get_k_plus_m(); i++) {
    want_to_encode.insert(i);
  }
  // Test erasure code can perform partial writes
  //
  // Create buffer 1 that consists of 3 randomly
  // generated chunks for each shard
  //
  // Create buffer 2 that has a different middle
  // chunk for each shard
  //
  // Create buffer 4 that just has the 1 different
  // middle chunk for each shard
  //
  // encoded the 3 buffers. Check if the first and
  // last chunk of encoded shard buffer 1 and 2 are
  // the same. Check if the midle chunk of encoded
  // shard buffer 2 is the same as encoded shard
  // buffer 3.
  //
  // If there are no differences the plugin should
  // report that it supports PARTIAL_WRITE_OPTIMIZATION
  bufferlist bl1;
  bufferlist bl2;
  bufferlist bl3;
  for (unsigned int i = 0; i < get_k(); i++) {
    bufferlist a1,a2,a3,b1,b2,b3,c2;
    generate_chunk(a1);
    generate_chunk(a2);
    generate_chunk(a3);
    b1 = a1;
    generate_chunk(b2);
    b3 = a3;
    c2 = b2;
    bl1.append(a1);
    bl1.append(a2);
    bl1.append(a3);
    bl2.append(b1);
    bl2.append(b2);
    bl2.append(b3);
    bl3.append(c2);
  }
  map<int,bufferlist> encoded1;
  erasure_code->encode(want_to_encode, bl1, &encoded1);
  map<int,bufferlist> encoded2;
  erasure_code->encode(want_to_encode, bl2, &encoded2);
  map<int,bufferlist> encoded3;
  erasure_code->encode(want_to_encode, bl3, &encoded3);
  bool different = false;
  for (unsigned int i = 0; i < get_k_plus_m(); i++) {
    EXPECT_EQ(chunk_size*3, encoded1[i].length());
    EXPECT_EQ(chunk_size*3, encoded2[i].length());
    EXPECT_EQ(chunk_size, encoded3[i].length());
    bufferlist a1,a2,a3,b1,b2,b3,c2;
    a1.substr_of(encoded1[i],0,chunk_size);
    a2.substr_of(encoded1[i],chunk_size,chunk_size);
    a3.substr_of(encoded1[i],chunk_size*2,chunk_size);
    b1.substr_of(encoded2[i],0,chunk_size);
    b2.substr_of(encoded2[i],chunk_size,chunk_size);
    b3.substr_of(encoded2[i],chunk_size*2,chunk_size);
    c2 = encoded3[i];
    if ((a1 != b1) || (a3 != b3) || (b2 != c2)) {
      different = true;
      std::cout << "plugin " << get_plugin() << " " << profile << " ";
      if (a1 != b1) {
	std::cout << "a1!=b1 ";
      }
      if (a3 != b3) {
	std::cout << "a3!=b3 ";
      }
      if (b2 != c2) {
	std::cout << "b2!=c2 ";
      }
      std::cout << std::endl;
    }
  }
  if (erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION) {
    // Plugin should not have PARTIAL_WRITE_OPTIMIZATION enabled, this
    // failure proves that it can cause a data integrity issue
    EXPECT_EQ(different, false);
  } else {
    // Very rare chance of a false positive because input buffers are random,
    // repeatedly hitting this failure means the plugin should be reporting
    // support for PARTIAL_WRITE_OPTIMIZAION
    EXPECT_EQ(different, true);
  }
}

TEST_P(PluginTest,ZeroInZeroOut)
{
  initialize();
  set<int> want_to_encode;
  for (unsigned int i = 0 ; i < get_k_plus_m(); i++) {
    want_to_encode.insert(i);
  }
  // Test erasure code generates zeros for coding parity if data chunks are zeros
  //
  // Create a buffer of all zeros and encode it, test if all the data and parity
  // chunks are all zeros.
  //
  // If there are no differences the plugin should
  // report that it supports ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION
  bufferlist bl;
  for (unsigned int i = 0; i < get_k(); i++) {
    generate_chunk(bl, 0);
  }
  map<int,bufferlist> encoded;
  erasure_code->encode(want_to_encode, bl, &encoded);
  bool different = false;
  bufferlist expects;
  generate_chunk(expects, 0);
  for (unsigned int i = 0; i < get_k_plus_m(); i++) {
    EXPECT_EQ(chunk_size, encoded[i].length());
    if (expects != encoded[i]) {
      different = true;
    }
  }
  if (erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION) {
    // Plugin should not have ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION enabled, this
    // failure proves that it can cause a data integrity issue
    EXPECT_EQ(different, false);
  } else {
    // Plugin should be supporting ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION
    EXPECT_EQ(different, true);
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
    "plugin=jerasure technique=reed_sol_van k=6 m=3 w=32"
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
