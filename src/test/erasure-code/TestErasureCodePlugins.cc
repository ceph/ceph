// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph distributed storage system
 */
#include <errno.h>
#include <stdlib.h>

#include <array>
#include <map>
#include <set>

#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "common/config_proxy.h"
#include "include/random.h" // for ceph::util::random_number_generator
#include "gtest/gtest.h"
#include "include/buffer.h"
#include "osd/ECTypes.h"
#include "osd/ECUtil.h"

using namespace std;

// ---------------------------------------------------------------------------
// Minimal self-contained GF(2^8) multiply used by GFLinearHashSupport.
//
// Uses the same field as ISA-L / Intel's ec_init_tables: GF(2^8) with the
// primitive polynomial x^8 + x^4 + x^3 + x^2 + 1  (0x1d in Koopman
// notation, i.e. the "ISA-L field").  This matches the field used by the
// ISA reed_sol_van and cauchy plugins, and the Jerasure plugins that share
// the same w=8 field.  It does NOT need to match for the PoC to be
// structurally valid — the sketch linearity property holds over any
// consistent GF(2^8); what matters is that the same multiply is used when
// hashing data shards and when verifying parity shards.
// ---------------------------------------------------------------------------
static uint8_t gf8_mul_poc(uint8_t a, uint8_t b)
{
  uint8_t result = 0;
  while (b) {
    if (b & 1) result ^= a;
    // multiply a by x in GF(2^8); reduce modulo x^8+x^4+x^3+x^2+1 (0x1d)
    a = (a << 1) ^ (a & 0x80 ? 0x1d : 0);
    b >>= 1;
  }
  return result;
}
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
  void generate_chunk(bufferlist& bl)
  {
    ceph::util::random_number_generator<char> random_generator = ceph::util::random_number_generator<char>();
    ceph::bufferptr b = buffer::create_aligned(chunk_size, 4096);
    for (int i = 0; i < chunk_size; i++) {
      b[i] = random_generator();
    }
    bl.append(b);
  }
  void generate_chunk(bufferlist& bl, char c)
  {
    ceph::bufferptr b = buffer::create_aligned(chunk_size, 4096);
    for (int i = 0; i < chunk_size; i++) {
      b[i] = c;
    }
    bl.append(b);
  }
  uint32_t calculate_crc(const bufferlist& bl, int crc_seed) {
    bufferhash hash(crc_seed);
    hash << bl;
    return hash.digest();
  }
  uint32_t calculate_zero_buffer_crc(int crc_seed) {
    bufferlist zero_bl;
    generate_chunk(zero_bl, 0);
    return calculate_crc(zero_bl, crc_seed);
  }
  bufferptr create_buffer_from_crc(uint32_t crc) {
    std::size_t length = sizeof(crc);
    char crc_bytes[length];
    for (std::size_t i = 0; i < length; i++) {
      crc_bytes[i] = crc >> (8 * i) & 0xFF;
    }
    ceph::bufferptr buffer = ceph::buffer::create_aligned(chunk_size, 4096);
    buffer.zero(true);
    buffer.copy_in(0, length, crc_bytes);
    return buffer;
  }
  uint32_t read_crc_from_bufferlist(bufferlist& bl,
                                    uint64_t offset = 0) {
    uint32_t crc = 0;
    std::size_t length = sizeof(uint32_t);
    for (std::size_t i = 0; i < length; i++) {
      crc |= ((bl.c_str()[offset + i] & 0xFF) << (8 * i));
    }

    return crc;
  }

  // -------------------------------------------------------------------------
  // GF(2^8) linear sketch helpers
  //
  // The sketch of a shard is four independent GF(2^8) inner products, packed
  // into a uint32_t (one byte per lane).  Each lane l uses a deterministic
  // random vector r_l[i] derived by hashing the byte index and lane:
  //   r_l[i] = ((i * 6364136223846793005ULL + lane * 2891336453ULL) >> 33) & 0xFF
  // This is cheap, reproducible across all OSDs without coordination, and
  // gives four statistically independent lanes so that the combined false-
  // positive probability is 1/2^32 — matching CRC32c's collision resistance.
  //
  // Linearity: for any GF(2^8) scalar α and shard X,
  //   gf_hash(α·X) = α · gf_hash(X)   (lane-wise in GF(2^8))
  // This makes the sketch compatible with all EC generator matrix rows, not
  // just the XOR row (parity 0).
  // -------------------------------------------------------------------------
  static uint8_t gf_rand_coeff(int byte_index, int lane)
  {
    // Cheap deterministic hash; result is nonzero with overwhelming probability.
    uint64_t v = static_cast<uint64_t>(byte_index) * 6364136223846793005ULL
                 + static_cast<uint64_t>(lane) * 2891336453ULL
                 + 1442695040888963407ULL;
    uint8_t r = static_cast<uint8_t>((v >> 33) & 0xFF);
    return r ? r : 0x01; // avoid zero coefficient (degenerate sketch)
  }

  // Compute the 4-lane GF(2^8) inner-product sketch of a shard.
  // Returns a uint32_t with lane l in byte l (little-endian).
  uint32_t calculate_gf_hash(bufferlist bl) {
    std::array<uint8_t, 4> lanes = {0, 0, 0, 0};
    const char* data = bl.c_str();
    for (int i = 0; i < chunk_size; ++i) {
      uint8_t byte = static_cast<uint8_t>(data[i]);
      for (int lane = 0; lane < 4; ++lane) {
        lanes[lane] ^= gf8_mul_poc(gf_rand_coeff(i, lane), byte);
      }
    }
    uint32_t result = 0;
    for (int lane = 0; lane < 4; ++lane) {
      result |= (static_cast<uint32_t>(lanes[lane]) << (8 * lane));
    }
    return result;
  }

  // Pack a 4-byte GF hash into the first 4 bytes of a chunk-sized buffer
  // (identical layout to create_buffer_from_crc — reuses that helper directly).
};
TEST_P(PluginTest,Initialize)
{
  initialize();
}
TEST_P(PluginTest,PartialRead)
{
  initialize();
  shard_id_set want_to_encode;
  for (shard_id_t i; i < get_k_plus_m(); ++i) {
    want_to_encode.insert(i);
  }
  // Test erasure code is systematic and that the data order is described by
  // get_chunk_mapping().
  //
  // Create a buffer and encode it. Compare the encoded shards of data with the
  // equivalent range of the buffer.
  //
  // If there are no differences the plugin should report that it supports
  // PARTIAL_READ_OPTIMIZATION
  bufferlist bl;
  for (unsigned int i = 0; i < get_k(); i++) {
    generate_chunk(bl);
  }
  shard_id_map<bufferlist> encoded(get_k_plus_m());
  erasure_code->encode(want_to_encode, bl, &encoded);
  std::vector<shard_id_t> chunk_mapping = erasure_code->get_chunk_mapping();
  bool different = false;
  for (shard_id_t i; i < get_k_plus_m(); ++i) {
    EXPECT_EQ(chunk_size, encoded[i].length());
    shard_id_t index = (chunk_mapping.size() > i) ? chunk_mapping[int(i)] : i;
    if (i < get_k()) {
      bufferlist expects;
      expects.substr_of(bl, int(i) * chunk_size, chunk_size);
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
  shard_id_set want_to_encode;
  for (shard_id_t i; i < get_k_plus_m(); ++i) {
    want_to_encode.insert(i);
  }
  // Test erasure code can perform partial writes
  //
  // Create buffer 1 that consists of 3 randomly generated chunks for each shard
  //
  // Create buffer 2 that has a different middle chunk for each shard
  //
  // Create buffer 3 that just has the 1 different middle chunk for each shard
  //
  // encoded the 3 buffers. Check if the first and last chunk of encoded shard
  // buffer 1 and 2 are the same. Check if the midle chunk of encoded shard
  // buffer 2 is the same as encoded shard buffer 3.
  //
  // If there are no differences the plugin should report that it supports
  // PARTIAL_WRITE_OPTIMIZATION
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
  shard_id_map<bufferlist> encoded1(get_k_plus_m());
  erasure_code->encode(want_to_encode, bl1, &encoded1);
  shard_id_map<bufferlist> encoded2(get_k_plus_m());
  erasure_code->encode(want_to_encode, bl2, &encoded2);
  shard_id_map<bufferlist> encoded3(get_k_plus_m());
  erasure_code->encode(want_to_encode, bl3, &encoded3);
  bool different = false;
  for (shard_id_t i; i < get_k_plus_m(); ++i) {
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
  shard_id_set want_to_encode;
  for (shard_id_t i; i < get_k_plus_m(); ++i) {
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
  shard_id_map<bufferlist> encoded(get_k_plus_m());
  erasure_code->encode(want_to_encode, bl, &encoded);
  bool different = false;
  bufferlist expects;
  generate_chunk(expects, 0);
  for (shard_id_t i; i < get_k_plus_m(); ++i) {
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
    GTEST_SKIP() << "ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION not supported"
      " but test indicates support is possible for this configuration";
  }
}
TEST_P(PluginTest,ParityDelta_SingleDeltaSingleParity)
{
  // Test erasure code plugin can perform parity delta writes
  // to a single parity chunk using a single delta.
  //
  // 1. Create a buffer of random chunks and do a full stripe write.
  // 2. Generate a new chunk to replace one of the original data chunks.
  // 3. Test that EncodeDelta generates the expected delta when given the
  //    original data chunk and the new data chunk.
  // 4. Do a second full write with the new chunk.
  // 5. Test that ApplyDelta correctly applies the delta to the original parity
  //    chunk and returns the same new parity chunk as the second full write.
  initialize();
  if (!(erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION)) {
        GTEST_SKIP() << "Plugin does not support parity delta optimization";
  }
  shard_id_set want_to_encode;
  for (shard_id_t i ; i < get_k_plus_m(); ++i) {
    want_to_encode.insert(i);
  }
  bufferlist old_bl;
  for (unsigned int i = 0; i < get_k(); ++i) {
    generate_chunk(old_bl);
  }
  shard_id_map<bufferlist> old_encoded(get_k_plus_m());
  erasure_code->encode(want_to_encode, old_bl, &old_encoded);
  
  bufferlist new_chunk_bl;
  generate_chunk(new_chunk_bl);

  random_device rand;
  mt19937 gen(rand());
  uniform_int_distribution<> chunk_range(0, get_k()-1);
  shard_id_t random_chunk(chunk_range(gen));

  ceph::bufferptr old_data = buffer::create_aligned(chunk_size, 4096);
  old_bl.begin(int(random_chunk) * chunk_size).copy(chunk_size, old_data.c_str());
  ceph::bufferptr new_data = new_chunk_bl.front();
  ceph::bufferptr delta = buffer::create_aligned(chunk_size, 4096);
  ceph::bufferptr expected_delta = buffer::create_aligned(chunk_size, 4096);

  for (int i = 0; i < chunk_size; i++) {
    expected_delta.c_str()[i] = old_data.c_str()[i] ^ new_data.c_str()[i];
  }

  erasure_code->encode_delta(old_data, new_data, &delta);

  bool delta_matches = true;
  for (int i = 0; i < chunk_size; i++) {
    if (expected_delta.c_str()[i] != delta.c_str()[i]) {
      delta_matches = false;
    }
  }
  EXPECT_EQ(delta_matches, true);

  uniform_int_distribution<> parity_range(get_k(), get_k_plus_m()-1);
  shard_id_t random_parity(parity_range(gen));
  ceph::bufferptr old_parity = buffer::create_aligned(chunk_size, 4096);
  old_encoded[random_parity].begin(0).copy(chunk_size, old_parity.c_str());

  shard_id_map<bufferlist> new_encoded(get_k_plus_m());
  bufferlist new_bl;
  for (auto i = old_encoded.begin(); i != old_encoded.end(); i++) {
    if ((unsigned int)i->first >= get_k()) {
      continue;
    }
    if (i->first == random_chunk) {
      new_bl.append(new_data);
    } 
    else {
      new_bl.append(i->second);
    }
  }

  erasure_code->encode(want_to_encode, new_bl, &new_encoded);
  ceph::bufferptr expected_parity = buffer::create_aligned(chunk_size, 4096);
  new_encoded[random_parity].begin().copy_deep(chunk_size, expected_parity);

  shard_id_map<bufferptr> in_map(get_k_plus_m());
  in_map[random_chunk] = delta;
  in_map[random_parity] = old_parity;
  shard_id_map<bufferptr> out_map(get_k_plus_m());
  out_map[random_parity] = old_parity;
  erasure_code->apply_delta(in_map, out_map);

  bool parity_matches = true;
  for (int i = 0; i < chunk_size; i++) {
    if (out_map[random_parity].c_str()[i] != expected_parity.c_str()[i]) {
      parity_matches = false;
    }
  }
  EXPECT_EQ(parity_matches, true);
}
TEST_P(PluginTest,ParityDelta_MultipleDeltaMultipleParity)
{
  // Test erasure code plugin can perform parity delta writes
  // to all parity chunks with deltas for all data chunks.
  //
  // 1. Create a buffer of random chunks and do a full write.
  // 2. Create a second buffer of random chunks and do a full write.
  // 3. Calculate the deltas between all of the chunks using xor.
  // 4. Test that EncodeDelta generates the expected delta when given the
  //    original data chunks and the new data chunks.
  // 5. Create an in map that contains every data delta and every parity chunk
  //    from the first full write. Test that ApplyDelta applies every delta to
  //    every parity, and returns an out map containing the same parity 
  //    chunks that were generated by the second full stripe write.
  initialize();
  if (!(erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION)) {
        GTEST_SKIP() << "Plugin does not support parity delta optimization";
  }
  shard_id_set want_to_encode;
  for (shard_id_t i ; i < get_k_plus_m(); ++i) {
    want_to_encode.insert(i);
  }

  bufferlist old_bl;
  for (unsigned int i = 0; i < get_k(); i++) {
    generate_chunk(old_bl);
  }
  shard_id_map<bufferlist> old_encoded(get_k_plus_m());
  erasure_code->encode(want_to_encode, old_bl, &old_encoded);
  
  bufferlist new_bl;
  for (unsigned int i = 0; i < get_k(); i++) {
    generate_chunk(new_bl);
  }
  shard_id_map<bufferlist> new_encoded(get_k_plus_m());
  erasure_code->encode(want_to_encode, new_bl, &new_encoded);

  ceph::bufferptr old_data = buffer::create_aligned(chunk_size*get_k(), 4096);
  ceph::bufferptr new_data = buffer::create_aligned(chunk_size*get_k(), 4096);
  ceph::bufferptr delta = buffer::create_aligned(chunk_size*get_k(), 4096);
  ceph::bufferptr expected_delta = buffer::create_aligned(chunk_size*get_k(), 4096);

  old_bl.begin().copy(chunk_size*get_k(), old_data.c_str());
  new_bl.begin().copy(chunk_size*get_k(), new_data.c_str());

  for (unsigned int i = 0; i < chunk_size*get_k() ; i++) {
    expected_delta.c_str()[i] = old_bl.c_str()[i] ^ new_bl.c_str()[i];
  }

  erasure_code->encode_delta(old_data, new_data, &delta);

  bool delta_matches = true;
  for (unsigned int i = 0; i < chunk_size * get_k(); i++) {
    if (expected_delta.c_str()[i] != delta.c_str()[i]) {
      delta_matches = false;
    }
  }
  EXPECT_EQ(delta_matches, true);

  shard_id_map<bufferptr> in_map(get_k_plus_m());
  shard_id_map<bufferptr> out_map(get_k_plus_m());
  for (shard_id_t i; i < get_k(); ++i) {
    ceph::bufferptr tmp = buffer::create_aligned(chunk_size, 4096);
    delta.copy_out(chunk_size * int(i), chunk_size, tmp.c_str());
    in_map[i] = tmp;
  }
  for (shard_id_t i(get_k()); i < get_k_plus_m(); ++i) {
    ceph::bufferptr tmp = buffer::create_aligned(chunk_size, 4096);
    old_encoded[i].begin().copy(chunk_size, tmp.c_str());
    in_map[i] = tmp;
    out_map[i] = tmp;
  }

  erasure_code->apply_delta(in_map, out_map);

  bool parity_matches = true;

  for (shard_id_t i(get_k()); i < get_k_plus_m(); ++i) {
    for (int j = 0; j < chunk_size; j++) {
      if (out_map[i].c_str()[j] != new_encoded[i].c_str()[j]) {
        parity_matches = false;
      }
    }
  }
  EXPECT_EQ(parity_matches, true);
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
TEST_P(PluginTest,SubChunkSupport)
{
  initialize();

  /* If any configurations of the plugin support !=1 sub chunk, then sub-chunk
   * support must be enabled.  Setting the flag unnecessarily is not-ideal, but
   * is a performance penalty.
   */
  if (erasure_code->get_sub_chunk_count() != 1) {
    ASSERT_TRUE((erasure_code->get_supported_optimizations() &
        ErasureCodeInterface::FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS) != 0);
  }
}
TEST_P(PluginTest, CRCEncodeDecodeSupport) {
  initialize();

  shard_id_set want_to_encode;
  for (shard_id_t i = shard_id_t(0); i < get_k_plus_m(); ++i) {
    want_to_encode.insert(i);
  }
  // Generate random data to encode
  bufferlist data_bl;
  for (unsigned int i = 0; i < get_k(); ++i) {
    generate_chunk(data_bl);
  }

  int crc_seed = -1;
  uint32_t zero_data_crc = calculate_zero_buffer_crc(crc_seed);

  // Calculate CRCs for the random data
  bufferlist hashes_bl;
  bufferlist unseeded_hashes_bl;
  for (unsigned int i = 0; i < get_k(); ++i) {
    // Calculate the CRC for the shard at position i
    bufferlist data_shard_bl;
    data_shard_bl.substr_of(data_bl, i * chunk_size, chunk_size);
    uint32_t crc = calculate_crc(data_shard_bl, crc_seed);

    // XOR with the CRC of zeros preseeded with the same preseed
    // This undoes the pre-seeding and gives us the CRC as if no seed was
    // applied
    uint32_t unseeded_crc = crc ^ zero_data_crc;

    // Convert integers to bufferlists
    hashes_bl.append(create_buffer_from_crc(crc));
    unseeded_hashes_bl.append(create_buffer_from_crc(unseeded_crc));
  }

  // Encode data and get back data + parity chunks
  shard_id_map<bufferlist> encoded_data(get_k_plus_m());
  erasure_code->encode(want_to_encode, data_bl, &encoded_data);

  // Encode CRCs to get back the data + parity CRCs
  shard_id_map<bufferlist> encoded_hashes(get_k_plus_m());
  shard_id_map<bufferlist> encoded_unseeded_hashes(get_k_plus_m());
  erasure_code->encode(want_to_encode, hashes_bl, &encoded_hashes);
  erasure_code->encode(want_to_encode, unseeded_hashes_bl,
                       &encoded_unseeded_hashes);

  shard_id_map<bufferlist> encoded_data_crcs(get_k_plus_m());

  // Calculate CRCs for the new data and new CRCs and compare first parity shard
  bool different = false;
  for (shard_id_t shard_id : want_to_encode) {
    // Calculations vary for additional parities, so only first parity supported
    if (shard_id < get_k() + 1) {
      // Calculate the CRC for the current shard from the encoded data
      uint32_t calculated_crc =
          calculate_crc(encoded_data.at(shard_id), crc_seed);
      encoded_data_crcs[shard_id].append(
          create_buffer_from_crc(calculated_crc));

      // XOR with the CRC of zeros preseeded with the same preseed
      // This undoes the pre-seeding and gives us the CRC as if no seed was
      // applied
      uint32_t calculated_unseeded_crc = calculated_crc ^ zero_data_crc;

      // Calculate integer form of CRCs in bufferlists
      uint32_t unseeded_crc =
          read_crc_from_bufferlist(encoded_unseeded_hashes.at(shard_id));

      if (calculated_unseeded_crc != unseeded_crc) {
        different = true;
      }
    }

    ECUtil::stripe_info_t sinfo{get_k(), get_m(), get_k() * chunk_size,
                                erasure_code->get_chunk_mapping()};

    // Decode CRCs as if 1 to m-1 data CRCs are missing and assert decoded CRC
    // is equal to missing CRC
    for (raw_shard_id_t missing_raw_shard_id{0}; missing_raw_shard_id < get_k();
         ++missing_raw_shard_id) {
      shard_id_t missing_shard_id = sinfo.get_shard(missing_raw_shard_id);

      shard_id_set need;
      need.insert(missing_shard_id);
      shard_id_map<bufferlist> chunks(get_k_plus_m());
      // Create a map of all buffers except the one (our missing shard)
      for (raw_shard_id_t raw_shard_id{0}; raw_shard_id < get_k_plus_m();
           ++raw_shard_id) {
        shard_id_t shard_id = sinfo.get_shard(raw_shard_id);

        if (shard_id != missing_shard_id) {
          chunks.insert(shard_id, encoded_hashes[shard_id]);
        }
      }

      // Decode the missing shard
      shard_id_map<bufferlist> out_bls(get_k_plus_m());
      int r = erasure_code->decode(need, chunks, &out_bls, chunk_size);

      EXPECT_EQ(r, 0);

      // Check the missing shard has been decoded correctly
      uint32_t decoded_crc =
          read_crc_from_bufferlist(out_bls[missing_shard_id]);
      uint32_t original_crc =
          read_crc_from_bufferlist(hashes_bl, missing_raw_shard_id.id * chunk_size);

      different = different | (decoded_crc != original_crc);
    }
  }

  if (erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_CRC_ENCODE_DECODE_SUPPORT) {
    // Plugin should not have FLAG_EC_PLUGIN_CRC_ENCODE_DECODE_SUPPORT enabled,
    // this failure proves that it can cause a data integrity issue
    EXPECT_EQ(different, false);
  }
}

// ---------------------------------------------------------------------------
// GFLinearHashSupport
//
// This is a proof-of-concept test for the GF(2^8) inner-product sketch
// described in the EC scrub analysis.  It mirrors CRCEncodeDecodeSupport
// exactly in structure, but swaps calculate_crc for calculate_gf_hash.
//
// Key differences from the CRC test:
//
//  1. ALL parity shards are checked (not just parity 0).  CRC32c only works
//     for parity 0 because that row's GF coefficients are all 0x01 (XOR).
//     The GF sketch satisfies H(α·X) = α·H(X) so it is correct for every
//     parity row regardless of its generator matrix coefficients.
//
//  2. There is no "unseeding" step.  CRC32c requires XOR-ing out the effect
//     of the initial seed; GF inner products start at 0 and need no
//     correction.
//
//  3. The test asserts EXPECT_EQ(different, false) unconditionally for all
//     plugins that have m > 1, confirming the sketch works where CRC cannot.
//     For plugins with m == 1 the result is identical to what CRC achieves.
//
//  4. A companion negative check confirms CRC32c *does* fail for parity
//     shards beyond the first (different_crc == true) when m > 1, making
//     it explicit that the two mechanisms diverge exactly there.
// ---------------------------------------------------------------------------
TEST_P(PluginTest, GFLinearHashSupport) {
  initialize();

  // Skip plugins that do not operate as pure GF(2^8)-per-byte linear codes.
  // The PoC hardcodes the ISA-L GF(2^8) field (primitive poly 0x1d).  Any
  // plugin that uses:
  //   - a different word size (w != 8): GF(2^16) or GF(2^32) encoding
  //   - packet/bit-matrix encoding (packetsize key present): liberation,
  //     blaum_roth, liber8tion, cauchy_orig, cauchy_good with m>1 — these
  //     codes operate over GF(2) bit-matrices at packet granularity, so
  //     byte-level GF(2^8) inner products are not the right abstraction
  //   - sub-chunks (clay): different encode input layout
  // will not satisfy the byte-level GF(2^8) linearity property that this
  // test verifies.  The correct production solution would have each plugin
  // expose its own scalar multiply, but for this PoC we skip non-GF(2^8)
  // byte-per-byte codecs.
  if (erasure_code->get_supported_optimizations() &
      ErasureCodeInterface::FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS) {
    GTEST_SKIP() << "Sub-chunk plugin; skipping GF hash PoC";
  }
  if (profile.count("packetsize")) {
    GTEST_SKIP() << "Packet/bit-matrix codec (packetsize="
                 << profile.at("packetsize")
                 << "); GF(2^8) byte-level sketch not applicable";
  }
  if (profile.count("w") && profile.at("w") != "8") {
    GTEST_SKIP() << "Non-byte-aligned word size w=" << profile.at("w")
                 << "; GF(2^8) byte-level sketch not applicable";
  }
  if (get_plugin() == "lrc") {
    GTEST_SKIP() << "LRC is a composite multi-layer code whose parity "
                    "structure depends on layout; skipping GF hash PoC";
  }

  shard_id_set want_to_encode;
  for (shard_id_t i = shard_id_t(0); i < get_k_plus_m(); ++i) {
    want_to_encode.insert(i);
  }

  // ---- Step 1: generate random data and encode it fully ------------------
  bufferlist data_bl;
  for (unsigned int i = 0; i < get_k(); ++i) {
    generate_chunk(data_bl);
  }
  shard_id_map<bufferlist> encoded_data(get_k_plus_m());
  erasure_code->encode(want_to_encode, data_bl, &encoded_data);

  // ---- Step 2: compute GF hash for each data shard -----------------------
  // Pack each 4-byte hash into a chunk-sized buffer (first 4 bytes used,
  // remainder zero) — same layout as create_buffer_from_crc.
  bufferlist gf_hashes_bl;
  for (unsigned int i = 0; i < get_k(); ++i) {
    bufferlist shard_bl;
    shard_bl.substr_of(data_bl, i * chunk_size, chunk_size);
    uint32_t h = calculate_gf_hash(shard_bl);
    gf_hashes_bl.append(create_buffer_from_crc(h));
  }

  // ---- Step 3: encode the GF hashes through the EC plugin ----------------
  // Because the sketch is GF(2^8)-linear, encoding the hash values produces
  // the predicted hash for each parity shard.
  shard_id_map<bufferlist> encoded_gf_hashes(get_k_plus_m());
  erasure_code->encode(want_to_encode, gf_hashes_bl, &encoded_gf_hashes);

  // ---- Step 4: check every parity shard ----------------------------------
  // For each parity shard j:
  //   predicted hash  = first 4 bytes of encoded_gf_hashes[j]
  //   actual hash     = calculate_gf_hash(encoded_data[j])
  // They must be equal for the sketch to be usable in EC scrub.
  bool different_gf = false;
  // Track whether CRC fails for shards beyond parity 0 (negative control).
  bool crc_fails_higher_parity = false;
  int crc_seed = -1;
  uint32_t zero_data_crc = calculate_zero_buffer_crc(crc_seed);

  for (shard_id_t shard_id(get_k()); shard_id < get_k_plus_m(); ++shard_id) {
    // --- GF hash check ---
    uint32_t predicted_gf = read_crc_from_bufferlist(encoded_gf_hashes.at(shard_id));
    uint32_t actual_gf    = calculate_gf_hash(encoded_data.at(shard_id));
    if (predicted_gf != actual_gf) {
      different_gf = true;
      ADD_FAILURE() << "GF hash mismatch on parity shard " << shard_id
                    << ": predicted 0x" << std::hex << predicted_gf
                    << " actual 0x"     << actual_gf << std::dec;
    }

    // --- CRC negative control: show CRC fails for parity shards beyond 0 ---
    if (shard_id > shard_id_t(get_k())) {
      // For parity 1 and above, CRC encode of data CRCs will not match the
      // CRC of the actual encoded data (because GF coefficients != 0x01).
      // Verify that this mismatch exists so the test documents the contrast.
      bufferlist crc_hashes_bl;
      for (unsigned int i = 0; i < get_k(); ++i) {
        bufferlist shard_bl;
        shard_bl.substr_of(data_bl, i * chunk_size, chunk_size);
        uint32_t crc        = calculate_crc(shard_bl, crc_seed);
        uint32_t unseeded   = crc ^ zero_data_crc;
        crc_hashes_bl.append(create_buffer_from_crc(unseeded));
      }
      shard_id_map<bufferlist> encoded_crc_hashes(get_k_plus_m());
      erasure_code->encode(want_to_encode, crc_hashes_bl, &encoded_crc_hashes);

      uint32_t crc_predicted_unseeded =
          read_crc_from_bufferlist(encoded_crc_hashes.at(shard_id));
      uint32_t crc_actual_unseeded =
          calculate_crc(encoded_data.at(shard_id), crc_seed) ^ zero_data_crc;

      if (crc_predicted_unseeded != crc_actual_unseeded) {
        crc_fails_higher_parity = true;
      }
    }
  }

  // GF sketch must be correct for every parity shard.
  EXPECT_EQ(different_gf, false);

  // For configurations with more than one parity shard, CRC encode must fail
  // on at least one higher parity (this is the whole motivation for the GF
  // approach).  If this assertion fires it means CRC accidentally worked,
  // which would be a remarkable (and suspicious) result worth investigating.
  if (get_m() > 1) {
    EXPECT_EQ(crc_fails_higher_parity, true)
        << "CRC unexpectedly matched all higher parity shards for this "
           "configuration — verify the test data was not all-zero";
  }

  // ---- Step 5: GF hash decode round-trip for all data shards -------------
  // Mirror the CRC decode sub-check: remove one data shard's GF hash and
  // verify the plugin can reconstruct it from the remaining shards + parity.
  ECUtil::stripe_info_t sinfo{get_k(), get_m(), get_k() * chunk_size,
                               erasure_code->get_chunk_mapping()};
  for (raw_shard_id_t missing_raw{0}; missing_raw < get_k(); ++missing_raw) {
    shard_id_t missing_shard = sinfo.get_shard(missing_raw);

    shard_id_set need;
    need.insert(missing_shard);
    shard_id_map<bufferlist> chunks(get_k_plus_m());
    for (raw_shard_id_t raw{0}; raw < get_k_plus_m(); ++raw) {
      shard_id_t sid = sinfo.get_shard(raw);
      if (sid != missing_shard) {
        chunks.insert(sid, encoded_gf_hashes[sid]);
      }
    }

    shard_id_map<bufferlist> out_bls(get_k_plus_m());
    int r = erasure_code->decode(need, chunks, &out_bls, chunk_size);
    EXPECT_EQ(r, 0);

    uint32_t decoded_hash  = read_crc_from_bufferlist(out_bls[missing_shard]);
    uint32_t original_hash = read_crc_from_bufferlist(
        gf_hashes_bl, missing_raw.id * chunk_size);

    EXPECT_EQ(decoded_hash, original_hash)
        << "GF hash decode round-trip failed for data shard " << missing_shard;
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
    // Disabling clay for now.  Needs more testing with optimized EC.
    // "plugin=clay k=2 m=1",
    // "plugin=clay k=3 m=1",
    // "plugin=clay k=4 m=1",
    // "plugin=clay k=5 m=1",
    // "plugin=clay k=6 m=1",
    // "plugin=clay k=2 m=2",
    // "plugin=clay k=3 m=2",
    // "plugin=clay k=4 m=2",
    // "plugin=clay k=5 m=2",
    // "plugin=clay k=6 m=2",
    // "plugin=clay k=2 m=3",
    // "plugin=clay k=3 m=3",
    // "plugin=clay k=4 m=3",
    // "plugin=clay k=5 m=3",
    // "plugin=clay k=6 m=3",
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
