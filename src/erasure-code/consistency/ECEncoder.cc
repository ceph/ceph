#include "ECEncoder.h"
#include <typeinfo>
#include "common/errno.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;
using stripe_info_o_t = ECUtil::stripe_info_t;

namespace ceph {
namespace consistency {

template <typename SInfo>
ECEncoder<SInfo>::ECEncoder(ceph::ErasureCodeProfile profile, int chunk_size) :
  profile(profile),
  chunk_size(chunk_size)
{
  int r = ec_init_plugin(ec_impl);
  if (r < 0) {
    std::cerr << "Failed to initialize plugin: " << r << std::endl;
  }
  stripe_info = ec_init_sinfo(ec_impl);
}

/**
 * Initialize the ErasureCodeInterfaceRef needed to perform encode.
 *
 * @param ec_impl Pointer to plugin being initialized
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ECEncoder<SInfo>::ec_init_plugin(ceph::ErasureCodeInterfaceRef &ec_impl)
{
  auto plugin = profile.find("plugin");
  if (plugin == profile.end()) {
    std::cerr << "Invalid profile: plugin not specified." << std::endl;
    return 1;
  }

  std::stringstream ss;
  std::string dir = g_conf().get_val<std::string>("erasure_code_dir");
  ceph::ErasureCodePluginRegistry::instance().factory(plugin->second,
                                                      dir, profile,
                                                      &ec_impl, &ss);
  if (!ec_impl) {
    std::cerr << "Invalid profile: " << ss.str() << std::endl;
    return 1;
  }

  return 0;
}

/**
 * Initialize the stripe_info_t needed to perform encode. Optimized version for new EC.
 *
 * @param ec_impl Pointer to plugin object
 * @returns Unique pointer to the stripe info object associated with the EC profile
 */
template <>
std::unique_ptr<stripe_info_o_t> ECEncoder<stripe_info_o_t>::ec_init_sinfo(
  ceph::ErasureCodeInterfaceRef &ec_impl)
{
  uint64_t k = std::stol(profile["k"]);
  ceph_assert(k > 0);
  uint64_t stripe_width = k * chunk_size;
  std::unique_ptr<stripe_info_o_t> s(
    new stripe_info_o_t(ec_impl, nullptr, stripe_width));
  return s;
}

/**
 * Initialize the stripe_info_t needed to perform encode. Legacy version for old EC.
 *
 * @param ec_impl Pointer to plugin object
 * @returns Unique pointer to the stripe info object associated with the EC profile.
 */
template <>
std::unique_ptr<stripe_info_l_t> ECEncoder<stripe_info_l_t>::ec_init_sinfo(
  ceph::ErasureCodeInterfaceRef &ec_impl)
{
  uint64_t k = stol(profile["k"]);
  ceph_assert(k > 0);
  uint64_t stripe_width = k * chunk_size;
  std::unique_ptr<stripe_info_l_t> s(new stripe_info_l_t(ec_impl, stripe_width));
  return s;
}

/**
 * Perform encode on the input buffer and place result in the supplied output buffer.
 * Optimized EC encode function.
 *
 * @param inbl Buffer to be encoded
 * @returns Optional, returns buffer for the encode output if encode is successful
 */
template <>
std::optional<ceph::bufferlist> ECEncoder<stripe_info_o_t>::do_encode(ceph::bufferlist inbl,
                                                                      stripe_info_o_t &sinfo)
{
  ECUtil::shard_extent_map_t encoded_data(&sinfo);

  uint64_t stripe_width = sinfo.get_stripe_width();
  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  sinfo.ro_range_to_shard_extent_map(0, inbl.length(), inbl, encoded_data);
  encoded_data.insert_parity_buffers();
  int r = encoded_data.encode(ec_impl);
  if (r < 0) {
    std::cerr << "Failed to encode: " << cpp_strerror(r) << std::endl;
    return {};
  }

  ceph::bufferlist outbl;
  for (const auto &[shard, _] : encoded_data.get_extent_maps()) {
    if (shard >= sinfo.get_k()) {
      encoded_data.get_shard_first_buffer(shard, outbl);
    }
  }

  return outbl;
}

/**
 * Perform encode on the input buffer and place result in the supplied output buffer.
 * Legacy EC encode function.
 *
 * @param inbl Buffer to be encoded
 * @returns Optional, returns buffer for the encode output if encode is successful
 */
template <>
std::optional<ceph::bufferlist> ECEncoder<stripe_info_l_t>::do_encode(ceph::bufferlist inbl,
                                                                      stripe_info_l_t &sinfo)
{
  uint64_t stripe_width = sinfo.get_stripe_width();

  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  std::set<int> want;
  int k_plus_m = sinfo.get_k_plus_m();
  for (int i = 0; i < k_plus_m; i++) {
    want.insert(i);
  }

  std::map<int, ceph::bufferlist> encoded_data;
  int r = ECLegacy::ECUtilL::encode(sinfo, ec_impl, inbl, want, &encoded_data);
  if (r < 0) {
    std::cerr << "Failed to encode, rc: " << r << std::endl;
    return {};
  }

  ceph::bufferlist outbl;
  for (const auto &[shard, bl] : encoded_data) {
    unsigned int raw_shard = sinfo.get_raw_shard(shard);
    if (raw_shard >= sinfo.get_k()) {
      bufferlist::const_iterator it = bl.begin();
      it.copy_all(outbl);
    }
  }

  return outbl;
}

/**
 * Generic function which call either legacy or optimized version of encode.
 *
 * @param inbl Buffer to be encoded
 * @returns Optional, returns buffer for the encode output if encode is successful
 */
template <typename SInfo>
std::optional<ceph::bufferlist> ECEncoder<SInfo>::do_encode(ceph::bufferlist inbl)
{
  return do_encode(inbl, *(stripe_info.get()));
}

/**
 * Return data shard count for the stripe
 *
 * @returns int Number of data shards
 */
template <typename SInfo>
int ECEncoder<SInfo>::get_k()
{
  return stripe_info->get_k();
}

/**
 * Return parity shard count for the stripe
 *
 * @returns int Number of parity shards
 */
template <typename SInfo>
int ECEncoder<SInfo>::get_m()
{
  return stripe_info->get_m();
}
}
}

template class ceph::consistency::ECEncoder<stripe_info_l_t>;
template class ceph::consistency::ECEncoder<stripe_info_o_t>;
