#include "ECEncoder.h"
#include "common/errno.h"

#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

using ECEncoder = ceph::consistency::ECEncoder;
using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;

ECEncoder::ECEncoder(ceph::ErasureCodeProfile profile, int chunk_size) :
profile(profile),
chunk_size(chunk_size) {}

/**
 * Initialize the ErasureCodeInterfaceRef needed to perform encode.
 *
 * @param ec_impl Pointer to plugin being initialized
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::ec_init_plugin(ceph::ErasureCodeInterfaceRef *ec_impl)
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
                                                      ec_impl, &ss);
  if (!*ec_impl) {
    std::cerr << "Invalid profile: " << ss.str() << std::endl;
    return 1;
  }

  return 0;
}

/**
 * Initialize the stripe_info_t needed to perform encode. Optimized version for new EC.
 *
 * @param ec_impl Pointer to plugin object
 * @param sinfo Pointer to the stripe info object associated with the EC profile
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::ec_init_sinfo_optimized(ceph::ErasureCodeInterfaceRef *ec_impl,
                                       std::unique_ptr<ECUtil::stripe_info_t> *sinfo)
{
  uint64_t k = atoi(profile["k"].c_str());
  ceph_assert(k > 0);
  uint64_t stripe_width = k * chunk_size;
  sinfo->reset(new ECUtil::stripe_info_t(*ec_impl, nullptr, stripe_width));
  return 0;
}

/**
 * Initialize the stripe_info_t needed to perform encode. Legacy version for old EC.
 *
 * @param ec_impl Pointer to plugin object
 * @param sinfo Pointer to the stripe info object associated with the EC profile
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::ec_init_sinfo_legacy(ceph::ErasureCodeInterfaceRef *ec_impl,
                                    std::unique_ptr<stripe_info_l_t> *sinfo)
{
  uint64_t k = atoi(profile["k"].c_str());
  ceph_assert(k > 0);
  uint64_t stripe_width = k * chunk_size;
  sinfo->reset(new stripe_info_l_t(*ec_impl, stripe_width));
  return 0;
}

/**
 * Perform encode on the input buffer.
 * Wrapper function for optimized and legacy variants of encode.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::do_encode(ceph::bufferlist inbl,
  ceph::bufferlist &outbl,
  bool is_optimized)
{
  if (is_optimized)
    return do_encode_optimized(inbl, outbl);
  else
    return do_encode_legacy(inbl, outbl);
}

/**
 * Optimized EC encode function.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::do_encode_optimized(ceph::bufferlist inbl,
                                   ceph::bufferlist &outbl)
{
  std::unique_ptr<ECUtil::stripe_info_t> sinfo;
  ceph::ErasureCodeInterfaceRef ec_impl;
  int r = ec_init_plugin(&ec_impl);
  if (r < 0) {
    std::cerr << "Failed to initialize plugin: " << r << std::endl;
    return 1;
  }
  ec_init_sinfo_optimized(&ec_impl, &sinfo);
  ECUtil::shard_extent_map_t encoded_data(sinfo.get());

  uint64_t stripe_width = sinfo->get_stripe_width();
  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  sinfo->ro_range_to_shard_extent_map(0, inbl.length(), inbl, encoded_data);
  r = encoded_data.encode(ec_impl, nullptr, encoded_data.get_ro_end());
  if (r < 0) {
    std::cerr << "Failed to encode: " << cpp_strerror(r) << std::endl;
    return 1;
  }

  for (auto &[shard, _] : encoded_data.get_extent_maps()) {
    encoded_data.get_shard_first_buffer(shard, outbl);
  }

  return 0;
}

/**
 * Legacy EC encode function.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::do_encode_legacy(ceph::bufferlist inbl,
                                ceph::bufferlist &outbl)
{
  std::unique_ptr<stripe_info_l_t> sinfo;
  ceph::ErasureCodeInterfaceRef ec_impl;
  int r = ec_init_plugin(&ec_impl);
  if (r < 0) {
    std::cerr << "Failed to initialize plugin: " << r << std::endl;
    return 1;
  }
  ec_init_sinfo_legacy(&ec_impl, &sinfo);

  uint64_t stripe_width = sinfo->get_stripe_width();
  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  std::set<int> want;
  int k_plus_m = atoi(profile["k"].c_str()) + atoi(profile["m"].c_str());
  for (int i = 0; i < k_plus_m; i++) {
    want.insert(i);
  }

  std::map<int, ceph::bufferlist> encoded_data;
  r = ECLegacy::ECUtilL::encode(*sinfo, ec_impl, inbl, want, &encoded_data);
  if (r < 0) {
    std::cerr << "Failed to encode, rc: " << r << std::endl;
    return 1;
  }

  for (auto &[shard, bl] : encoded_data) {
    if (shard >= atoi(profile["k"].c_str()))
    {
      bufferlist::iterator it = bl.begin();
      it.copy_all(outbl);
    }
  }

  return 0;
}