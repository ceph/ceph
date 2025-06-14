#include <typeinfo>
#include "ECEncoder.h"
#include "common/errno.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

template class ceph::consistency::ECEncoder<stripe_info_l_t>;
template class ceph::consistency::ECEncoder<ECUtil::stripe_info_t>;

using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;

template <typename SInfo>
ceph::consistency::ECEncoder<SInfo>::ECEncoder(ceph::ErasureCodeProfile profile, int chunk_size) :
profile(profile),
chunk_size(chunk_size)
{
  int r = ec_init_plugin(&ec_impl);
  if (r < 0) {
    std::cerr << "Failed to initialize plugin: " << r << std::endl;
  }
  ec_init_sinfo(&ec_impl, &stripe_info);
}

/**
 * Initialize the ErasureCodeInterfaceRef needed to perform encode.
 *
 * @param ec_impl Pointer to plugin being initialized
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::ec_init_plugin(ceph::ErasureCodeInterfaceRef *ec_impl)
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
 * @param s Pointer to the stripe info object associated with the EC profile
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::ec_init_sinfo(ceph::ErasureCodeInterfaceRef *ec_impl,
                                                       std::unique_ptr<ECUtil::stripe_info_t> *s)
{
  uint64_t k = atoi(profile["k"].c_str());
  ceph_assert(k > 0);
  uint64_t stripe_width = k * chunk_size;
  s->reset(new ECUtil::stripe_info_t(*ec_impl, nullptr, stripe_width));
  return 0;
}

/**
 * Initialize the stripe_info_t needed to perform encode. Legacy version for old EC.
 *
 * @param ec_impl Pointer to plugin object
 * @param s Pointer to the stripe info object associated with the EC profile
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::ec_init_sinfo(ceph::ErasureCodeInterfaceRef *ec_impl,
                                                       std::unique_ptr<stripe_info_l_t> *s)
{
  uint64_t k = atoi(profile["k"].c_str());
  ceph_assert(k > 0);
  uint64_t stripe_width = k * chunk_size;
  s->reset(new stripe_info_l_t(*ec_impl, stripe_width));
  return 0;
}

/**
 * Perform encode on the input buffer and place result in the supplied output buffer.
 * Optimized EC encode function.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::do_encode(ceph::bufferlist inbl,
                                                   ceph::bufferlist &outbl,
                                                   std::unique_ptr<ECUtil::stripe_info_t> *s)
{
  ECUtil::stripe_info_t *sinfo = s->get();
  ECUtil::shard_extent_map_t encoded_data(sinfo);

  uint64_t stripe_width = sinfo->get_stripe_width();
  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  sinfo->ro_range_to_shard_extent_map(0, inbl.length(), inbl, encoded_data);
  encoded_data.insert_parity_buffers();
  int r = encoded_data.encode(ec_impl, nullptr, encoded_data.get_ro_end());
  if (r < 0) {
    std::cerr << "Failed to encode: " << cpp_strerror(r) << std::endl;
    return 1;
  }

  for (auto &[shard, _] : encoded_data.get_extent_maps()) {
    if (shard >= sinfo->get_k()) {
      encoded_data.get_shard_first_buffer(shard, outbl);
    }
  }

  return 0;
}

/**
 *
 * Generic function which call either legacy or optimized version of encode.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::do_encode(ceph::bufferlist inbl,
                                                   ceph::bufferlist &outbl)
{
  return do_encode(inbl, outbl, &stripe_info);
}

/**
 * Perform encode on the input buffer and place result in the supplied output buffer.
 * Legacy EC encode function.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::do_encode(ceph::bufferlist inbl,
                                                   ceph::bufferlist &outbl,
                                                   std::unique_ptr<stripe_info_l_t> *s)
{
  stripe_info_l_t *sinfo = s->get();
  uint64_t stripe_width = sinfo->get_stripe_width();

  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  std::set<int> want;
  int k_plus_m = sinfo->get_k_plus_m();
  for (int i = 0; i < k_plus_m; i++) {
    want.insert(i);
  }

  std::map<int, ceph::bufferlist> encoded_data;
  int r = ECLegacy::ECUtilL::encode(*sinfo, ec_impl, inbl, want, &encoded_data);
  if (r < 0) {
    std::cerr << "Failed to encode, rc: " << r << std::endl;
    return 1;
  }

  for (auto &[shard, bl] : encoded_data) {
    unsigned int raw_shard = sinfo->get_raw_shard(shard);
    if (raw_shard >= sinfo->get_k()) {
      bufferlist::iterator it = bl.begin();
      it.copy_all(outbl);
    }
  }

  return 0;
}

/**
 * Return data shard count for the stripe
 *
 * @returns int Number of data shards
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::get_k()
{
  return stripe_info->get_k();
}

/**
 * Return parity shard count for the stripe
 *
 * @returns int Number of parity shards
 */
template <typename SInfo>
int ceph::consistency::ECEncoder<SInfo>::get_m()
{
  return stripe_info->get_m();
}