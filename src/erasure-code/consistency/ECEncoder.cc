#include "ECEncoder.h"
#include "osd/ECUtilL.h"

using ECEncoder = ceph::consistency::ECEncoder;

ECEncoder::ECEncoder(ceph::ErasureCodeProfile profile, int stripe_unit) :
profile(profile),
stripe_unit(stripe_unit)
{
}

/**
 * Initialize the ErasureCodeInterfaceRef and stripe_info_t needed to perform encode.
 * for the specified object and pool. Assert on failure.
 *
 * @param stripe_unit Size of each shard in the stripe
 * @param ec_impl Pointer to plugin being initialized
 * @param sinfo Pointer to the stripe info object associated with the EC profile
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::ec_init(int stripe_unit,
                       ceph::ErasureCodeInterfaceRef *ec_impl,
                       std::unique_ptr<ECLegacy::ECUtilL::stripe_info_t> *sinfo)
{
  auto plugin = profile.find("plugin");
  if (plugin == profile.end()) {
    std::cerr << "Invalid profile: plugin not specified." << std::endl;
    return 1;
  }

  std::stringstream ss;
  std::string dir = g_conf().get_val<std::string>("erasure_code_dir");
  ceph::ErasureCodePluginRegistry::instance().factory(plugin->second, dir, profile, ec_impl, &ss);
  if (!*ec_impl) {
    std::cerr << "Invalid profile: " << ss.str() << std::endl;
    return 1;
  }

  if (sinfo == nullptr) {
    return 0;
  }

  uint64_t stripe_size = atoi(profile["k"].c_str());
  ceph_assert(stripe_size > 0);
  uint64_t stripe_width = stripe_size * stripe_unit;
  sinfo->reset(new ECLegacy::ECUtilL::stripe_info_t(*ec_impl, stripe_width));

  return 0;
  }

/**
 * Perform encode on the input buffer.
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoder::do_encode(ceph::bufferlist inbl,
                         ceph::bufferlist &outbl)
{
  std::unique_ptr<ECLegacy::ECUtilL::stripe_info_t> sinfo;
  ceph::ErasureCodeInterfaceRef ec_impl;
  ec_init(stripe_unit, &ec_impl, &sinfo);

  uint64_t stripe_width = (*sinfo).get_stripe_width();
  if (inbl.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - inbl.length() % stripe_width;
    inbl.append_zero(pad);
  }

  std::set<int> want;
  int shard_count = atoi(profile["k"].c_str()) + atoi(profile["m"].c_str());
  for (int i=0; i<shard_count; i++)
    want.insert(i);

  std::map<int, ceph::bufferlist> encoded_data;
  int rc = ECLegacy::ECUtilL::encode(*sinfo, ec_impl, inbl, want, &encoded_data);
  if (rc < 0) {
    std::cerr << "Failed to encode, rc: " << rc << std::endl;
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