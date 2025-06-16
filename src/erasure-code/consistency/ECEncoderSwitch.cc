
#include "ECEncoder.h"
#include "ECEncoderSwitch.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;
using ECEncoderSwitch = ceph::consistency::ECEncoderSwitch;

ECEncoderSwitch::ECEncoderSwitch(ceph::ErasureCodeProfile profile,
                                 int stripe_unit,
                                 bool optimizations_enabled) :
  encoder_optimized(ceph::consistency::ECEncoder<ECUtil::stripe_info_t>(profile, stripe_unit)),
  encoder_legacy(ceph::consistency::ECEncoder<stripe_info_l_t>(profile, stripe_unit)),
  optimizations_enabled(optimizations_enabled) {}

/**
 *
 * Generic function which call either legacy or optimized version of encode
 * from the correct version of the encoder
 *
 * @param inbl Buffer to be encoded
 * @param outbl Buffer for the encode output
 * @returns int 0 if successful, otherwise 1
 */
int ECEncoderSwitch::do_encode(ceph::bufferlist inbl, ceph::bufferlist &outbl)
{
  if (optimizations_enabled) {
    return encoder_optimized.do_encode(inbl, outbl);
  } else {
    return encoder_legacy.do_encode(inbl, outbl);
  }
}

/**
 * Return data shard count for the stripe from the correct version of the encoder
 *
 * @returns int Number of data shards
 */
int ECEncoderSwitch::get_k()
{
  if (optimizations_enabled) {
    return encoder_optimized.get_k();
  } else {
    return encoder_legacy.get_k();
  }
}

/**
 * Return parity shard count for the stripe from the correct version of the encoder
 *
 * @returns int Number of parity shards
 */
int ECEncoderSwitch::get_m()
{
  if (optimizations_enabled) {
    return encoder_optimized.get_m();
  } else {
    return encoder_legacy.get_m();
  }
}
