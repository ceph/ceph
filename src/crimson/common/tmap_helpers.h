// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/expected.hpp"

#include "include/buffer.h"
#include "include/encoding.h"

namespace crimson::common {

/**
 * do_tmap_up
 *
 * Performs tmap update instructions encoded in buffer referenced by in.
 *
 * @param [in] in iterator to buffer containing encoded tmap update operations
 * @param [in] contents current contents of object
 * @return buffer containing new object contents,
 *   -EINVAL for decoding errors,
 *   -EEXIST for CEPH_OSD_TMAP_CREATE on a key that exists
 *   -ENOENT for CEPH_OSD_TMAP_RM on a key that does not exist 
 */
using do_tmap_up_ret = tl::expected<bufferlist, int>;
do_tmap_up_ret do_tmap_up(bufferlist::const_iterator in, bufferlist contents);

/**
 * do_tmap_put
 *
 * Validates passed buffer pointed to by in and returns resulting object buffer.
 *
 * @param [in] in iterator to buffer containing tmap encoding
 * @return buffer containing validated tmap encoded by in
 *   -EINVAL for decoding errors,
 */
using do_tmap_up_ret = tl::expected<bufferlist, int>;
do_tmap_up_ret do_tmap_put(bufferlist::const_iterator in);

}
