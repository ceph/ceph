// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string.hpp>

#include "librbd/Utils.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include "include/rbd/features.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

namespace librbd {
namespace util {

const std::string id_obj_name(const std::string &name)
{
  return RBD_ID_PREFIX + name;
}

const std::string header_name(const std::string &image_id)
{
  return RBD_HEADER_PREFIX + image_id;
}

const std::string old_header_name(const std::string &image_name)
{
  return image_name + RBD_SUFFIX;
}

std::string unique_lock_name(const std::string &name, void *address) {
  return name + " (" + stringify(address) + ")";
}

librados::AioCompletion *create_rados_ack_callback(Context *on_finish) {
  return create_rados_ack_callback<Context, &Context::complete>(on_finish);
}

uint64_t parse_rbd_default_features(CephContext* cct) 
{
  int ret = 0;
  uint64_t value = 0;
  try {
    value = std::stoi(cct->_conf->rbd_default_features, NULL, 10);
  } catch (...) {
    map<std::string, int> conf_vals = {{RBD_FEATURE_NAME_LAYERING, RBD_FEATURE_LAYERING}, 
                                       {RBD_FEATURE_NAME_STRIPINGV2, RBD_FEATURE_STRIPINGV2},
                                       {RBD_FEATURE_NAME_EXCLUSIVE_LOCK, RBD_FEATURE_EXCLUSIVE_LOCK},
                                       {RBD_FEATURE_NAME_OBJECT_MAP, RBD_FEATURE_OBJECT_MAP},
                                       {RBD_FEATURE_NAME_FAST_DIFF, RBD_FEATURE_FAST_DIFF},
                                       {RBD_FEATURE_NAME_DEEP_FLATTEN, RBD_FEATURE_DEEP_FLATTEN},
                                       {RBD_FEATURE_NAME_JOURNALING, RBD_FEATURE_JOURNALING}};
    std::vector<std::string> strs;
    boost::split(strs, cct->_conf->rbd_default_features, boost::is_any_of(","));
    for (std::vector<std::string>::const_iterator i = strs.begin(); i != strs.end(); ++i) {
    	std::string feature = *i;
    	boost::trim(feature);
      if (conf_vals.find(feature) != conf_vals.end()) {
        value += conf_vals[feature];
      } else {
        ret = -EINVAL;
        ldout(cct, 1) << "Warning: unknown rbd feature " << feature << dendl;
      }
    }
    if (value == 0 && ret == -EINVAL)
      value = RBD_FEATURES_DEFAULT;
  }
  return value;
}

} // namespace util

} // namespace librbd
