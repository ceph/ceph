// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <algorithm>
#include <array>
#include <string_view>

#include <boost/algorithm/string/predicate.hpp>

#include "common/split.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_op_internal.h"
#include "rgw_sal.h"

uint16_t RGWGetObjAttrs::recognize_attrs(const std::string_view hdr, const uint16_t deflt)
{
  struct AttrName {
    std::string_view name;
    uint16_t flag;
  };

  static constexpr std::array attr_names {
    AttrName {"etag", as_flag(ReqAttributes::Etag)},
    AttrName {"checksum", as_flag(ReqAttributes::Checksum)},
    AttrName {"objectparts", as_flag(ReqAttributes::ObjectParts)},
    AttrName {"objectsize", as_flag(ReqAttributes::ObjectSize)},
    AttrName {"storageclass", as_flag(ReqAttributes::StorageClass)}
  };

  auto attrs {deflt};
  for (const auto& k : ceph::split(hdr, ",")) {
    const auto attr = std::ranges::find_if(attr_names, [&k](const AttrName& attr) {
      return boost::iequals(k, attr.name);
    });

    if (attr != attr_names.end()) {
      attrs |= attr->flag;
    }
  }
  return attrs;
} /* RGWGetObjAttrs::recognize_attrs */

int RGWGetObjAttrs::verify_permission(optional_yield y)
{
  if (rgw::sal::Object::empty(s->object.get())) {
    return -EACCES;
  }

  const auto get_action = rgw_object_action_for_instance(s->object_key.instance,
      rgw::IAM::s3GetObject,
      rgw::IAM::s3GetObjectVersion);
  const auto attrs_action = rgw_object_action_for_instance(s->object_key.instance,
      rgw::IAM::s3GetObjectAttributes,
      rgw::IAM::s3GetObjectVersionAttributes);

  rgw_iam_add_objtags_for_policy(this, s);

  if (!verify_object_permission(this, s, get_action) ||
      !verify_object_permission(this, s, attrs_action)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetObjAttrs::execute(optional_yield y)
{
  RGWGetObj::execute(y);
} /* RGWGetObjAttrs::execute */
