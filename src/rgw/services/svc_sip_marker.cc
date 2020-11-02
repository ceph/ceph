#include "svc_sip_marker.h"

#include "common/ceph_json.h"

using namespace ceph;

void RGWSI_SIP_Marker::target_marker_info::dump(Formatter *f) const
{
  encode_json("pos", pos, f);
  encode_json("mtime", mtime, f);
}

void RGWSI_SIP_Marker::stage_shard_info::dump(Formatter *f) const
{
  encode_json("targets", targets, f);
  encode_json("min_targets_pos", min_targets_pos, f);
  encode_json("min_source_pos", min_source_pos, f);
}

void RGWSI_SIP_Marker::Handler::modify_result::dump(Formatter *f) const
{
  encode_json("modified", modified, f);
  encode_json("min_pos", min_pos, f);
}

void RGWSI_SIP_Marker::SetParams::dump(Formatter *f) const
{
  encode_json("target_id", target_id, f);
  encode_json("marker", marker, f);
  encode_json("mtime", mtime, f);
  encode_json("check_exists", check_exists, f);
}

void RGWSI_SIP_Marker::SetParams::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("target_id", target_id, obj);
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("mtime", mtime, obj);
  JSONDecoder::decode_json("check_exists", check_exists, obj);
}

std::string RGWSI_SIP_Marker::create_target_id(const rgw_zone_id& zid,
                                               std::optional<std::string> bucket_id)
{
  if (!bucket_id) {
    return zid.id;
  }

  return zid.id + ":" + *bucket_id;
}

void RGWSI_SIP_Marker::parse_target_id(const std::string& target_id,
                                       rgw_zone_id *zid,
                                       std::string *bucket_id)
{
  auto pos = target_id.find(':');
  if (pos == string::npos) {
    if (zid) {
      *zid = target_id;
    }
    return;
  }

  *zid = target_id.substr(0, pos);
  if (!bucket_id) {
    return;
  }

  *bucket_id = target_id.substr(pos + 1);
}
