#include "svc_sip_marker.h"

#include "common/ceph_json.h"

using namespace ceph;

void RGWSI_SIP_Marker::client_marker_info::dump(Formatter *f) const
{
  encode_json("pos", pos, f);
  encode_json("mtime", mtime, f);
}

void RGWSI_SIP_Marker::stage_shard_info::dump(Formatter *f) const
{
  encode_json("clients", clients, f);
  encode_json("min_clients_pos", min_clients_pos, f);
  encode_json("low_pos", low_pos, f);
}
