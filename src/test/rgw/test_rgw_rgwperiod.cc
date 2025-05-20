
#include <catch2/catch_config.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#define CATCH_CONFIG_MAIN

#include "rgw/driver/rados/rgw_zone.h"

#include <ranges>
#include <cstring>

using std::end;
using std::begin;
using std::string;
using std::string_view;

/* I'm playing loose with the terminology here, so to clarify:
symmetrical(f, a, b)
transitive(a, b, c)
*/

TEST_CASE("rgwperiod", "[rgw][conversion]") {


//Formatter *f = new Formatter;

RGWPeriod rgw_p;
rgw_p.id = "7e616bb8-a7ca-4218-b5e9-5c205fc4bd9c";

JSONParser p;
const auto id_before = rgw_p.get_id();
CAPTURE(id_before);

decode_json_obj(rgw_p, &p);

const auto id_after = rgw_p.get_id();
CAPTURE(id_after);

SUCCEED();
/*

2025-05-17T16:43:32.734 INFO:teuthology.orchestra.run.smithi089.stderr:JFW: pushing explicitly empty period id
2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:JFW: RESPONSE was:
2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:

ID field is populated here:
{"id":"7e616bb8-a7ca-4218-b5e9-5c205fc4bd9c","epoch":2,"predecessor_uuid":"c60606a4-8c84-46b9-bbe5-9882f2adc9fe","sync_status":[],"period_map":{"id":"","zonegroups":[{"id":"a","name":"a","api_name":"a","is_master":true,"endpoints":["http://smithi089.front.sepia.ceph.com:8000"],"hostnames":[],"hostnames_s3website":[],"master_zone":"220674a7-0e01-411c-b676-eb762529b914","zones":[{"id":"220674a7-0e01-411c-b676-eb762529b914","name":"a1","endpoints":["http://smithi089.front.sepia.ceph.com:8000"],"log_meta":false,"log_data":true,"bucket_index_max_shards":11,"read_only":false,"tier_type":"","sync_from_all":true,"sync_from":[],"redirect_zone":"","supported_features":["compress-encrypted","notification_v2","resharding"]},{"id":"6bc124c1-c987-46e7-b45b-fa9093a5d467","name":"a2","endpoints":["http://smithi097.front.sepia.ceph.com:8001"],"log_meta":false,"log_data":true,"bucket_index_max_shards":11,"read_only":false,"tier_type":"","sync_from_all":true,"sync_from":[],"redirect_zone":"","supported_features":["compress-encrypted","notification_v2","resharding"]}],"placement_targets":[{"name":"default-placement","tags":[],"storage_classes":["STANDARD"]}],"default_placement":"default-placement","realm_id":"b636135f-f528-4e56-aeb9-62497ff49e9a","sync_policy":{"groups":[]},"enabled_features":["notification_v2","resharding"]}],"short_zone_ids":[{"key":"220674a7-0e01-411c-b676-eb762529b914","val":937528750},{"key":"6bc124c1-c987-46e7-b45b-fa9093a5d467","val":1187391256}]},"master_zonegroup":"a","master_zone":"220674a7-0e01-411c-b676-eb762529b914","period_config":{"bucket_quota":{"enabled":false,"check_on_raw":false,"max_size":-1,"max_size_kb":0,"max_objects":-1},"user_quota":{"enabled":false,"check_on_raw":false,"max_size":-1,"max_size_kb":0,"max_objects":-1},"user_ratelimit":{"max_read_ops":0,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":false},"bucket_ratelimit":{"max_read_ops":0,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":false},"anonymous_ratelimit":{"max_read_ops":0,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":false}},"realm_id":"b636135f-f528-4e56-aeb9-62497ff49e9a","realm_epoch":2}

2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:commit_period(): Period commit got back an empty period id
2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:JFW: period encoded as (len=2134):
2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:

ID field is empty here:
{"id":"",
"epoch":1,"predecessor_uuid":"7e616bb8-a7ca-4218-b5e9-5c205fc4bd9c","sync_status":[],"period_map":{"id":"","zonegroups":[{"id":"a","name":"a","api_name":"a","is_master":true,"endpoints":["http://smithi089.front.sepia.ceph.com:8000"],"hostnames":[],"hostnames_s3website":[],"master_zone":"220674a7-0e01-411c-b676-eb762529b914","zones":[{"id":"220674a7-0e01-411c-b676-eb762529b914","name":"a1","endpoints":["http://smithi089.front.sepia.ceph.com:8000"],"log_meta":false,"log_data":true,"bucket_index_max_shards":11,"read_only":false,"tier_type":"","sync_from_all":true,"sync_from":[],"redirect_zone":"","supported_features":["compress-encrypted","notification_v2","resharding"]},{"id":"6bc124c1-c987-46e7-b45b-fa9093a5d467","name":"a2","endpoints":["http://smithi097.front.sepia.ceph.com:8001"],"log_meta":false,"log_data":true,"bucket_index_max_shards":11,"read_only":false,"tier_type":"","sync_from_all":true,"sync_from":[],"redirect_zone":"","supported_features":["compress-encrypted","notification_v2","resharding"]}],"placement_targets":[{"name":"default-placement","tags":[],"storage_classes":["STANDARD"]}],"default_placement":"default-placement","realm_id":"b636135f-f528-4e56-aeb9-62497ff49e9a","sync_policy":{"groups":[]},"enabled_features":["notification_v2","resharding"]}],"short_zone_ids":[{"key":"220674a7-0e01-411c-b676-eb762529b914","val":937528750},{"key":"6bc124c1-c987-46e7-b45b-fa9093a5d467","val":1187391256}]},"master_zonegroup":"a","master_zone":"220674a7-0e01-411c-b676-eb762529b914","period_config":{"bucket_quota":{"enabled":false,"check_on_raw":false,"max_size":-1,"max_size_kb":0,"max_objects":-1},"user_quota":{"enabled":false,"check_on_raw":false,"max_size":-1,"max_size_kb":0,"max_objects":-1},"user_ratelimit":{"max_read_ops":0,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":false},"bucket_ratelimit":{"max_read_ops":0,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":false},"anonymous_ratelimit":{"max_read_ops":0,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":false}},"realm_id":"b636135f-f528-4e56-aeb9-62497ff49e9a","realm_epoch":3}
2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:----- JFW
2025-05-17T16:43:32.753 INFO:teuthology.orchestra.run.smithi089.stderr:update_period(): failed to commit period: (22) Invalid argument
*/
/*JFW:
radosgw-admin.cc calls commit_period();
rgw_zone.cc: commit_period():
	 JSONParser p;
	    decode_json_obj(period, &p);

-------------------
common/Formatter.h:class Formatter
	- evil revelation: Formatter  is JSON-specific
		...in that it returns "json-pretty", triggering JSONFormatter...
  class JSONFormatter : public Formatter {

rgw/rgw_common.h: struct req_state : DoutPrefixProvider {
   ceph::Formatter *formatter{nullptr};


rgw/rgw_op.h:
 class RGWOp : public DoutPrefixProvider {
    req_state *s;

rgw/rgw_rest.h:class RGWRESTOp : public RGWOp {
 ...not /too/ interesting, but does have init()
rgw/rgw_rest.h:class RGWRESTOp : public RGWOp {
rgw_rest_realm.cc:
class RGWOp_Period_Base : public RGWRESTOp {
 RGWPeriod period;
 RGWOp_Period_Base::send_response()
  encode_json("period", period, s->formatter);
  end_header(s, NULL, "application/json", s->formatter->get_len());
*/
}

