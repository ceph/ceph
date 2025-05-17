
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

