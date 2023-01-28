// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_op.h"
#include "rgw_rest_info.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_Info_Get : public RGWRESTOp {

public:
  RGWOp_Info_Get() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("info", RGW_CAP_READ);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "get_info"; }
};

void RGWOp_Info_Get::execute(optional_yield y) {
  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  /* extensible array of general info sections, currently only
   * storage backend is defined:
   * {"info":{"storage_backends":[{"name":"rados","cluster_id":"75d1938b-2949-4933-8386-fb2d1449ff03"}]}}
   */
  formatter->open_object_section("dummy");
  formatter->open_object_section("info");
  formatter->open_array_section("storage_backends");
  // for now, just return the backend that is accessible
  formatter->open_object_section("dummy");
  formatter->dump_string("name", driver->get_name());
  formatter->dump_string("cluster_id", driver->get_cluster_id(this, y));
  formatter->close_section();
  formatter->close_section();
  formatter->close_section();
  formatter->close_section();

  flusher.flush();
} /* RGWOp_Info_Get::execute */

RGWOp *RGWHandler_Info::op_get()
{
  return new RGWOp_Info_Get;
}
