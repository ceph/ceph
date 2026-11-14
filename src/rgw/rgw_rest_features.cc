// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_features.h"

#include "rgw_op.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_Features_Get : public RGWRESTOp {
public:
  RGWOp_Features_Get() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("info", RGW_CAP_READ);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "get_features"; }
};

void RGWOp_Features_Get::execute(optional_yield y)
{
  std::map<std::string, std::string> features;
  driver->get_features(features);

  Formatter* formatter = flusher.get_formatter();
  flusher.start(0);

  /* the outermost section is not rendered -- same shape as
   * RGWOp_Info_Get, where it is spelled "dummy" */
  formatter->open_object_section("dummy");
  formatter->dump_string("backend", driver->get_name());
  formatter->open_object_section("features");
  for (const auto& [k, v] : features) {
    if ((v == "true") || (v == "false")) {
      formatter->dump_bool(k.c_str(), (v == "true"));
    } else {
      formatter->dump_string(k.c_str(), v);
    }
  }
  formatter->close_section();
  formatter->close_section();

  flusher.flush();
} /* RGWOp_Features_Get::execute */

RGWOp *RGWHandler_Features::op_get()
{
  return new RGWOp_Features_Get;
}
