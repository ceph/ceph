// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_op.h"
#include "rgw_rest_mon_command.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_MonCommand_Get : public RGWRESTOp {
public:
  RGWOp_MonCommand_Get() = default;

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("admin", RGW_CAP_READ);
  }
  void execute() override;

  virtual const string name() override { return "mon_command"; }
};

void RGWOp_MonCommand_Get::execute() {
  std::string format = "xml";
  if (s->format == RGW_FORMAT_JSON) {
    format = "json";
  }

  std::string command;
  RESTArgs::get_string(s, "command", command, &command);
    
  librados::bufferlist inbl;
  librados::bufferlist outbl;
  std::string outs;
  librados::Rados* rados = store->get_rados_handle();
  http_ret = rados->mon_command("{\"prefix\": \"" + command + "\", \"format\": \"" + format + "\"}", 
          inbl, &outbl, &outs);
  if (http_ret != 0) {
    ldout(s->cct, 2) << "failed to mon_command http_ret " << http_ret << dendl;
    return;
  }   

  flusher.start(0);
  Formatter *formatter = flusher.get_formatter(); 
  formatter->write_raw_data(std::string(outbl.c_str(), outbl.length()).c_str());
  flusher.flush();

  http_ret = 0;
}

class RGWHandler_MonCommand : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override {
    return new RGWOp_MonCommand_Get; 
  }

  int read_permissions(RGWOp*) override {
    return 0;
  }

public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_MonCommand() override = default;
};

RGWHandler_REST* RGWRESTMgr_MonCommand::get_handler(struct req_state* s, 
        const rgw::auth::StrategyRegistry& auth_registry, 
        const std::string& frontend_prefix){
  return new RGWHandler_MonCommand(auth_registry);
}
