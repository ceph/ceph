#include "rgw_op.h"
#include "rgw_usage.h"
#include "rgw_rest_usage.h"

class RGWOp_Usage : public RGWOp {

public:
  RGWOp_Usage() {}

  int verify_permission() { return 0; }
  void execute();

  virtual const char *name() { return "get_usage"; }
};

void RGWOp_Usage::execute() {
  map<std::string, bool> categories;
  int ret = RGWUsage::show(rgwstore, s->user.user_id, 0, (uint64_t)-1, true, true, &categories, s->formatter);
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  dump_start(s);

  rgw_flush_formatter_and_reset(s, s->formatter);
}

RGWOp *RGWHandler_Usage::op_get()
{
  return new RGWOp_Usage;
};


