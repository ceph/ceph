#include "rgw_op.h"
#include "rgw_usage.h"
#include "rgw_rest_usage.h"

class RGWOp_Usage : public RGWRESTOp {

public:
  RGWOp_Usage() {}

  int verify_permission() { return 0; }
  void execute();

  virtual const char *name() { return "get_usage"; }
};

void RGWOp_Usage::execute() {
  map<std::string, bool> categories;
  http_ret = RGWUsage::show(rgwstore, s->user.user_id, 0, (uint64_t)-1, true, true, &categories, flusher);
}

RGWOp *RGWHandler_Usage::op_get()
{
  return new RGWOp_Usage;
};


