#include "rgw_rest_account.h"
#include "rgw_rest.h"
#include "rgw_account.h"

class RGWOp_Account_Create : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("accounts", RGW_CAP_WRITE);
  }

  void execute() override;

  const char* name() const override { return "create_account"; }
};

void RGWOp_Account_Create::execute()
{
  std::string account_id;
  std::string tenant;
  uint32_t max_users;
  uint32_t max_roles;
  RESTArgs::get_string(s, "account", account_id, &account_id);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  RESTArgs::get_uint32(s, "max-users", DEFAULT_QUOTA_LIMIT, &max_users);
  RESTArgs::get_uint32(s, "max-roles", DEFAULT_QUOTA_LIMIT, &max_roles);

  RGWAccountInfo account_info(account_id, tenant);
  RGWObjVersionTracker objv_tracker;
  http_ret = store->ctl()->account->store_info(account_info, &objv_tracker,
                                               real_time(), true, nullptr, s->yield);
  if (http_ret < 0) {
    if (http_ret == -EEXIST) {
      http_ret = -ERR_ACCOUNT_EXISTS;
    }
    return;
  }

  flusher.start(0);
  encode_json("AccountInfo", account_info, s->formatter);
  flusher.flush();

}


class RGWOp_Account_Get : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_READ);
  }

  void execute() override;

  const char* name() const override { return "get_account"; }
};

void RGWOp_Account_Get::execute()
{
  std::string account_id;

  RESTArgs::get_string(s, "account", account_id, &account_id);

  real_time mtime;
  RGWAccountInfo account_info;
  map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv_tracker;

  http_ret = store->ctl()->account->read_info(account_id,
                                              &account_info,
                                              &objv_tracker,
                                              &mtime,
                                              &attrs,
                                              s->yield);
  if (http_ret < 0) {
    return;
  }

  flusher.start(0);
  encode_json("AccountInfo", account_info, s->formatter);
  flusher.flush();
}

class RGWOp_Account_Delete : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_WRITE);
  }

  void execute() override;

  const char* name() const override { return "delete_account"; }
};

void RGWOp_Account_Delete::execute()
{
  std::string account_id;

  RESTArgs::get_string(s, "account", account_id, &account_id);
  RGWObjVersionTracker objv_tracker;

  http_ret = store->ctl()->account->remove_info(account_id,
                                                &objv_tracker,
                                                s->yield);
}

RGWOp *RGWHandler_Account::op_put()
{
  return new RGWOp_Account_Create;
}

RGWOp *RGWHandler_Account::op_get()
{
  return new RGWOp_Account_Get;
}

RGWOp *RGWHandler_Account::op_delete()
{
  return new RGWOp_Account_Delete;
}
