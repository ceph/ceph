#include "rgw_rest_account.h"
#include "rgw_rest.h"
#include "rgw_account.h"
#include "rgw_sal_rados.h"

class RGWOp_Account_Create : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("accounts", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "create_account"; }
};

void RGWOp_Account_Create::execute(optional_yield y)
{
  std::string account_id;
  std::string tenant;
  uint32_t max_users;
  RESTArgs::get_string(s, "account", account_id, &account_id);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  bool has_max_users = false;
  RESTArgs::get_uint32(s, "max-users", 0, &max_users, &has_max_users);

  RGWAccountAdminOpState acc_op_state(account_id, tenant);
  if (has_max_users) {
    acc_op_state.set_max_users(max_users);
  }

  op_ret = RGWAdminOp_Account::add(this, store, acc_op_state, flusher, s->yield);
  if (op_ret < 0) {
    if (op_ret == -EEXIST) {
      op_ret = -ERR_ACCOUNT_EXISTS;
    }
  }
}


class RGWOp_Account_Get : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_account"; }
};

void RGWOp_Account_Get::execute(optional_yield y)
{
  std::string account_id;

  RESTArgs::get_string(s, "account", account_id, &account_id);
  RGWAccountAdminOpState acc_op_state(account_id);

  op_ret = RGWAdminOp_Account::info(this, store, acc_op_state, flusher, s->yield);
}

class RGWOp_Account_Delete : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "delete_account"; }
};

void RGWOp_Account_Delete::execute(optional_yield y)
{
  std::string account_id;

  RESTArgs::get_string(s, "account", account_id, &account_id);
  RGWAccountAdminOpState acc_op_state(account_id);

  op_ret = RGWAdminOp_Account::remove(this, store, acc_op_state, flusher, s->yield);
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
