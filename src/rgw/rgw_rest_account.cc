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
  std::string id;
  std::string tenant;
  std::string name;
  uint32_t max_users = 0;
  RESTArgs::get_string(s, "id", "", &id);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  RESTArgs::get_string(s, "name", "", &name);
  bool has_max_users = false;
  RESTArgs::get_uint32(s, "max-users", 0, &max_users, &has_max_users);

  RGWAccountAdminOpState acc_op_state;
  acc_op_state.account_id = id;
  acc_op_state.tenant = tenant;
  acc_op_state.account_name = name;
  if (has_max_users) {
    acc_op_state.set_max_users(max_users);
  }

  op_ret = RGWAdminOp_Account::create(this, store, acc_op_state,
                                      s->err.message, flusher, s->yield);
  if (op_ret < 0) {
    if (op_ret == -EEXIST) {
      op_ret = -ERR_ACCOUNT_EXISTS;
    }
  }
}

class RGWOp_Account_Modify : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("accounts", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "modify_account"; }
};

void RGWOp_Account_Modify::execute(optional_yield y)
{
  std::string id;
  std::string tenant;
  std::string name;
  uint32_t max_users = 0;
  RESTArgs::get_string(s, "id", "", &id);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  RESTArgs::get_string(s, "name", "", &name);
  bool has_max_users = false;
  RESTArgs::get_uint32(s, "max-users", 0, &max_users, &has_max_users);

  RGWAccountAdminOpState acc_op_state;
  acc_op_state.account_id = id;
  acc_op_state.tenant = tenant;
  acc_op_state.account_name = name;
  if (has_max_users) {
    acc_op_state.set_max_users(max_users);
  }

  op_ret = RGWAdminOp_Account::modify(this, store, acc_op_state,
                                      s->err.message, flusher, s->yield);
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
  std::string id;
  std::string tenant;
  std::string name;

  RESTArgs::get_string(s, "id", "", &id);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  RESTArgs::get_string(s, "name", "", &name);

  RGWAccountAdminOpState acc_op_state;
  acc_op_state.account_id = id;
  acc_op_state.tenant = tenant;
  acc_op_state.account_name = name;

  op_ret = RGWAdminOp_Account::info(this, store, acc_op_state,
                                    s->err.message, flusher, s->yield);
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
  std::string id;
  std::string tenant;
  std::string name;

  RESTArgs::get_string(s, "id", "", &id);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  RESTArgs::get_string(s, "name", "", &name);

  RGWAccountAdminOpState acc_op_state;
  acc_op_state.account_id = id;
  acc_op_state.tenant = tenant;
  acc_op_state.account_name = name;

  op_ret = RGWAdminOp_Account::remove(this, store, acc_op_state,
                                      s->err.message, flusher, s->yield);
}

class RGWOp_Account_Users : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "list_account_users"; }
};

void RGWOp_Account_Users::execute(optional_yield y)
{
  std::string id;
  std::string tenant;
  std::string name;
  std::string marker;
  uint32_t max_entries;

  RESTArgs::get_string(s, "id", "", &id);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  RESTArgs::get_string(s, "name", "", &name);
  RESTArgs::get_string(s, "marker", "", &marker);
  RESTArgs::get_uint32(s, "max-entries", 1000, &max_entries);

  RGWAccountAdminOpState acc_op_state;
  acc_op_state.account_id = id;
  acc_op_state.tenant = tenant;
  acc_op_state.account_name = name;
  acc_op_state.marker = marker;
  acc_op_state.max_entries = max_entries;

  op_ret = RGWAdminOp_Account::list_users(this, store, acc_op_state,
                                          s->err.message, flusher, s->yield);
}


RGWOp *RGWHandler_Account::op_post()
{
  return new RGWOp_Account_Create;
}

RGWOp *RGWHandler_Account::op_put()
{
  return new RGWOp_Account_Modify;
}

RGWOp *RGWHandler_Account::op_get()
{
  if (s->info.args.sub_resource_exists("users")) {
    return new RGWOp_Account_Users;
  }
  return new RGWOp_Account_Get;
}

RGWOp *RGWHandler_Account::op_delete()
{
  return new RGWOp_Account_Delete;
}
