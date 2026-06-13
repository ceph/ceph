// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "posixDB.h"
#include "log/Log.h"

using namespace std;

namespace rgw { namespace store {

int POSIXUserDB::ProcessOp(const DoutPrefixProvider *dpp, string_view Op, DBOpParams *params) {
  int ret = -1;
  shared_ptr<class DBOp> db_op;

  db_op = getDBOp(dpp, Op, params);

  if (!db_op) {
    ldpp_dout(dpp, 0)<<"No db_op found for Op("<<Op<<")" << dendl;
    return ret;
  }
  ret = db_op->Execute(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In Process op Execute failed for fop(" << Op << ")" << dendl;
  } else {
    ldpp_dout(dpp, 20)<<"Successfully processed fop(" << Op << ")" << dendl;
  }

  return ret;
}

int POSIXUserDB::Initialize(string logfile, int loglevel)
{
  int ret = -1;
  const DoutPrefixProvider *dpp = get_def_dpp();

  if (!cct) {
    cout << "Failed to Initialize. No ceph Context \n";
    return -1;
  }

  if (loglevel > 0) {
    cct->_conf->subsys.set_log_level(ceph_subsys_rgw, loglevel);
  }
  if (!logfile.empty()) {
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
  }

  db = openDB(dpp);

  if (!db) {
    ldpp_dout(dpp, 0) <<"Failed to open database " << dendl;
    return ret;
  }

  ret = InitializeDBOps(dpp);

  if (ret) {
    ldpp_dout(dpp, 0) <<"InitializePOSIXUserDBOps failed " << dendl;
    closeDB(dpp);
    db = NULL;
    return ret;
  }

  ldpp_dout(dpp, 0) << "POSIXUserDB successfully initialized - name:" \
    << db_name << "" << dendl;

  // Create default user that corresponds to vstart user (TODO: Temporary fix)
  dbops = SQLiteDB::dbops;
  DBOpParams params = {};
  RGWAccessKey key("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");

  params.user_table = getUserTable();
  params.bucket_table = getBucketTable();
  params.quota_table = getQuotaTable();
  params.lc_entry_table = getLCEntryTable();
  params.lc_head_table = getLCHeadTable();

  params.op.user.uinfo.display_name = "tester";
  params.op.user.uinfo.user_id.id = "test";
  params.op.user.uinfo.access_keys["default"] = key;

  shared_ptr<class DBOp> db_op;
  db_op = dbops.GetUser;
  ret = db_op->Execute(dpp, &params);

  if (ret == -ENOENT) {
    db_op = dbops.InsertUser;
    ret = db_op->Execute(dpp, &params);

    if (ret) {
      ldpp_dout(dpp, 0)<<"Op Execute failed for fop(InsertUser)" << dendl;
    } else {
      ldpp_dout(dpp, 20)<<"Successfully processed fop(InsertUser)" << dendl;
    }
  } 

  return ret;
}

int POSIXUserDB::Destroy(const DoutPrefixProvider *dpp)
{
  DB::Destroy(dpp);

  ldpp_dout(dpp, 20)<<"POSIXUserDB successfully destroyed - name:" \
    <<db_name << dendl;

  return 0;
}

int POSIXAccountDB::ProcessOp(const DoutPrefixProvider *dpp, string_view Op, DBOpParams *params) {
  int ret = -1;
  shared_ptr<class DBOp> db_op;

  db_op = getDBOp(dpp, Op, params);

  if (!db_op) {
    ldpp_dout(dpp, 0)<<"No db_op found for Op("<<Op<<")" << dendl;
    return ret;
  }
  ret = db_op->Execute(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In Process op Execute failed for fop(" << Op << ")" << dendl;
  } else {
    ldpp_dout(dpp, 20)<<"Successfully processed fop(" << Op << ")" << dendl;
  }

  return ret;
}

int POSIXAccountDB::Initialize(string logfile, int loglevel)
{
  int ret = -1;
  const DoutPrefixProvider *dpp = get_def_dpp();

  if (!cct) {
    cout << "Failed to Initialize. No ceph Context \n";
    return -1;
  }

  if (loglevel > 0) {
    cct->_conf->subsys.set_log_level(ceph_subsys_rgw, loglevel);
  }
  if (!logfile.empty()) {
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
  }

  db = openDB(dpp);

  if (!db) {
    ldpp_dout(dpp, 0) <<"Failed to open database " << dendl;
    return ret;
  }

  ret = InitializeDBOps(dpp);

  if (ret) {
    ldpp_dout(dpp, 0) <<"InitializePOSIXAccountDBOps failed " << dendl;
    closeDB(dpp);
    db = NULL;
    return ret;
  }

  ldpp_dout(dpp, 0) << "POSIXAccountDB successfully initialized - name:" \
    << db_name << "" << dendl;

  return ret;
}

int POSIXAccountDB::Destroy(const DoutPrefixProvider *dpp)
{
  DB::Destroy(dpp);

  ldpp_dout(dpp, 20)<<"POSIXAccountDB successfully destroyed - name:" \
    <<db_name << dendl;

  return 0;
}

} } // namespace rgw::store

namespace rgw::sal {

int DBStoreRole::load_by_name(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string query;
  if (!info.account_id.empty()) {
    query = "name_account";
  } else {
    query = "name";
  }
  return db->get_role(dpp, query, info);
}

int DBStoreRole::load_by_id(const DoutPrefixProvider *dpp, optional_yield y)
{
  return db->get_role(dpp, "role_id", info);
}

int DBStoreRole::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  return db->store_role(dpp, info, exclusive);
}

int DBStoreRole::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  if (info.id.empty()) {
    int r = load_by_name(dpp, y);
    if (r < 0) {
      return r;
    }
  }
  return db->remove_role(dpp, info);
}

} // namespace rgw::sal

