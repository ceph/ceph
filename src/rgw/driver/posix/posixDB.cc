// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

  params.user_table = user_table;
  params.bucket_table = bucket_table;
  params.quota_table = quota_table;
  params.lc_entry_table = lc_entry_table;
  params.lc_head_table = lc_head_table;
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

} } // namespace rgw::store

