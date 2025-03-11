// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "posixDB.h"
#include "log/Log.h"

using namespace std;

namespace rgw { namespace store {

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

