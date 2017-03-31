// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CrushLocation.h"
#include "CrushWrapper.h"
#include "common/config.h"
#include "include/str_list.h"
#include "common/debug.h"

#include <common/SubProcess.h>

#include <vector>

int CrushLocation::update_from_conf()
{
  if (cct->_conf->crush_location.length())
    return _parse(cct->_conf->crush_location);
  return 0;
}

int CrushLocation::_parse(const std::string& s)
{
  std::multimap<std::string,std::string> new_crush_location;
  std::vector<std::string> lvec;
  get_str_vec(s, ";, \t", lvec);
  int r = CrushWrapper::parse_loc_multimap(lvec, &new_crush_location);
  if (r < 0) {
    lderr(cct) << "warning: crush_location '" << cct->_conf->crush_location
	       << "' does not parse, keeping original crush_location "
	       << loc << dendl;
    return -EINVAL;
  }
  std::lock_guard<std::mutex> l(lock);
  loc.swap(new_crush_location);
  lgeneric_dout(cct, 10) << "crush_location is " << loc << dendl;
  return 0;
}

int CrushLocation::update_from_hook()
{
  if (cct->_conf->crush_location_hook.length() == 0)
    return 0;

  SubProcessTimed hook(
    cct->_conf->crush_location_hook.c_str(),
    SubProcess::CLOSE, SubProcess::PIPE, SubProcess::PIPE,
    cct->_conf->crush_location_hook_timeout);
  hook.add_cmd_args(
    "--cluster", cct->_conf->cluster.c_str(),
    "--id", cct->_conf->name.get_id().c_str(),
    "--type", cct->_conf->name.get_type_str(),
    NULL);
  int ret = hook.spawn();
  if (ret != 0) {
    lderr(cct) << "error: failed run " << cct->_conf->crush_location_hook << ": "
	       << hook.err() << dendl;
    return ret;
  }

  bufferlist bl;
  ret = bl.read_fd(hook.get_stdout(), 100 * 1024);
  if (ret < 0) {
    lderr(cct) << "error: failed read stdout from "
	       << cct->_conf->crush_location_hook
	       << ": " << cpp_strerror(-ret) << dendl;
    bufferlist err;
    err.read_fd(hook.get_stderr(), 100 * 1024);
    lderr(cct) << "stderr:\n";
    err.hexdump(*_dout);
    *_dout << dendl;
    return ret;
  }

  if (hook.join() != 0) {
    lderr(cct) << "error: failed to join: " << hook.err() << dendl;
    return -EINVAL;
  }

  std::string out;
  bl.copy(0, bl.length(), out);
  out.erase(out.find_last_not_of(" \n\r\t")+1);
  return _parse(out);
}

int CrushLocation::init_on_startup()
{
  if (cct->_conf->crush_location.length()) {
    return update_from_conf();
  }
  if (cct->_conf->crush_location_hook.length()) {
    return update_from_hook();
  }

  // start with a sane default
  char hostname[HOST_NAME_MAX + 1];
  int r = gethostname(hostname, sizeof(hostname)-1);
  if (r < 0)
    strcpy(hostname, "unknown_host");
  std::lock_guard<std::mutex> l(lock);
  loc.clear();
  loc.insert(make_pair<std::string,std::string>("host", hostname));
  loc.insert(make_pair<std::string,std::string>("root", "default"));
  lgeneric_dout(cct, 10) << "crush_location is (default) " << loc << dendl;
  return 0;
}
