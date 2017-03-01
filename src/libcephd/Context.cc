// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#define dout_context g_ceph_context

#include "common/debug.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/ceph_argparse.h"

#define dout_subsys ceph_subsys_osd

namespace ceph
{
namespace osd
{

int context_create(int id, const char *config, const char *cluster,
                   int argc, const char **argv, CephContext** cct_out)
{
  CephInitParameters params(CEPH_ENTITY_TYPE_OSD);
  char name[12];
  CephContext* cct;
  sprintf(name, "%d", id);
  params.name.set_id(name);

  cct = common_preinit(params, CODE_ENVIRONMENT_DAEMON, 0);

  if (cluster && cluster[0])
    cct->_conf->cluster.assign(cluster);

  // parse configuration
  std::deque<std::string> parse_errors;
//  int r = cct->_conf->parse_config_files(config, &parse_errors, &cerr, 0);
    int r = cct->_conf->parse_config_files(config, &cerr, 0);
  if (r != 0) {
    derr << "failed to parse configuration " << config << dendl;
    return r;
  }
  // parse command line arguments
  if (argc) {
    assert(argv);
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    cct->_conf->parse_argv(args);
  }
  cct->_conf->apply_changes(nullptr);
  complain_about_parse_errors(cct, &parse_errors);

//  cct->init(); FIXME? NEEDED?
  *cct_out = cct;
  return 0;
}

} // namespace osd
} // namespace ceph
