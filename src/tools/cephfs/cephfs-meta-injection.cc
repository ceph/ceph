// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include <include/types.h>
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"

#include "MetaTool.h"
#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>
namespace po = boost::program_options;
using std::string;
using namespace std;
static string version = "cephfs-meta-injection v1.1";

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  string rank_str, minfo, ino, out,in;
  po::options_description general("general options");
  general.add_options()
      ("help,h", "produce help message")
      ("debug", "show debug info")
      ("rank,r", po::value<string>(&rank_str), "the rank of cephfs, default(0) (e.g. -r cephfs_a:0)")
      ("minfo", po::value<string>(&minfo), "specify metapool, datapools and rank (e.g. cephfs_metadata_a:cephfs_data_a:0)")
      ("ino,i", po::value<string>(&ino), "specify inode. e.g. 1099511627776, you can find it with cmd, 'ls -i'")
      ("out,o", po::value<string>(&out), "output file")
      ("in", po::value<string>(&in), "input file")
      ("yes-i-really-really-mean-it", "need by amend info")
      ;

  string mode;
  po::options_description modeoptions("mode options");
  modeoptions.add_options()
      ("mode", po::value<string>(&mode),
       "\tconv : convert decimal ino to hex\n"  \
       "\tlistc : list all obj of dir\n"        \
       "\tshowm : show the info of ino\n"          \
       "\tshowfn : show the fnode of dir\n"        \
       "\tamend : amend part of the meta data\n"   \
       "\tamendfn : amend fnode from file\n"
       );

  po::positional_options_description p;
  p.add("mode", 1);

  po::options_description all("all options");
  all.add(modeoptions).add(general);
  po::variables_map vm;
  try {
    po::store(po::command_line_parser(argc, argv).options(all).positional(p).allow_unregistered().run(), vm);
  } catch(exception &e) {
    cerr << "error : " << e.what() << std::endl;
    return -1;
  } catch(...) {
    cout << "param error" << std::endl;
    return 0;
  }

  boost::program_options::notify(vm);
  if (vm.count("help")) {
    std::cout << version << std::endl;
    std::cout << "usage : \n"
              << "  cephfs-meta-injection <conv|listc|showm|showfn|amend|amendfn> -r <fsname:rank> -i <ino>"
              << std::endl;
    std::cout << "example : \n"
              << "  amend info of inode(1099531628828)\n"
              << "    cephfs-meta-injection showm -r cephfs_a:0 -i 1099531628828 -o out\n"
              << "    alter file\n"
              << "    cephfs-meta-injection amend -r cephfs_a:0 -i 1099531628828 --in out --yes-i-really-mean-it"
              << std::endl;
    std::cout << all << std::endl;
    return 0;
  }

  if (mode == "conv") {
    MetaTool::conv2hexino(ino.c_str());
    return 0;
  }

  MetaTool mt(vm.count("debug"));
  int rc = mt.init();
  if (rc != 0) {
    std::cerr << "error in initialization: " << cpp_strerror(rc) << std::endl;
    return rc;
  }
  rc = mt.main(mode, rank_str, minfo, ino, out, in, vm.count("yes-i-really-really-mean-it"));
  if (rc != 0) {
    std::cerr << "error (" << cpp_strerror(rc) << ")" << std::endl;
    return -1;
  }
  return rc;
}
